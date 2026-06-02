package settlement_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob-server/internal/workers/settlement"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

const mkt = "BTC-USD"

// seed inserts a precision-2/2 market and returns store handles.
func seed(t *testing.T, pool *pgxpool.Pool) (*settlement.Handler, *wallet.PgStore, ordersstore.Store) {
	t.Helper()
	ctx := context.Background()
	if err := pgstore.InsertMarket(ctx, pool, pgstore.MarketRow{
		MarketID: mkt, PricePrecision: 2, QtyPrecision: 2,
		TickSize: 1, LotSize: 1, Features: 1, FeeModel: "flat", State: "open",
	}); err != nil {
		t.Fatalf("insert market: %v", err)
	}
	orderStore := ordersstore.NewPgStore(pool)
	w := wallet.NewPgStore(pool, 2)
	h := settlement.New(pool, orderStore, zerolog.Nop())
	return h, w, orderStore
}

func runEvent(t *testing.T, pool *pgxpool.Pool, h *settlement.Handler, env workers.EventEnvelope) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := h.HandleEvent(ctx, tx, env); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("handle event: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func d2(s string) types.Decimal { return types.MustDecimal(s, 2) }

// A fill advances the order's status, filled_qty, and remain_qty.
func TestSettlement_FillUpdatesOrderStatus(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, orders := seed(t, pool)
	ctx := context.Background()

	w.Credit(ctx, "alice", d2("500.00"))
	w.Reserve(ctx, "alice", d2("500.00"))
	orders.InsertOrder(ctx, ordersstore.OrderRow{
		OrderID: "ord_fill", UserID: "alice", MarketID: mkt,
		Side: "bid", OrderType: "limit", Price: d2("100.00").Value(),
		OrigQty: 500, RemainQty: 500, Status: "new", TIF: "GTC",
	})
	orders.UpdateReservedPerUnit(ctx, "ord_fill", d2("100.00").Value())

	// Partial fill: 2.00 of 5.00 → remain 3.00, status partial.
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: &events.TradeFill{
			Base: events.NewBase(1, time.Now().UnixNano(), mkt),
			OrderID: "ord_fill", UserID: "alice", Role: events.RoleTaker, Side: types.Bid,
			Price: d2("100.00"), FilledQty: d2("2.00"), RemainQty: d2("3.00"), Fee: d2("0.00"),
		},
		EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 1,
	})

	o, _ := orders.GetOrder(ctx, "ord_fill")
	if o.Status != "partial" {
		t.Errorf("status after partial fill = %q, want partial", o.Status)
	}
	if o.RemainQty != 300 || o.FilledQty != 200 {
		t.Errorf("after partial: remain=%d filled=%d, want 300/200", o.RemainQty, o.FilledQty)
	}

	// Fill the remainder: remain 0 → status filled.
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: &events.TradeFill{
			Base: events.NewBase(2, time.Now().UnixNano(), mkt),
			OrderID: "ord_fill", UserID: "alice", Role: events.RoleTaker, Side: types.Bid,
			Price: d2("100.00"), FilledQty: d2("3.00"), RemainQty: d2("0.00"), Fee: d2("0.00"),
		},
		EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 2,
	})

	o, _ = orders.GetOrder(ctx, "ord_fill")
	if o.Status != "filled" {
		t.Errorf("status after full fill = %q, want filled", o.Status)
	}
	if o.RemainQty != 0 || o.FilledQty != 500 {
		t.Errorf("after full fill: remain=%d filled=%d, want 0/500", o.RemainQty, o.FilledQty)
	}
}

// Taker buyer with a market order: the 2× BBO buffer excess is returned.
func TestSettlement_TakerBuyerReleasesReservationAndDeductsCost(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, orders := seed(t, pool)
	ctx := context.Background()

	// Taker placed a market buy. The hook reserved with a 2x BBO buffer:
	// reservedPerUnit = 200.00, qty 5.00 → reserved 1000.00.
	w.Credit(ctx, "taker", d2("1000.00"))
	w.Reserve(ctx, "taker", d2("1000.00"))

	orders.InsertOrder(ctx, ordersstore.OrderRow{
		OrderID: "ord_taker", UserID: "taker", MarketID: mkt,
		Side: "bid", OrderType: "market", OrigQty: 500, RemainQty: 500, Status: "new", TIF: "IOC",
	})
	orders.UpdateReservedPerUnit(ctx, "ord_taker", d2("200.00").Value())

	fill := &events.TradeFill{
		Base:      events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:   "ord_taker", UserID: "taker", Role: events.RoleTaker, Side: types.Bid,
		Price: d2("100.00"), FilledQty: d2("5.00"), Fee: d2("0.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: fill, EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 1,
	})

	// reservationForFill = 200.00 × 5.00 = 1000.00 released; cost = 500.00 deducted.
	// available = 0 + 1000.00 − 500.00 = 500.00; reserved = 0.
	assertWallet(t, ctx, pool, "taker", "500.00", "0.00")
}

// Maker buyer fills at their own resting price: they pay the full cost, no refund.
// This is the regression guard for the old bug that refunded resting buyers.
func TestSettlement_MakerBuyerPaysFullCost(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, orders := seed(t, pool)
	ctx := context.Background()

	// Resting bid at 100.00 for 5.00 → reserved 500.00, reserved_per_unit 100.00.
	w.Credit(ctx, "buyer", d2("500.00"))
	w.Reserve(ctx, "buyer", d2("500.00"))
	orders.InsertOrder(ctx, ordersstore.OrderRow{
		OrderID: "ord_mb", UserID: "buyer", MarketID: mkt,
		Side: "bid", OrderType: "limit", Price: d2("100.00").Value(),
		OrigQty: 500, RemainQty: 500, Status: "rested", TIF: "GTC",
	})
	orders.UpdateReservedPerUnit(ctx, "ord_mb", d2("100.00").Value())

	fill := &events.TradeFill{
		Base:      events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:   "ord_mb", UserID: "buyer", Role: events.RoleMaker, Side: types.Bid,
		Price: d2("100.00"), FilledQty: d2("5.00"), Fee: d2("0.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: fill, EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 1,
	})

	// reservationForFill = 500.00 released; cost = 500.00 deducted.
	// available = 0 + 500.00 − 500.00 = 0; reserved = 0. The buyer paid the full 500.00.
	assertWallet(t, ctx, pool, "buyer", "0.00", "0.00")
}

// Seller receives proceeds and never touches reserved.
func TestSettlement_SellerReceivesProceeds(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, _ := seed(t, pool)
	ctx := context.Background()

	// Seller starts with an empty wallet (created via a zero credit).
	w.Credit(ctx, "seller", d2("0.00"))

	fill := &events.TradeFill{
		Base:      events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:   "ord_sell", UserID: "seller", Role: events.RoleMaker, Side: types.Ask,
		Price: d2("100.00"), FilledQty: d2("5.00"), Fee: d2("0.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: fill, EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 1,
	})

	// proceeds = 100.00 × 5.00 = 500.00 credited to available; reserved untouched.
	assertWallet(t, ctx, pool, "seller", "500.00", "0.00")
}

// Fee is deducted from the seller's proceeds.
func TestSettlement_SellerProceedsNetOfFee(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, _ := seed(t, pool)
	ctx := context.Background()

	w.Credit(ctx, "seller", d2("0.00"))
	fill := &events.TradeFill{
		Base:      events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:   "ord_sf", UserID: "seller", Role: events.RoleTaker, Side: types.Ask,
		Price: d2("100.00"), FilledQty: d2("5.00"), Fee: d2("1.50"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: fill, EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 1,
	})

	// proceeds = 500.00 − 1.50 = 498.50.
	assertWallet(t, ctx, pool, "seller", "498.50", "0.00")
}

func assertWallet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID, wantAvail, wantReserved string) {
	t.Helper()
	wr, err := pgstore.GetWallet(ctx, pool, userID)
	if err != nil {
		t.Fatalf("get wallet %s: %v", userID, err)
	}
	if got := types.NewDecimal(wr.Available, 2).String(); got != wantAvail {
		t.Errorf("%s available = %s, want %s", userID, got, wantAvail)
	}
	if got := types.NewDecimal(wr.Reserved, 2).String(); got != wantReserved {
		t.Errorf("%s reserved = %s, want %s", userID, got, wantReserved)
	}
}

func TestSettlement_OrderCanceledReleasesAndMarksCanceled(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, orders := seed(t, pool)
	ctx := context.Background()

	w.Credit(ctx, "alice", d2("500.00"))
	w.Reserve(ctx, "alice", d2("500.00"))

	orders.InsertOrder(ctx, ordersstore.OrderRow{
		OrderID: "ord_cancel", UserID: "alice", MarketID: mkt,
		Side: "bid", OrderType: "limit", Price: d2("100.00").Value(),
		OrigQty: 500, RemainQty: 500, Status: "rested", TIF: "GTC",
	})
	// reserved_per_unit = 100.00; remain 5.00 → release 500.00.
	orders.UpdateReservedPerUnit(ctx, "ord_cancel", d2("100.00").Value())

	ev := &events.OrderCanceled{
		Base:        events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:     "ord_cancel",
		UserID:      "alice",
		CanceledQty: d2("5.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: ev, EventType: events.TypeOrderCanceled, MarketID: mkt, SeqNum: 1,
	})

	wr, _ := pgstore.GetWallet(ctx, pool, "alice")
	if types.NewDecimal(wr.Available, 2).String() != "500.00" {
		t.Errorf("available after cancel = %s, want 500.00", types.NewDecimal(wr.Available, 2))
	}
	if types.NewDecimal(wr.Reserved, 2).String() != "0.00" {
		t.Errorf("reserved after cancel = %s, want 0.00", types.NewDecimal(wr.Reserved, 2))
	}
	o, _ := orders.GetOrder(ctx, "ord_cancel")
	if o.Status != "canceled" {
		t.Errorf("order status = %q, want canceled", o.Status)
	}
}

// Canceling a sell (reserved_per_unit = 0) releases no credits — the seller never
// reserved any. Guards against the removed price×qty fallback wrongly refunding.
func TestSettlement_SellCancelReleasesNothing(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, orders := seed(t, pool)
	ctx := context.Background()

	// Seller holds 300.00 available, nothing reserved.
	w.Credit(ctx, "seller", d2("300.00"))

	orders.InsertOrder(ctx, ordersstore.OrderRow{
		OrderID: "ord_sellcancel", UserID: "seller", MarketID: mkt,
		Side: "ask", OrderType: "limit", Price: d2("100.00").Value(),
		OrigQty: 500, RemainQty: 500, Status: "rested", TIF: "GTC",
		// reserved_per_unit defaults to 0 for sells
	})

	ev := &events.OrderCanceled{
		Base:        events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:     "ord_sellcancel",
		UserID:      "seller",
		CanceledQty: d2("5.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: ev, EventType: events.TypeOrderCanceled, MarketID: mkt, SeqNum: 1,
	})

	// Wallet unchanged; order marked canceled.
	assertWallet(t, ctx, pool, "seller", "300.00", "0.00")
	o, _ := orders.GetOrder(ctx, "ord_sellcancel")
	if o.Status != "canceled" {
		t.Errorf("order status = %q, want canceled", o.Status)
	}
}

func TestSettlement_TradeExecutedPersistsTrade(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, _, _ := seed(t, pool)
	ctx := context.Background()

	ev := &events.TradeExecuted{
		Base:         events.NewBase(7, time.Now().UnixNano(), mkt),
		TradeID:      "trd_1",
		MakerOrderID: "ord_m",
		TakerOrderID: "ord_t",
		MakerUserID:  "maker",
		TakerUserID:  "taker",
		MakerSide:    types.Bid,
		Price:        d2("100.00"),
		Qty:          d2("5.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: ev, EventType: events.TypeTradeExecuted, MarketID: mkt, SeqNum: 7,
	})

	trades, err := pgstore.ListTradesByMarket(ctx, pool, mkt, 10)
	if err != nil {
		t.Fatalf("list trades: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade persisted, got %d", len(trades))
	}
	if trades[0].TradeID != "trd_1" || trades[0].Price != d2("100.00").Value() {
		t.Errorf("unexpected trade row: %+v", trades[0])
	}
}

func TestSettlement_IdempotentTradePersist(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, _, _ := seed(t, pool)
	ctx := context.Background()

	ev := &events.TradeExecuted{
		Base: events.NewBase(7, time.Now().UnixNano(), mkt), TradeID: "trd_dup",
		MakerOrderID: "a", TakerOrderID: "b", MakerUserID: "m", TakerUserID: "t",
		MakerSide: types.Bid, Price: d2("100.00"), Qty: d2("5.00"),
	}
	env := workers.EventEnvelope{Event: ev, EventType: events.TypeTradeExecuted, MarketID: mkt, SeqNum: 7}

	runEvent(t, pool, h, env)
	runEvent(t, pool, h, env) // replay

	trades, _ := pgstore.ListTradesByMarket(ctx, pool, mkt, 10)
	if len(trades) != 1 {
		t.Errorf("duplicate trade insert should be a no-op, got %d rows", len(trades))
	}
}
