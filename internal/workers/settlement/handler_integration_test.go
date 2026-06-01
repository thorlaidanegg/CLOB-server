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

func TestSettlement_TakerReleasesReservationAndDeductsCost(t *testing.T) {
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

	// Fill: 5.00 @ 100.00, no fee.
	fill := &events.TradeFill{
		Base:      events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:   "ord_taker",
		UserID:    "taker",
		Role:      events.RoleTaker,
		Side:      types.Bid,
		Price:     d2("100.00"),
		FilledQty: d2("5.00"),
		Fee:       d2("0.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: fill, EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 1,
	})

	// reservationForFill = 200.00 * 5.00 = 1000.00 released from reserved.
	// cost = 100.00 * 5.00 = 500.00 deducted.
	// available = 0 + 1000.00 - 500.00 = 500.00; reserved = 0.
	wr, _ := pgstore.GetWallet(ctx, pool, "taker")
	if types.NewDecimal(wr.Available, 2).String() != "500.00" {
		t.Errorf("taker available = %s, want 500.00 (2x buffer excess returned)", types.NewDecimal(wr.Available, 2))
	}
	if types.NewDecimal(wr.Reserved, 2).String() != "0.00" {
		t.Errorf("taker reserved = %s, want 0.00", types.NewDecimal(wr.Reserved, 2))
	}
}

func TestSettlement_MakerReleasesReservedCreditsNet(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h, w, _ := seed(t, pool)
	ctx := context.Background()

	// Maker had 500.00 reserved against a resting order.
	w.Credit(ctx, "maker", d2("500.00"))
	w.Reserve(ctx, "maker", d2("500.00"))

	// Fill 5.00 @ 100.00, fee 0. Mechanically: reserved -= 500.00, available += 500.00.
	fill := &events.TradeFill{
		Base:      events.NewBase(1, time.Now().UnixNano(), mkt),
		OrderID:   "ord_maker",
		UserID:    "maker",
		Role:      events.RoleMaker,
		Side:      types.Bid,
		Price:     d2("100.00"),
		FilledQty: d2("5.00"),
		Fee:       d2("0.00"),
	}
	runEvent(t, pool, h, workers.EventEnvelope{
		Event: fill, EventType: events.TypeTradeFill, MarketID: mkt, SeqNum: 1,
	})

	wr, _ := pgstore.GetWallet(ctx, pool, "maker")
	if types.NewDecimal(wr.Reserved, 2).String() != "0.00" {
		t.Errorf("maker reserved = %s, want 0.00", types.NewDecimal(wr.Reserved, 2))
	}
	if types.NewDecimal(wr.Available, 2).String() != "500.00" {
		t.Errorf("maker available = %s, want 500.00", types.NewDecimal(wr.Available, 2))
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
