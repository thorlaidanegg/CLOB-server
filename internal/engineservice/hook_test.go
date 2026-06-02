package engineservice

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/hooks"
	"github.com/thorlaidanegg/clob/types"
)

type fakePositions struct {
	held int64
	err  error
}

func (f fakePositions) HeldQty(_ context.Context, _, _ string) (int64, error) {
	return f.held, f.err
}

func sellCtx(qty string, qtyPrec uint8) hooks.OrderContext {
	return hooks.OrderContext{
		Context:  context.Background(),
		UserID:   "alice",
		MarketID: "BTC-USD",
		Side:     types.Ask,
		Qty:      types.MustDecimal(qty, qtyPrec),
		Config:   &clobconfig.MarketConfig{PricePrecision: 2, QtyPrecision: qtyPrec},
	}
}

func TestHook_SellWithinHoldingsAccepted(t *testing.T) {
	// Holds 5.00 (raw 500 at precision 2).
	h := &PostgresWalletHook{positions: fakePositions{held: 500}, logger: zerolog.Nop()}

	if res := h.Validate(sellCtx("5.00", 2)); !res.OK {
		t.Errorf("selling exactly held qty should be accepted, got reject: %s", res.Message)
	}
	if res := h.Validate(sellCtx("3.00", 2)); !res.OK {
		t.Errorf("selling less than held qty should be accepted, got reject: %s", res.Message)
	}
}

func TestHook_SellExceedingHoldingsRejected(t *testing.T) {
	h := &PostgresWalletHook{positions: fakePositions{held: 500}, logger: zerolog.Nop()}

	res := h.Validate(sellCtx("6.00", 2))
	if res.OK {
		t.Error("selling more than held qty should be rejected (long-only)")
	}
}

func TestHook_SellWithNoPositionRejected(t *testing.T) {
	h := &PostgresWalletHook{positions: fakePositions{held: 0}, logger: zerolog.Nop()}

	res := h.Validate(sellCtx("1.00", 2))
	if res.OK {
		t.Error("selling with no position should be rejected")
	}
}

func TestHook_SellPositionReadErrorRejected(t *testing.T) {
	h := &PostgresWalletHook{positions: fakePositions{err: context.DeadlineExceeded}, logger: zerolog.Nop()}

	res := h.Validate(sellCtx("1.00", 2))
	if res.OK {
		t.Error("a position-read error should reject the order, not accept it")
	}
}
