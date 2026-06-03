package engineservice

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob/hooks"
	"github.com/thorlaidanegg/clob/types"
)

// PositionReader returns a user's held quantity for a market as a raw integer at
// the market's qty precision. Used by the hook to enforce long-only sells.
type PositionReader interface {
	HeldQty(ctx context.Context, userID, marketID string) (int64, error)
}

// pgPositionReader implements PositionReader against Postgres.
type pgPositionReader struct{ pool *pgxpool.Pool }

// NewPgPositionReader builds a Postgres-backed position reader.
func NewPgPositionReader(pool *pgxpool.Pool) PositionReader { return &pgPositionReader{pool: pool} }

func (r *pgPositionReader) HeldQty(ctx context.Context, userID, marketID string) (int64, error) {
	row, err := pgstore.GetPosition(ctx, r.pool, userID, marketID)
	if err != nil {
		return 0, err
	}
	return row.Quantity, nil // GetPosition returns a zero-value row (qty 0) when none exists
}

// PostgresWalletHook enforces the wallet model (see doc/WALLET_MODEL.md) before
// an order reaches the matching engine:
//   - Buy (bid): reserve credits (available → reserved), write reserved_per_unit.
//   - Sell (ask): long-only — validate held position, reserve nothing.
type PostgresWalletHook struct {
	wallets     wallet.Store
	ordersStore ordersstore.Store
	positions   PositionReader
	rdb         *redis.Client
	logger      zerolog.Logger
}

// NewPostgresWalletHook constructs the hook.
func NewPostgresWalletHook(wallets wallet.Store, ordersStore ordersstore.Store, positions PositionReader, rdb *redis.Client, logger zerolog.Logger) *PostgresWalletHook {
	return &PostgresWalletHook{
		wallets:     wallets,
		ordersStore: ordersStore,
		positions:   positions,
		rdb:         rdb,
		logger:      logger,
	}
}

// Validate dispatches to the buy or sell path.
func (h *PostgresWalletHook) Validate(ctx hooks.OrderContext) hooks.ValidationResult {
	if ctx.Side == types.Ask {
		return h.validateSell(ctx)
	}
	return h.validateBuy(ctx)
}

// validateSell enforces long-only: the user must already hold enough quantity.
// Sells reserve no credits, so reserved_per_unit stays 0.
func (h *PostgresWalletHook) validateSell(ctx hooks.OrderContext) hooks.ValidationResult {
	heldRaw, err := h.positions.HeldQty(ctx.Context, string(ctx.UserID), string(ctx.MarketID))
	if err != nil {
		h.logger.Error().Err(err).Msg("hook: failed to read position")
		return hooks.Reject(types.RejectPreOrderHook, "position service unavailable")
	}
	held := types.NewDecimal(heldRaw, ctx.Config.QtyPrecision)
	if held.Value() < ctx.Qty.Value() {
		return hooks.Reject(types.RejectPreOrderHook,
			fmt.Sprintf("insufficient position to sell: have %s need %s", held.String(), ctx.Qty.String()))
	}
	return hooks.OK()
}

// validateBuy reserves the credits a buy could cost and records reserved_per_unit.
func (h *PostgresWalletHook) validateBuy(ctx hooks.OrderContext) hooks.ValidationResult {
	available, err := h.wallets.GetAvailable(ctx.Context, string(ctx.UserID))
	if err != nil {
		h.logger.Error().Err(err).Msg("hook: failed to get available balance")
		return hooks.Reject(types.RejectPreOrderHook, "wallet service unavailable")
	}

	var required types.Decimal
	if ctx.OrderType == types.Market {
		// Buy at the ask; reserve a 2× buffer for slippage.
		_, ask, ok, _ := redisstore.GetBBO(ctx.Context, h.rdb, string(ctx.MarketID))
		if ok && ask != "" {
			askDec, err := types.ParseDecimal(ask, ctx.Config.PricePrecision)
			if err == nil {
				required = askDec.MulQty(ctx.Qty).MulInt(2)
			}
		}
		if required.Value() == 0 {
			// Empty book or no BBO; accept and let settlement reconcile.
			return hooks.OK()
		}
	} else {
		required = ctx.Price.MulQty(ctx.Qty)
	}

	if available.Value() < required.Value() {
		return hooks.Reject(types.RejectPreOrderHook,
			fmt.Sprintf("insufficient credits: have %s need %s", available.String(), required.String()))
	}

	if err := h.wallets.Reserve(ctx.Context, string(ctx.UserID), required); err != nil {
		return hooks.Reject(types.RejectPreOrderHook, "reservation failed")
	}

	// Write reserved_per_unit synchronously so settlement can release the exact amount.
	reservedPerUnit := required.Div(ctx.Qty, ctx.Config.PricePrecision)
	if err := h.ordersStore.UpdateReservedPerUnit(
		context.Background(), string(ctx.OrderID), reservedPerUnit.Value(),
	); err != nil {
		h.logger.Warn().Err(err).Str("orderID", string(ctx.OrderID)).Msg("failed to write reserved_per_unit")
	}

	return hooks.OK()
}
