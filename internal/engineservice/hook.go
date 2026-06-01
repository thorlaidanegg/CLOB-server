package engineservice

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob/hooks"
	"github.com/thorlaidanegg/clob/types"
)

// PostgresWalletHook validates and reserves credits before orders enter the engine.
type PostgresWalletHook struct {
	wallets     wallet.Store
	ordersStore ordersstore.Store
	rdb         *redis.Client
	logger      zerolog.Logger
}

// NewPostgresWalletHook constructs the hook.
func NewPostgresWalletHook(wallets wallet.Store, ordersStore ordersstore.Store, rdb *redis.Client, logger zerolog.Logger) *PostgresWalletHook {
	return &PostgresWalletHook{
		wallets:     wallets,
		ordersStore: ordersStore,
		rdb:         rdb,
		logger:      logger,
	}
}

// Validate checks available credits and reserves the required amount.
func (h *PostgresWalletHook) Validate(ctx hooks.OrderContext) hooks.ValidationResult {
	available, err := h.wallets.GetAvailable(ctx.Context, string(ctx.UserID))
	if err != nil {
		h.logger.Error().Err(err).Msg("hook: failed to get available balance")
		return hooks.Reject(types.RejectPreOrderHook, "wallet service unavailable")
	}

	var required types.Decimal
	if ctx.OrderType == types.Market {
		bid, ask, ok, _ := redisstore.GetBBO(ctx.Context, h.rdb, string(ctx.MarketID))
		if ok && ctx.Side == types.Bid && ask != "" {
			askDec, err := types.ParseDecimal(ask, ctx.Config.PricePrecision)
			if err == nil {
				required = askDec.Mul(ctx.Qty).MulInt(2)
			}
		} else if ok && ctx.Side == types.Ask && bid != "" {
			bidDec, err := types.ParseDecimal(bid, ctx.Config.PricePrecision)
			if err == nil {
				required = bidDec.Mul(ctx.Qty).MulInt(2)
			}
		}
		if required.Value() == 0 {
			// Empty book or no BBO; accept and let settlement handle any shortfall.
			return hooks.OK()
		}
	} else {
		required = ctx.Price.Mul(ctx.Qty)
	}

	if available.Value() < required.Value() {
		return hooks.Reject(types.RejectPreOrderHook,
			fmt.Sprintf("insufficient credits: have %s need %s", available.String(), required.String()))
	}

	if err := h.wallets.Reserve(ctx.Context, string(ctx.UserID), required); err != nil {
		return hooks.Reject(types.RejectPreOrderHook, "reservation failed")
	}

	// Write reserved_per_unit synchronously so settlement can compute exact release.
	reservedPerUnit := required.Div(ctx.Qty, ctx.Config.PricePrecision)
	if err := h.ordersStore.UpdateReservedPerUnit(
		context.Background(), string(ctx.OrderID), reservedPerUnit.Value(),
	); err != nil {
		h.logger.Warn().Err(err).Str("orderID", string(ctx.OrderID)).Msg("failed to write reserved_per_unit")
	}

	return hooks.OK()
}
