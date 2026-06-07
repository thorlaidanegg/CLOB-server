package engineservice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/hooks"
	"github.com/thorlaidanegg/clob/types"

	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

// defaultAuctionPreOpen is the accumulation window used when a caller requests an
// opening auction without specifying one.
const defaultAuctionPreOpen = 5 * time.Second

// MarketCreator registers brand-new markets in an already-running MultiEngine.
// It rebuilds the exact same options startup uses (wallet hook + fee calculator),
// attaches the new market's event stream to the publisher so async projections
// keep working, and opens the market (continuous) or lets the opening auction run.
//
// It is shared by the gRPC engine server (ROLE=engine) and the in-process direct
// adapter (ROLE=all) so both deployments behave identically.
type MarketCreator struct {
	ctx       context.Context
	multi     *engine.MultiEngine
	pool      *pgxpool.Pool
	hook      hooks.PreOrderHook
	vc        *VolumeCache
	publisher *EventPublisher
	log       zerolog.Logger
}

// NewMarketCreator wires a MarketCreator. ctx must be the long-lived service
// context (it backs the per-market event-publishing goroutines).
func NewMarketCreator(ctx context.Context, multi *engine.MultiEngine, pool *pgxpool.Pool, hook hooks.PreOrderHook, vc *VolumeCache, publisher *EventPublisher, log zerolog.Logger) *MarketCreator {
	return &MarketCreator{ctx: ctx, multi: multi, pool: pool, hook: hook, vc: vc, publisher: publisher, log: log}
}

// CreateParams controls a runtime market creation.
type CreateParams struct {
	MarketID       string
	Auction        bool          // run an opening call-auction before continuous trading
	PreOpen        time.Duration // auction pre-open window (0 → default)
	ReferencePrice string        // auction clearing-price tiebreaker (decimal string)
}

// Create loads the market's config from Postgres, registers it with the live
// engine, attaches its events to the publisher, and (for continuous markets)
// opens it. Returns the resulting config and the engine state ("open", "auction",
// or "exists" when the market was already registered).
func (c *MarketCreator) Create(p CreateParams) (clobconfig.MarketConfig, string, error) {
	row, err := pgstore.GetMarket(c.ctx, c.pool, p.MarketID)
	if err != nil {
		return clobconfig.MarketConfig{}, "", fmt.Errorf("market not found: %s", p.MarketID)
	}
	cfg, err := rowToMarketConfig(row)
	if err != nil {
		return clobconfig.MarketConfig{}, "", err
	}

	if p.Auction {
		preopen := p.PreOpen
		if preopen <= 0 {
			preopen = defaultAuctionPreOpen
		}
		ref := types.Zero(cfg.PricePrecision)
		if p.ReferencePrice != "" {
			if rp, perr := types.ParseDecimal(p.ReferencePrice, cfg.PricePrecision); perr == nil {
				ref = rp
			}
		}
		cfg.Features = cfg.Features.Add(clobconfig.FeatureAuctions)
		cfg.Auction = &clobconfig.AuctionConfig{
			PreOpenDuration: preopen,
			OpenTime:        time.Now().Add(preopen),
			ReferencePrice:  ref,
		}
	}

	opts := []engine.Option{
		engine.WithPreOrderHook(c.hook),
		engine.WithFeeCalculator(FeeCalculatorFor(cfg, c.vc)),
	}
	if err := c.multi.CreateMarket(cfg, opts...); err != nil {
		if errors.Is(err, engine.ErrMarketAlreadyExists) {
			return cfg, "exists", nil
		}
		return clobconfig.MarketConfig{}, "", err
	}

	// Publish this new market's events. AllEvents() was captured at startup and
	// won't include a market created later, so without this the settlement /
	// portfolio / leaderboard / feed workers would never see its trades.
	if ch, eerr := c.multi.Events(types.MarketID(p.MarketID)); eerr == nil {
		go c.publisher.Run(c.ctx, ch)
	}

	// Continuous markets start in PreOpen — open them so orders match. Auction
	// markets open themselves when the engine's pre-open timer elapses.
	state := "auction"
	if !p.Auction {
		if err := c.multi.Submit(engine.AdminResumeMarket{MarketID: types.MarketID(p.MarketID)}); err != nil {
			c.log.Error().Err(err).Str("market", p.MarketID).Msg("engine: open new market")
		}
		state = "open"
	}

	c.log.Info().Str("market", p.MarketID).Bool("auction", p.Auction).Msg("engine: market created live")
	return cfg, state, nil
}
