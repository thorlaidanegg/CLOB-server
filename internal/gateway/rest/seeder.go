package rest

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob/types"

	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/normalizer"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
)

const (
	seedTrades   = 4   // crossing trades printed per market
	seedLevels   = 5   // resting price levels left on each side
	seedBaseSize = 1.0 // base order size, in whole units
	seedInvUnits = 1000.0
	seedCredits  = "100000000.00" // house wallet top-up per seed (precision 2)
)

// marketSeeder primes a fresh market with a two-sided book and a few trades using
// two funded house accounts (long-only means the sell side needs real inventory).
type marketSeeder struct {
	pool        *pgxpool.Pool
	orderStore  ordersstore.Store
	walletStore wallet.Store
	eng         client.EngineAdapter
	log         zerolog.Logger
}

func (s *marketSeeder) seed(ctx context.Context, mkt pgstore.MarketRow, opening types.Decimal, spreadBps int, auction bool) error {
	if err := s.fund(ctx, mkt, opening); err != nil {
		return err
	}

	p, err := strconv.ParseFloat(opening.String(), 64)
	if err != nil {
		return err
	}
	pp, qp := mkt.PricePrecision, mkt.QtyPrecision

	// Snap the half-spread to a whole number of ticks (>= 1). Without this, a small
	// spread at coarse price precision rounds to zero and the resting levels collapse
	// onto the mid and cross each other away, leaving an empty book.
	tick := 1 / math.Pow10(int(pp))
	off := p * float64(spreadBps) / 10000
	ticks := math.Round(off / tick)
	if ticks < 1 {
		ticks = 1
	}
	off = ticks * tick

	place := func(userID, side string, px, qty float64, tif string) {
		built, err := normalizer.BuildPlaceRequest(userID, normalizer.OrderParams{
			MarketID:  mkt.MarketID,
			Side:      side,
			OrderType: "limit",
			Price:     fmtDec(px, pp),
			Qty:       fmtDec(qty, qp),
			TIF:       tif,
		}, mkt)
		if err != nil {
			s.log.Debug().Err(err).Str("side", side).Msg("seed: build")
			return
		}
		if err := s.orderStore.InsertOrder(ctx, built.OrderRow); err != nil {
			s.log.Debug().Err(err).Msg("seed: insert order")
			return
		}
		if _, err := s.eng.PlaceOrder(ctx, built.EngineReq); err != nil {
			s.log.Debug().Err(err).Msg("seed: place order")
		}
	}

	if auction {
		// Let the engine flip PreOpen → Auction, then submit a *band* of crossing
		// orders at several prices: buyers bidding above the reference and sellers
		// asking below it. The engine's auction then computes the single clearing
		// price that maximises matched volume across this whole distribution — that
		// is the point of a call auction (vs. continuous matching walking levels).
		time.Sleep(300 * time.Millisecond)
		for k := seedTrades; k >= 1; k-- {
			d := float64(k) * off
			place(houseTaker, "bid", p+d, seedBaseSize, "GTC") // willing to pay up
			place(houseMaker, "ask", p-d, seedBaseSize, "GTC") // willing to sell down
		}
		// Heaviest interest at the reference → the equilibrium settles around here.
		place(houseTaker, "bid", p, seedBaseSize*3, "GTC")
		place(houseMaker, "ask", p, seedBaseSize*3, "GTC")
		// Non-crossing orders away from the cross become the continuous book once
		// the auction clears.
		for lvl := 1; lvl <= seedLevels; lvl++ {
			d := float64(seedTrades+lvl) * off
			place(houseTaker, "bid", p-d, seedBaseSize*float64(lvl), "GTC")
			place(houseMaker, "ask", p+d, seedBaseSize*float64(lvl), "GTC")
		}
		return nil
	}

	// Continuous: rest a maker order then cross it with an IOC taker to print a
	// trade — alternating buy and sell so the tape has both sides.
	for i := 0; i < seedTrades; i++ {
		place(houseMaker, "ask", p+off, seedBaseSize, "GTC")
		place(houseTaker, "bid", p+off, seedBaseSize, "IOC") // taker buys
		place(houseMaker, "bid", p-off, seedBaseSize, "GTC")
		place(houseTaker, "ask", p-off, seedBaseSize, "IOC") // taker sells
	}
	// Leave a resting, multi-level two-sided book.
	for lvl := 1; lvl <= seedLevels; lvl++ {
		place(houseMaker, "bid", p-float64(lvl)*off, seedBaseSize*float64(lvl), "GTC")
		place(houseMaker, "ask", p+float64(lvl)*off, seedBaseSize*float64(lvl), "GTC")
	}
	return nil
}

// fund ensures both house accounts exist, hold quote credits, and have base
// inventory on this market (so they can post the ask side on a long-only book).
func (s *marketSeeder) fund(ctx context.Context, mkt pgstore.MarketRow, opening types.Decimal) error {
	credits, err := types.ParseDecimal(seedCredits, 2)
	if err != nil {
		return err
	}
	invRaw := int64(seedInvUnits) * pow10(mkt.QtyPrecision)
	for _, uid := range []string{houseMaker, houseTaker} {
		_ = pgstore.InsertUser(ctx, s.pool, uid, uid+"@local")
		if err := s.walletStore.Credit(ctx, uid, credits); err != nil {
			return err
		}
		if err := pgstore.GrantPosition(ctx, s.pool, uid, mkt.MarketID, invRaw, opening.Value()); err != nil {
			return err
		}
	}
	return nil
}
