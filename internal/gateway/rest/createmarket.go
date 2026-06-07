package rest

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob/types"

	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
)

// House accounts used to seed brand-new markets with a two-sided book and a few
// trades so they look alive immediately. They are funded + granted inventory on
// demand; the seller side is required because the exchange is long-only.
const (
	houseMaker = "usr_house_maker"
	houseTaker = "usr_house_taker"
)

// Default feature set for user-created markets: limit (implicit) + market, IOC,
// FOK, stop, iceberg, post-only, reduce-only (bits 0..6). The auction bit is
// added by the engine when an opening auction is requested.
const defaultMarketFeatures = 0x7F

// auctionPreOpen is the opening-auction accumulation window. Long enough to make
// the UI countdown legible and let the seeder fill the auction book.
const auctionPreOpen = 12 * time.Second

// createMarketInput is the request body for POST /v1/markets. Prices/sizes are
// decimal strings; precisions and sizing have sensible defaults.
type createMarketInput struct {
	MarketID       string `json:"marketID"`
	BaseAsset      string `json:"baseAsset"`
	QuoteAsset     string `json:"quoteAsset"`
	PricePrecision *uint8 `json:"pricePrecision"`
	QtyPrecision   *uint8 `json:"qtyPrecision"`
	OpeningPrice   string `json:"openingPrice"`
	Auction        bool   `json:"auction"`
	Seed           *bool  `json:"seed"`      // default true
	SpreadBps      int    `json:"spreadBps"` // resting half-spread; default 10
}

// CreateMarket handles POST /v1/markets — any authenticated user can spin up a
// market. It persists the market, registers it with the live engine (no restart),
// and (unless seed=false) primes it with a two-sided book + a few trades.
func CreateMarket(pool *pgxpool.Pool, orderStore ordersstore.Store, walletStore wallet.Store, eng client.EngineAdapter, log zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}

		var in createMarketInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid body")
			return
		}

		in.MarketID = strings.ToUpper(strings.TrimSpace(in.MarketID))
		if in.MarketID == "" {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "marketID is required")
			return
		}
		if in.OpeningPrice == "" {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "openingPrice is required")
			return
		}

		pp := uint8(2)
		if in.PricePrecision != nil {
			pp = *in.PricePrecision
		}
		qp := uint8(4)
		if in.QtyPrecision != nil {
			qp = *in.QtyPrecision
		}
		if pp > 8 || qp > 8 {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "precision must be <= 8")
			return
		}

		opening, err := types.ParseDecimal(in.OpeningPrice, pp)
		if err != nil || !opening.IsPositive() {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid openingPrice")
			return
		}

		base, quote := in.BaseAsset, in.QuoteAsset
		if base == "" || quote == "" {
			if b, q, ok := splitSymbol(in.MarketID); ok {
				base, quote = b, q
			}
		}

		// When running an opening auction, record when it will clear so the UI can
		// show a countdown and reveal the opening price.
		var clearsAt *time.Time
		if in.Auction {
			t := time.Now().Add(auctionPreOpen)
			clearsAt = &t
		}

		// tick = 1 unit at price precision, lot = 1 unit at qty precision: any value
		// rounded to those precisions is valid, which keeps seeding simple.
		row := pgstore.MarketRow{
			MarketID:       in.MarketID,
			BaseAsset:      base,
			QuoteAsset:     quote,
			PricePrecision: pp,
			QtyPrecision:   qp,
			TickSize:       1,
			LotSize:        1,
			MinOrderQty:    1,
			MaxOrderQty:    pow10(qp) * 1_000_000, // 1e6 units
			MaxOrderValue:  pow10(pp) * 1_000_000_000,
			MaxDepth:       0,
			Features:       defaultMarketFeatures,
			MakerFeeRate:   0,
			TakerFeeRate:   0,
			FeeCurrency:    quote,
			FeeModel:       "flat",
			State:           "open",
			CreatedBy:       ac.UserID,
			AuctionClearsAt: clearsAt,
		}

		if err := pgstore.InsertMarket(r.Context(), pool, row); err != nil {
			// Most likely a duplicate primary key.
			apierrors.WriteErrorMsg(w, http.StatusConflict, "market already exists or could not be created")
			return
		}

		// Register with the live engine (optionally as an opening auction).
		var preopenMs int64
		if in.Auction {
			preopenMs = auctionPreOpen.Milliseconds()
		}
		if _, err := eng.CreateMarket(r.Context(), client.CreateMarketRequest{
			MarketID:         in.MarketID,
			Auction:          in.Auction,
			AuctionPreOpenMs: preopenMs,
			ReferencePrice:   in.OpeningPrice,
		}); err != nil {
			log.Error().Err(err).Str("market", in.MarketID).Msg("create market: engine register failed")
			apierrors.WriteErrorMsg(w, http.StatusBadGateway, "market saved but engine registration failed: "+err.Error())
			return
		}

		// Seed liquidity unless explicitly disabled.
		seed := in.Seed == nil || *in.Seed
		if seed {
			spread := in.SpreadBps
			if spread <= 0 {
				spread = 10
			}
			s := &marketSeeder{pool: pool, orderStore: orderStore, walletStore: walletStore, eng: eng, log: log}
			if err := s.seed(r.Context(), row, opening, spread, in.Auction); err != nil {
				log.Warn().Err(err).Str("market", in.MarketID).Msg("create market: seeding incomplete")
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(row)
	}
}

func splitSymbol(sym string) (base, quote string, ok bool) {
	parts := strings.SplitN(sym, "-", 2)
	if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
		return parts[0], parts[1], true
	}
	return "", "", false
}

func pow10(n uint8) int64 {
	r := int64(1)
	for i := uint8(0); i < n; i++ {
		r *= 10
	}
	return r
}

func fmtDec(v float64, prec uint8) string {
	return strconv.FormatFloat(v, 'f', int(prec), 64)
}
