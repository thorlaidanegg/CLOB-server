package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob/types"
)

// GetMarket handles GET /v1/markets/:id
func GetMarket(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketID := chi.URLParam(r, "id")
		m, err := pgstore.GetMarket(r.Context(), pool, marketID)
		if err != nil {
			apierrors.WriteError(w, apierrors.ErrMarketNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m)
	}
}

// tradeResp is the wire format for a single trade — uses decimal strings, never BIGINT.
type tradeResp struct {
	TradeID      string `json:"tradeID"`
	MarketID     string `json:"marketID"`
	MakerOrderID string `json:"makerOrderID"`
	TakerOrderID string `json:"takerOrderID"`
	MakerUserID  string `json:"makerUserID"`
	TakerUserID  string `json:"takerUserID"`
	MakerSide    string `json:"makerSide"`
	Price        string `json:"price"`
	Qty          string `json:"qty"`
	MakerFee     string `json:"makerFee"`
	TakerFee     string `json:"takerFee"`
	FeeCurrency  string `json:"feeCurrency"`
	CreatedAt    string `json:"createdAt"`
}

// GetTrades handles GET /v1/markets/:id/trades
func GetTrades(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketID := chi.URLParam(r, "id")

		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 {
				limit = n
			}
		}

		// Verify market exists first.
		market, err := pgstore.GetMarket(r.Context(), pool, marketID)
		if err != nil {
			apierrors.WriteError(w, apierrors.ErrMarketNotFound)
			return
		}
		pp := market.PricePrecision
		qp := market.QtyPrecision

		rows, err := pgstore.ListTradesByMarket(r.Context(), pool, marketID, limit)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}

		resp := make([]tradeResp, 0, len(rows))
		for _, t := range rows {
			resp = append(resp, tradeResp{
				TradeID:      t.TradeID,
				MarketID:     t.MarketID,
				MakerOrderID: t.MakerOrderID,
				TakerOrderID: t.TakerOrderID,
				MakerUserID:  t.MakerUserID,
				TakerUserID:  t.TakerUserID,
				MakerSide:    t.MakerSide,
				Price:        types.NewDecimal(t.Price, pp).String(),
				Qty:          types.NewDecimal(t.Qty, qp).String(),
				MakerFee:     types.NewDecimal(t.MakerFee, pp).String(),
				TakerFee:     types.NewDecimal(t.TakerFee, pp).String(),
				FeeCurrency:  t.FeeCurrency,
				CreatedAt:    t.CreatedAt,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// GetMarketStats handles GET /v1/admin/markets/:id/stats
func GetMarketStats(pool *pgxpool.Pool, eng client.EngineAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketID := chi.URLParam(r, "id")

		stats, err := eng.GetStats(r.Context(), marketID)
		if err != nil {
			apierrors.WriteError(w, apierrors.ErrMarketNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	}
}
