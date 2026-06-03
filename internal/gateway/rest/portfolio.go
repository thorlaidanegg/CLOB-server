package rest

import (
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob/types"
)

type positionResp struct {
	MarketID       string `json:"marketID"`
	Quantity       string `json:"quantity"`
	AvgEntryPrice  string `json:"avgEntryPrice"`
	RealisedPnl    string `json:"realisedPnl"`
	UnrealisedPnl  string `json:"unrealisedPnl"`
	LastPrice      string `json:"lastPrice"`
}

type portfolioResp struct {
	WalletAvailable string         `json:"walletAvailable"`
	WalletReserved  string         `json:"walletReserved"`
	Positions       []positionResp `json:"positions"`
}

// GetPortfolio handles GET /v1/portfolio
func GetPortfolio(pool *pgxpool.Pool, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}

		wallet, err := pgstore.GetWallet(r.Context(), pool, ac.UserID)
		if err != nil {
			// Return empty portfolio if no wallet yet
			wallet = pgstore.WalletRow{UserID: ac.UserID, Precision: 2}
		}

		positions, err := pgstore.ListPositionsByUser(r.Context(), pool, ac.UserID)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}

		markets, err := pgstore.ListMarkets(r.Context(), pool)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}
		marketMap := make(map[string]pgstore.MarketRow, len(markets))
		for _, m := range markets {
			marketMap[m.MarketID] = m
		}

		posResps := make([]positionResp, 0, len(positions))
		for _, p := range positions {
			m, ok := marketMap[p.MarketID]
			if !ok {
				continue
			}
			pp := m.PricePrecision
			qp := m.QtyPrecision

			qty := types.NewDecimal(p.Quantity, qp)
			avgEntry := types.NewDecimal(p.AvgEntryPrice, pp)
			realisedPnl := types.NewDecimal(p.RealisedPnl, pp)

			var unrealised types.Decimal
			lastPriceStr := ""
			if lp, ok2, _ := redisstore.GetLastPrice(r.Context(), rdb, p.MarketID); ok2 {
				lastPriceStr = lp
				if lastDec, err := types.ParseDecimal(lp, pp); err == nil {
					unrealised = lastDec.Sub(avgEntry).MulQty(qty)
				}
			}

			posResps = append(posResps, positionResp{
				MarketID:      p.MarketID,
				Quantity:      qty.String(),
				AvgEntryPrice: avgEntry.String(),
				RealisedPnl:   realisedPnl.String(),
				UnrealisedPnl: unrealised.String(),
				LastPrice:     lastPriceStr,
			})
		}

		walletPrec := wallet.Precision
		resp := portfolioResp{
			WalletAvailable: types.NewDecimal(wallet.Available, walletPrec).String(),
			WalletReserved:  types.NewDecimal(wallet.Reserved, walletPrec).String(),
			Positions:       posResps,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
