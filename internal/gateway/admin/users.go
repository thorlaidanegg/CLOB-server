package admin

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob/types"
)

type creditReq struct {
	Amount    string `json:"amount"`
	Precision uint8  `json:"precision"`
}

// CreditUser handles POST /v1/admin/users/{id}/credits
func CreditUser(pool *pgxpool.Pool, walletStore wallet.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := chi.URLParam(r, "id")

		var req creditReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid body")
			return
		}

		precision := req.Precision
		if precision == 0 {
			precision = 2
		}

		amount, err := types.ParseDecimal(req.Amount, precision)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid amount")
			return
		}

		if err := walletStore.Credit(r.Context(), userID, amount); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

type grantPositionReq struct {
	MarketID      string `json:"marketID"`
	Quantity      string `json:"quantity"`      // decimal string at the market's qty precision
	AvgEntryPrice string `json:"avgEntryPrice"` // decimal string at the market's price precision
}

// GrantPosition handles POST /v1/admin/users/{id}/positions — seeds base inventory
// so a user can sell on a fresh (long-only) market. Operational/seeding tool.
func GrantPosition(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := chi.URLParam(r, "id")

		var req grantPositionReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MarketID == "" {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "marketID and quantity required")
			return
		}
		mkt, err := pgstore.GetMarket(r.Context(), pool, req.MarketID)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusNotFound, "market not found: "+req.MarketID)
			return
		}
		qty, err := types.ParseDecimal(req.Quantity, mkt.QtyPrecision)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid quantity")
			return
		}
		avg, err := types.ParseDecimal(orDefault(req.AvgEntryPrice, "0"), mkt.PricePrecision)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid avgEntryPrice")
			return
		}
		if err := pgstore.GrantPosition(r.Context(), pool, userID, req.MarketID, qty.Value(), avg.Value()); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// CreateUser handles POST /v1/admin/users
func CreateUser(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"userID"`
			Email  string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.Email == "" {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "userID and email required")
			return
		}
		if err := pgstore.InsertUser(r.Context(), pool, req.UserID, req.Email); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}
}
