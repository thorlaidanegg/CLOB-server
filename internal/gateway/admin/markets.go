package admin

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

// CreateMarket handles POST /v1/admin/markets
func CreateMarket(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}

		var m pgstore.MarketRow
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid body")
			return
		}
		m.CreatedBy = ac.UserID
		m.State = "open"

		if err := pgstore.InsertMarket(r.Context(), pool, m); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(m)
	}
}

// HaltMarket handles PATCH /v1/admin/markets/{id}/halt
func HaltMarket(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketID := chi.URLParam(r, "id")
		if err := pgstore.UpdateMarketState(r.Context(), pool, marketID, "halted"); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// ResumeMarket handles PATCH /v1/admin/markets/{id}/resume
func ResumeMarket(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketID := chi.URLParam(r, "id")
		if err := pgstore.UpdateMarketState(r.Context(), pool, marketID, "open"); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
