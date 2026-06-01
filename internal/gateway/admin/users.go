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
