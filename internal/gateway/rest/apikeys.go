package rest

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

type createKeyReq struct {
	Name string `json:"name"`
}

type createKeyResp struct {
	FullKey   string `json:"fullKey"`
	KeyPrefix string `json:"keyPrefix"`
}

// CreateAPIKey handles POST /v1/apikeys
func CreateAPIKey(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}

		var req createKeyReq
		json.NewDecoder(r.Body).Decode(&req)

		fullKey, keyHash, keyPrefix, err := auth.GenerateKey("clob_live")
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}

		row := pgstore.APIKeyRow{
			UserID:    ac.UserID,
			KeyHash:   keyHash,
			KeyPrefix: keyPrefix,
			Name:      req.Name,
			Scopes:    []string{"orders:write", "orders:read", "portfolio:read"},
			Tier:      "standard",
			RateLimit: 300,
		}
		if err := pgstore.InsertAPIKey(r.Context(), pool, row); err != nil {
			apierrors.WriteError(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(createKeyResp{FullKey: fullKey, KeyPrefix: keyPrefix})
	}
}

// ListAPIKeys handles GET /v1/apikeys
func ListAPIKeys(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}
		keys, err := pgstore.ListAPIKeysByUser(r.Context(), pool, ac.UserID)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}
		// Scrub hashes before sending to client
		for i := range keys {
			keys[i].KeyHash = ""
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(keys)
	}
}

// RevokeAPIKey handles DELETE /v1/apikeys/{id}
func RevokeAPIKey(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}
		keyID := chi.URLParam(r, "id")
		if err := pgstore.RevokeAPIKey(r.Context(), pool, keyID, ac.UserID); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
