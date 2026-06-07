package rest

import (
	"encoding/json"
	"net/http"

	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
)

// WhoAmI handles GET /v1/whoami — returns the authenticated identity (works with
// either a session cookie or an API key). Useful for bots/SDKs that hold a key
// but need their userID (e.g. to target admin grants).
func WhoAmI() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"userID":  ac.UserID,
			"isAdmin": ac.HasScope("admin:all"),
			"scopes":  ac.Scopes,
		})
	}
}
