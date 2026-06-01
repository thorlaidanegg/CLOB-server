package admin

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/rs/zerolog"
)

// ForceCancelOrder handles DELETE /v1/admin/orders/{id}
// Force-cancels any order regardless of ownership. Logs admin action for audit.
func ForceCancelOrder(orderStore ordersstore.Store, eng client.EngineAdapter, log zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		adminAC, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}

		orderID := chi.URLParam(r, "id")
		order, err := orderStore.GetOrder(r.Context(), orderID)
		if err != nil {
			apierrors.WriteError(w, apierrors.ErrOrderNotFound)
			return
		}

		log.Warn().
			Str("adminUserID", adminAC.UserID).
			Str("orderID", orderID).
			Str("ownerUserID", order.UserID).
			Msg("admin force-cancel order")

		if err := eng.CancelOrder(r.Context(), orderID, order.UserID); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
