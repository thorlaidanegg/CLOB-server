package rest

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/normalizer"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
)

// PlaceOrder handles POST /v1/orders
func PlaceOrder(pool *pgxpool.Pool, orderStore ordersstore.Store, eng client.EngineAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}

		var params normalizer.OrderParams
		if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid request body")
			return
		}

		mkt, err := pgstore.GetMarket(r.Context(), pool, params.MarketID)
		if err != nil {
			apierrors.WriteError(w, apierrors.ErrMarketNotFound)
			return
		}

		built, err := normalizer.BuildPlaceRequest(ac.UserID, params, mkt)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := orderStore.InsertOrder(r.Context(), built.OrderRow); err != nil {
			apierrors.WriteError(w, err)
			return
		}

		resp, err := eng.PlaceOrder(r.Context(), built.EngineReq)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(resp)
	}
}

// CancelOrder handles DELETE /v1/orders/{id}
func CancelOrder(pool *pgxpool.Pool, orderStore ordersstore.Store, eng client.EngineAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
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
		if order.UserID != ac.UserID {
			apierrors.WriteError(w, apierrors.ErrForbidden)
			return
		}

		if err := eng.CancelOrder(r.Context(), orderID, ac.UserID); err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

// GetOrder handles GET /v1/orders/{id}
func GetOrder(orderStore ordersstore.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
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
		if order.UserID != ac.UserID {
			apierrors.WriteError(w, apierrors.ErrForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(order)
	}
}

// ListOrders handles GET /v1/orders
func ListOrders(pool *pgxpool.Pool, orderStore ordersstore.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}
		orders, err := orderStore.ListOpenOrders(r.Context(), ac.UserID)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(orders)
	}
}

// GetDepth handles GET /v1/markets/{id}/depth
func GetDepth(eng client.EngineAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		marketID := chi.URLParam(r, "id")
		snap, err := eng.GetDepth(r.Context(), marketID, 20)
		if err != nil {
			apierrors.WriteError(w, apierrors.ErrMarketNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
	}
}

// GetMarkets handles GET /v1/markets
func GetMarkets(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		markets, err := pgstore.ListMarkets(r.Context(), pool)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(markets)
	}
}
