package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

type placeOrderReq struct {
	MarketID   string `json:"marketID"`
	Side       string `json:"side"`
	OrderType  string `json:"orderType"`
	Price      string `json:"price"`
	StopPrice  string `json:"stopPrice"`
	Qty        string `json:"qty"`
	DisplayQty string `json:"displayQty"`
	TIF        string `json:"tif"`
	ExpireAt   string `json:"expireAt"` // RFC3339
	STPMode    string `json:"stpMode"`
}

// PlaceOrder handles POST /v1/orders
func PlaceOrder(pool *pgxpool.Pool, orderStore ordersstore.Store, eng client.EngineAdapter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac, ok := auth.FromContext(r.Context())
		if !ok {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}

		var req placeOrderReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid request body")
			return
		}

		orderID := fmt.Sprintf("ord_%d", time.Now().UnixNano())
		var expireAt int64
		if req.ExpireAt != "" {
			t, err := time.Parse(time.RFC3339, req.ExpireAt)
			if err != nil {
				apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid expireAt")
				return
			}
			expireAt = t.UnixNano()
		}

		// Insert order record before submitting to engine.
		pgReq := ordersstore.OrderRow{
			OrderID:   orderID,
			UserID:    ac.UserID,
			MarketID:  req.MarketID,
			Side:      req.Side,
			OrderType: req.OrderType,
			Status:    "new",
			TIF:       req.TIF,
		}
		if err := orderStore.InsertOrder(r.Context(), pgReq); err != nil {
			apierrors.WriteError(w, err)
			return
		}

		resp, err := eng.PlaceOrder(r.Context(), client.PlaceOrderRequest{
			OrderID:    orderID,
			UserID:     ac.UserID,
			MarketID:   req.MarketID,
			Side:       req.Side,
			OrderType:  req.OrderType,
			Price:      req.Price,
			StopPrice:  req.StopPrice,
			Qty:        req.Qty,
			DisplayQty: req.DisplayQty,
			TIF:        req.TIF,
			ExpireAt:   expireAt,
			STPMode:    req.STPMode,
		})
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
