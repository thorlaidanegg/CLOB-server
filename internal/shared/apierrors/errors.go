package apierrors

import (
	"encoding/json"
	"errors"
	"net/http"
)

var (
	ErrInvalidKey        = errors.New("invalid API key")
	ErrKeyRevoked        = errors.New("API key revoked")
	ErrKeyExpired        = errors.New("API key expired")
	ErrInsufficientFunds = errors.New("insufficient credits")
	ErrOrderNotFound     = errors.New("order not found")
	ErrUnauthorized      = errors.New("unauthorized")
	ErrForbidden         = errors.New("forbidden: missing scope")
	ErrCommandQueueFull  = errors.New("engine command queue full")
	ErrMarketNotFound    = errors.New("market not found")
	ErrEngineUnavailable = errors.New("engine unavailable")
)

// HTTPStatus maps known errors to HTTP status codes.
func HTTPStatus(err error) int {
	switch {
	case errors.Is(err, ErrUnauthorized), errors.Is(err, ErrInvalidKey),
		errors.Is(err, ErrKeyRevoked), errors.Is(err, ErrKeyExpired):
		return http.StatusUnauthorized
	case errors.Is(err, ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, ErrOrderNotFound), errors.Is(err, ErrMarketNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrInsufficientFunds):
		return http.StatusPaymentRequired
	case errors.Is(err, ErrCommandQueueFull), errors.Is(err, ErrEngineUnavailable):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

type errResponse struct {
	Error string `json:"error"`
}

// WriteError writes a JSON error response.
func WriteError(w http.ResponseWriter, err error) {
	status := HTTPStatus(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errResponse{Error: err.Error()})
}

// WriteErrorMsg writes a JSON error response with a literal message.
func WriteErrorMsg(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errResponse{Error: msg})
}
