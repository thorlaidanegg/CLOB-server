package apierrors

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPStatus(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{ErrUnauthorized, http.StatusUnauthorized},
		{ErrInvalidKey, http.StatusUnauthorized},
		{ErrKeyRevoked, http.StatusUnauthorized},
		{ErrKeyExpired, http.StatusUnauthorized},
		{ErrForbidden, http.StatusForbidden},
		{ErrOrderNotFound, http.StatusNotFound},
		{ErrMarketNotFound, http.StatusNotFound},
		{ErrInsufficientFunds, http.StatusPaymentRequired},
		{ErrCommandQueueFull, http.StatusServiceUnavailable},
		{ErrEngineUnavailable, http.StatusServiceUnavailable},
		{errors.New("random"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		if got := HTTPStatus(tc.err); got != tc.want {
			t.Errorf("HTTPStatus(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

func TestHTTPStatus_WrappedError(t *testing.T) {
	wrapped := fmt.Errorf("context: %w", ErrInsufficientFunds)
	if got := HTTPStatus(wrapped); got != http.StatusPaymentRequired {
		t.Errorf("wrapped error not matched via errors.Is: got %d", got)
	}
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, ErrOrderNotFound)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	var body errResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}
	if body.Error != ErrOrderNotFound.Error() {
		t.Errorf("body.error = %q, want %q", body.Error, ErrOrderNotFound.Error())
	}
}

func TestWriteErrorMsg(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteErrorMsg(rec, http.StatusBadRequest, "bad input")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	var body errResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}
	if body.Error != "bad input" {
		t.Errorf("body.error = %q, want 'bad input'", body.Error)
	}
}
