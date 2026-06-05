package rest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob/types"
)

// AuthConfig carries the bits the auth handlers need from server config.
type AuthConfig struct {
	JWTSecret      string
	Secure         bool   // set Secure/SameSite=None cookies (production HTTPS)
	StarterCredits string // decimal string granted to new users, e.g. "100000.00"
}

type authUser struct {
	UserID  string `json:"userID"`
	Email   string `json:"email"`
	IsAdmin bool   `json:"isAdmin"`
}

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func newUserID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "usr_" + hex.EncodeToString(b)
}

// Signup creates a user, grants starter credits, and sets the session cookie.
func Signup(pool *pgxpool.Pool, walletStore wallet.Store, cfg AuthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req credentials
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid body")
			return
		}
		req.Email = strings.TrimSpace(strings.ToLower(req.Email))
		if req.Email == "" || !strings.Contains(req.Email, "@") {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "a valid email is required")
			return
		}
		if len(req.Password) < 6 {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "password must be at least 6 characters")
			return
		}

		hash, err := auth.HashPassword(req.Password)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusInternalServerError, "could not hash password")
			return
		}
		userID := newUserID()
		if err := pgstore.CreateUserWithPassword(r.Context(), pool, userID, req.Email, hash); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusConflict, "email already registered")
			return
		}

		// Grant starter virtual credits so the user can trade immediately.
		if amt, perr := types.ParseDecimal(cfg.StarterCredits, 2); perr == nil {
			_ = walletStore.Credit(r.Context(), userID, amt)
		}

		token, err := auth.SignSession(cfg.JWTSecret, userID, req.Email, false)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusInternalServerError, "could not start session")
			return
		}
		auth.SetSessionCookie(w, token, cfg.Secure)
		writeJSON(w, authUser{UserID: userID, Email: req.Email, IsAdmin: false})
	}
}

// Login verifies credentials and sets the session cookie.
func Login(pool *pgxpool.Pool, cfg AuthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req credentials
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			apierrors.WriteErrorMsg(w, http.StatusBadRequest, "invalid body")
			return
		}
		req.Email = strings.TrimSpace(strings.ToLower(req.Email))

		u, err := pgstore.GetUserByEmail(r.Context(), pool, req.Email)
		if err != nil || u.PasswordHash == "" || !auth.CheckPassword(u.PasswordHash, req.Password) {
			apierrors.WriteErrorMsg(w, http.StatusUnauthorized, "invalid email or password")
			return
		}

		token, err := auth.SignSession(cfg.JWTSecret, u.UserID, u.Email, u.IsAdmin)
		if err != nil {
			apierrors.WriteErrorMsg(w, http.StatusInternalServerError, "could not start session")
			return
		}
		auth.SetSessionCookie(w, token, cfg.Secure)
		writeJSON(w, authUser{UserID: u.UserID, Email: u.Email, IsAdmin: u.IsAdmin})
	}
}

// Logout clears the session cookie.
func Logout(cfg AuthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		auth.ClearSessionCookie(w, cfg.Secure)
		w.WriteHeader(http.StatusNoContent)
	}
}

// Me returns the current user from the session cookie (401 if absent/invalid).
func Me(cfg AuthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, err := auth.ParseSession(cfg.JWTSecret, auth.ReadSessionToken(r))
		if err != nil {
			apierrors.WriteError(w, apierrors.ErrUnauthorized)
			return
		}
		writeJSON(w, authUser{UserID: claims.UserID, Email: claims.Email, IsAdmin: claims.IsAdmin})
	}
}
