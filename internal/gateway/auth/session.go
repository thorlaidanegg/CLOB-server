package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// SessionCookie is the name of the httpOnly JWT cookie used for browser auth.
const SessionCookie = "clob_session"

const sessionTTL = 7 * 24 * time.Hour

// ErrInvalidToken is returned for a malformed, mis-signed, or expired token.
var ErrInvalidToken = errors.New("invalid session token")

// SessionClaims is the JWT payload. Bots use API keys instead; both resolve to
// the same AuthContext (see SessionAuthContext).
type SessionClaims struct {
	UserID  string `json:"sub"`
	Email   string `json:"email"`
	IsAdmin bool   `json:"admin"`
	Exp     int64  `json:"exp"`
}

// ── Minimal HS256 JWT (stdlib only) ──────────────────────────────────────────

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func hmacSHA256(secret, msg string) []byte {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// SignSession issues a signed JWT for the user, valid for sessionTTL.
func SignSession(secret, userID, email string, isAdmin bool) (string, error) {
	payload, err := json.Marshal(SessionClaims{
		UserID: userID, Email: email, IsAdmin: isAdmin,
		Exp: time.Now().Add(sessionTTL).Unix(),
	})
	if err != nil {
		return "", err
	}
	signing := b64([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + b64(payload)
	return signing + "." + b64(hmacSHA256(secret, signing)), nil
}

// ParseSession verifies the signature and expiry, returning the claims.
func ParseSession(secret, token string) (SessionClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return SessionClaims{}, ErrInvalidToken
	}
	signing := parts[0] + "." + parts[1]
	expected := b64(hmacSHA256(secret, signing))
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return SessionClaims{}, ErrInvalidToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return SessionClaims{}, ErrInvalidToken
	}
	var c SessionClaims
	if err := json.Unmarshal(raw, &c); err != nil {
		return SessionClaims{}, ErrInvalidToken
	}
	if c.Exp != 0 && time.Now().Unix() > c.Exp {
		return SessionClaims{}, ErrInvalidToken
	}
	return c, nil
}

// SessionAuthContext maps verified claims to the same AuthContext API keys produce.
func SessionAuthContext(c SessionClaims) AuthContext {
	scopes := []string{"trade:read", "trade:write", "feed:read"}
	tier := "standard"
	if c.IsAdmin {
		scopes = append(scopes, "admin:all")
		tier = "admin"
	}
	return AuthContext{UserID: c.UserID, Scopes: scopes, Tier: tier, RateLimit: 600}
}

// ── Cookies ──────────────────────────────────────────────────────────────────

// SetSessionCookie writes the httpOnly session cookie. secure should be true in
// production (HTTPS); cross-site browsers then need SameSite=None.
func SetSessionCookie(w http.ResponseWriter, token string, secure bool) {
	sameSite := http.SameSiteLaxMode
	if secure {
		sameSite = http.SameSiteNoneMode
	}
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: token, Path: "/",
		HttpOnly: true, Secure: secure, SameSite: sameSite,
		MaxAge: int(sessionTTL.Seconds()),
	})
}

// ClearSessionCookie expires the session cookie.
func ClearSessionCookie(w http.ResponseWriter, secure bool) {
	sameSite := http.SameSiteLaxMode
	if secure {
		sameSite = http.SameSiteNoneMode
	}
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: "", Path: "/",
		HttpOnly: true, Secure: secure, SameSite: sameSite, MaxAge: -1,
	})
}

// ReadSessionToken returns the raw token from the cookie, or "" if absent.
func ReadSessionToken(r *http.Request) string {
	c, err := r.Cookie(SessionCookie)
	if err != nil {
		return ""
	}
	return c.Value
}

// ── Passwords ────────────────────────────────────────────────────────────────

// HashPassword bcrypt-hashes a plaintext password.
func HashPassword(pw string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(b), err
}

// CheckPassword reports whether pw matches the bcrypt hash.
func CheckPassword(hash, pw string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pw)) == nil
}
