package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/mr-tron/base58"
)

// GenerateKey creates a new API key. Returns the full key, its hash, and the display prefix.
func GenerateKey(prefix string) (fullKey, keyHash, keyPrefix string, err error) {
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", "", "", fmt.Errorf("auth: generate key: %w", err)
	}
	encoded := base58.Encode(raw)
	fullKey = prefix + "_" + encoded
	keyHash = HashKey(fullKey)
	// First 8 chars of the encoded part as display prefix.
	if len(encoded) > 8 {
		keyPrefix = prefix + "_" + encoded[:8] + "..."
	} else {
		keyPrefix = fullKey
	}
	return fullKey, keyHash, keyPrefix, nil
}

// HashKey returns hex(sha256(fullKey)).
func HashKey(fullKey string) string {
	h := sha256.Sum256([]byte(fullKey))
	return hex.EncodeToString(h[:])
}

// AuthContext carries the validated identity for a request.
type AuthContext struct {
	UserID    string
	Scopes    []string
	Tier      string
	RateLimit int
}

// HasScope returns true if the given scope is granted.
func (a AuthContext) HasScope(scope string) bool {
	for _, s := range a.Scopes {
		if s == scope || s == "admin:all" {
			return true
		}
	}
	return false
}
