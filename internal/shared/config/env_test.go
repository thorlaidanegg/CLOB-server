package config

import (
	"testing"
)

func TestLoadFromEnv_Defaults(t *testing.T) {
	t.Setenv("ROLE", "all")
	t.Setenv("POSTGRES_DSN", "postgres://x")
	// Ensure optional vars are unset so defaults apply.
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("HTTP_PORT", "")

	cfg := LoadFromEnv()

	if cfg.Role != "all" {
		t.Errorf("Role = %q, want all", cfg.Role)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel default = %q, want info", cfg.LogLevel)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr default = %q", cfg.RedisAddr)
	}
	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort default = %d, want 8080", cfg.HTTPPort)
	}
	if cfg.RateLimitRPM != 300 {
		t.Errorf("RateLimitRPM default = %d, want 300", cfg.RateLimitRPM)
	}
}

func TestLoadFromEnv_ParsesLists(t *testing.T) {
	t.Setenv("ROLE", "engine")
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("KAFKA_BROKERS", "a:9092,b:9092,c:9092")
	t.Setenv("MARKETS", "BTC-USD,ETH-USD")
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://app.example.com,https://admin.example.com")

	cfg := LoadFromEnv()

	if len(cfg.KafkaBrokers) != 3 {
		t.Errorf("KafkaBrokers = %v, want 3 entries", cfg.KafkaBrokers)
	}
	if len(cfg.Markets) != 2 || cfg.Markets[0] != "BTC-USD" {
		t.Errorf("Markets = %v", cfg.Markets)
	}
	if len(cfg.CORSAllowedOrigins) != 2 {
		t.Errorf("CORSAllowedOrigins = %v, want 2 entries", cfg.CORSAllowedOrigins)
	}
}

func TestLoadFromEnv_IntOverride(t *testing.T) {
	t.Setenv("ROLE", "gateway")
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("RATE_LIMIT_RPM", "1000")

	cfg := LoadFromEnv()

	if cfg.HTTPPort != 9090 {
		t.Errorf("HTTPPort = %d, want 9090", cfg.HTTPPort)
	}
	if cfg.RateLimitRPM != 1000 {
		t.Errorf("RateLimitRPM = %d, want 1000", cfg.RateLimitRPM)
	}
}

func TestLoadFromEnv_TLSAndCORSEmptyByDefault(t *testing.T) {
	t.Setenv("ROLE", "all")
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("GRPC_TLS_CERT_FILE", "")
	t.Setenv("CORS_ALLOWED_ORIGINS", "")

	cfg := LoadFromEnv()

	if cfg.GRPCTLSCertFile != "" {
		t.Error("GRPCTLSCertFile should be empty by default")
	}
	if len(cfg.CORSAllowedOrigins) != 0 {
		t.Error("CORSAllowedOrigins should be empty by default")
	}
}
