package config

import (
	"log"
	"os"
	"strconv"
	"strings"
)

// Config holds all runtime configuration loaded from environment variables.
type Config struct {
	Role        string
	LogLevel    string
	Environment string

	PostgresDSN  string
	KafkaBrokers []string
	RedisAddr    string

	EngineGRPCPort int
	Markets        []string

	HTTPPort       int
	EngineGRPCAddr string
	RateLimitRPM   int
	RateLimitWSRPS int

	NodePoolSize    int
	CmdBufferSize   int
	EventBufferSize int

	AdminBootstrapKey string

	// Browser auth (cookie-JWT). JWTSecret signs session tokens; SignupCredits is
	// the starter balance granted to a new user on signup.
	JWTSecret     string
	SignupCredits string

	// EngineRecovery selects how the engine rebuilds book state on restart:
	// "replay" (default) rebuilds each market from the event-log checkpoint;
	// "cancel" cancels all open orders and releases their reservations.
	EngineRecovery string

	// gRPC TLS. If GRPCTLSCertFile and GRPCTLSKeyFile are both set, the engine
	// serves gRPC over TLS. If GRPCTLSCAFile is set, the gateway dials over TLS
	// verifying the server against that CA. All empty = plaintext (fine for VPC).
	GRPCTLSCertFile string
	GRPCTLSKeyFile  string
	GRPCTLSCAFile   string

	// CORS. Comma-separated list of allowed origins for browser frontends.
	// Empty = CORS disabled (non-browser clients are unaffected either way).
	// Use "*" to allow any origin.
	CORSAllowedOrigins []string
}

// LoadFromEnv reads all configuration from environment variables.
// Calls log.Fatal for missing required vars.
func LoadFromEnv() *Config {
	cfg := &Config{
		Role:        requireEnv("ROLE"),
		LogLevel:    envOr("LOG_LEVEL", "info"),
		Environment: envOr("ENVIRONMENT", "local"),

		PostgresDSN: requireEnv("POSTGRES_DSN"),
		RedisAddr:   envOr("REDIS_ADDR", "localhost:6379"),

		EngineGRPCPort: envInt("ENGINE_GRPC_PORT", 50051),
		HTTPPort:       envInt("HTTP_PORT", 8080),
		EngineGRPCAddr: envOr("ENGINE_GRPC_ADDR", "localhost:50051"),
		RateLimitRPM:   envInt("RATE_LIMIT_RPM", 300),
		RateLimitWSRPS: envInt("RATE_LIMIT_WS_RPS", 50),

		NodePoolSize:    envInt("NODE_POOL_SIZE", 100000),
		CmdBufferSize:   envInt("CMD_BUFFER_SIZE", 10000),
		EventBufferSize: envInt("EVENT_BUFFER_SIZE", 50000),

		AdminBootstrapKey: os.Getenv("ADMIN_BOOTSTRAP_KEY"),

		JWTSecret:     envOr("JWT_SECRET", "dev-insecure-secret-change-me"),
		SignupCredits: envOr("SIGNUP_CREDITS", "100000.00"),

		EngineRecovery: envOr("ENGINE_RECOVERY", "replay"),

		GRPCTLSCertFile: os.Getenv("GRPC_TLS_CERT_FILE"),
		GRPCTLSKeyFile:  os.Getenv("GRPC_TLS_KEY_FILE"),
		GRPCTLSCAFile:   os.Getenv("GRPC_TLS_CA_FILE"),
	}

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.KafkaBrokers = strings.Split(brokers, ",")
	}

	if markets := os.Getenv("MARKETS"); markets != "" {
		cfg.Markets = strings.Split(markets, ",")
	}

	if origins := os.Getenv("CORS_ALLOWED_ORIGINS"); origins != "" {
		cfg.CORSAllowedOrigins = strings.Split(origins, ",")
	}

	return cfg
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("env var %s must be an integer, got %q", key, v)
	}
	return n
}
