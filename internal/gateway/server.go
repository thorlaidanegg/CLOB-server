package gateway

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/gateway/admin"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ratelimit"
	"github.com/thorlaidanegg/clob-server/internal/gateway/rest"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ws"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
)

// compile-time interface check
var _ client.EngineAdapter = (*client.EngineClient)(nil)

// Deps groups all gateway dependencies.
type Deps struct {
	PG          *pgxpool.Pool
	Redis       *redis.Client
	Engine      client.EngineAdapter
	Hub         *ws.Hub
	OrderStore  ordersstore.Store
	WalletStore wallet.Store
	Cfg         *srvconfig.Config
	Log         zerolog.Logger
}

// NewRouter wires all routes and returns the root handler.
func NewRouter(deps *Deps) http.Handler {
	r := chi.NewRouter()
	r.Use(chimiddleware.Recoverer)
	r.Use(chimiddleware.RequestID)

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})

	r.Route("/v1", func(r chi.Router) {
		r.Use(auth.Middleware(deps.PG, deps.Redis))
		r.Use(ratelimit.Middleware(deps.Redis, deps.Cfg.RateLimitRPM))

		r.Post("/orders", rest.PlaceOrder(deps.PG, deps.OrderStore, deps.Engine))
		r.Get("/orders", rest.ListOrders(deps.PG, deps.OrderStore))
		r.Get("/orders/{id}", rest.GetOrder(deps.OrderStore))
		r.Delete("/orders/{id}", rest.CancelOrder(deps.PG, deps.OrderStore, deps.Engine))

		r.Get("/markets", rest.GetMarkets(deps.PG))
		r.Get("/markets/{id}", rest.GetMarket(deps.PG))
		r.Get("/markets/{id}/depth", rest.GetDepth(deps.Engine))
		r.Get("/markets/{id}/trades", rest.GetTrades(deps.PG))

		r.Get("/portfolio", rest.GetPortfolio(deps.PG, deps.Redis))
		r.Get("/leaderboard", rest.GetLeaderboard(deps.PG, deps.Redis))

		r.Post("/apikeys", rest.CreateAPIKey(deps.PG))
		r.Get("/apikeys", rest.ListAPIKeys(deps.PG))
		r.Delete("/apikeys/{id}", rest.RevokeAPIKey(deps.PG))

		r.Route("/admin", func(r chi.Router) {
			r.Use(auth.RequireScope("admin:all"))
			r.Post("/markets", admin.CreateMarket(deps.PG))
			r.Patch("/markets/{id}/halt", admin.HaltMarket(deps.PG))
			r.Patch("/markets/{id}/resume", admin.ResumeMarket(deps.PG))
			r.Get("/markets/{id}/stats", rest.GetMarketStats(deps.PG, deps.Engine))
			r.Post("/users", admin.CreateUser(deps.PG))
			r.Post("/users/{id}/credits", admin.CreditUser(deps.PG, deps.WalletStore))
			r.Delete("/orders/{id}", admin.ForceCancelOrder(deps.OrderStore, deps.Engine, deps.Log))
		})
	})

	r.Get("/v1/stream", ws.ServeWS(deps.Hub, deps.Engine, deps.PG, deps.Redis, deps.OrderStore, deps.Cfg.RateLimitWSRPS))

	return r
}

// Run starts the gateway HTTP server.
func Run(ctx context.Context, cfg *srvconfig.Config, log zerolog.Logger) {
	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("gateway: connect postgres")
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("gateway: run migrations")
	}

	rdb, err := redisstore.Connect(cfg.RedisAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("gateway: connect redis")
	}

	// Bootstrap admin key if configured and no admin exists yet.
	if cfg.AdminBootstrapKey != "" {
		BootstrapAdminKey(ctx, pool, cfg.AdminBootstrapKey, log)
	}

	walletStore := wallet.NewPgStore(pool, 2)
	orderStore := ordersstore.NewPgStore(pool)

	// Connect to engine via gRPC.
	eng, err := client.NewEngineClient(cfg.EngineGRPCAddr, cfg.GRPCTLSCAFile)
	if err != nil {
		log.Fatal().Err(err).Str("addr", cfg.EngineGRPCAddr).Msg("gateway: engine grpc client")
	}

	hub := ws.NewHub()
	go hub.Run()

	deps := &Deps{
		PG:          pool,
		Redis:       rdb,
		Engine:      eng,
		Hub:         hub,
		OrderStore:  orderStore,
		WalletStore: walletStore,
		Cfg:         cfg,
		Log:         log,
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: NewRouter(deps),
	}

	go func() {
		log.Info().Int("port", cfg.HTTPPort).Msg("gateway: HTTP server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("gateway: HTTP server error")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("gateway: shutting down")
	srv.Shutdown(context.Background())
}

// BootstrapAdminKey inserts a bootstrap admin key if no admin keys exist yet.
func BootstrapAdminKey(ctx context.Context, pool *pgxpool.Pool, bootstrapKey string, log zerolog.Logger) {
	exists, err := pgstore.AdminKeyExists(ctx, pool)
	if err != nil || exists {
		return
	}
	// Insert bootstrap user and key.
	pgstore.InsertUser(ctx, pool, "bootstrap", "bootstrap@local")
	hash := auth.HashKey(bootstrapKey)
	row := pgstore.APIKeyRow{
		UserID:    "bootstrap",
		KeyHash:   hash,
		KeyPrefix: bootstrapKey[:min(len(bootstrapKey), 12)] + "...",
		Name:      "bootstrap admin key",
		Scopes:    []string{"admin:all"},
		Tier:      "admin",
		RateLimit: 1000,
	}
	if err := pgstore.InsertAPIKey(ctx, pool, row); err != nil {
		log.Error().Err(err).Msg("gateway: failed to insert bootstrap admin key")
		return
	}
	log.Warn().Msg("gateway: bootstrap admin key created — rotate before production use")
}

// RunWithDeps starts the HTTP server with pre-built dependencies (used by ROLE=all).
func RunWithDeps(ctx context.Context, deps *Deps) {
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", deps.Cfg.HTTPPort),
		Handler: NewRouter(deps),
	}
	go func() {
		deps.Log.Info().Int("port", deps.Cfg.HTTPPort).Msg("gateway: HTTP server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			deps.Log.Error().Err(err).Msg("gateway: HTTP server error")
		}
	}()
	<-ctx.Done()
	srv.Shutdown(context.Background())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
