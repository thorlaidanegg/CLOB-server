// Package sdk provides a programmatic API for embedding clob-server in Go applications.
// It wires up the matching engine, gateway, workers, and storage in one call.
package sdk

import (
	"context"
	"fmt"
	"net/http"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/fees"
	"github.com/thorlaidanegg/clob/hooks"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	"github.com/thorlaidanegg/clob-server/internal/engineservice"
	"github.com/thorlaidanegg/clob-server/internal/gateway"
	gatewayclient "github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ws"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	"github.com/thorlaidanegg/clob-server/internal/shared/logger"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	feedworker "github.com/thorlaidanegg/clob-server/internal/workers/feed"
	lbworker "github.com/thorlaidanegg/clob-server/internal/workers/leaderboard"
	portfolioworker "github.com/thorlaidanegg/clob-server/internal/workers/portfolio"
	settlementworker "github.com/thorlaidanegg/clob-server/internal/workers/settlement"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob-server/internal/workers"
)

// Config is the programmatic configuration for a Server.
type Config struct {
	Markets     []clobconfig.MarketConfig
	PostgresDSN string
	RedisAddr   string
	HTTPPort    int
	LogLevel    string
	Environment string
}

// Server is a fully wired clob-server instance.
type Server struct {
	multi       *engine.MultiEngine
	inMemBus    *bus.InMemBus
	hub         *ws.Hub
	httpServer  *http.Server
	cancel      context.CancelFunc

	preHooks  map[string]hooks.PreOrderHook
	feeCalcs  map[string]fees.FeeCalculator
	cfg       Config
	log       zerolog.Logger
}

// New creates a Server but does not start it. Call Start to begin serving.
func New(cfg Config) (*Server, error) {
	if cfg.PostgresDSN == "" {
		return nil, fmt.Errorf("sdk: PostgresDSN is required")
	}
	if cfg.RedisAddr == "" {
		cfg.RedisAddr = "localhost:6379"
	}
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = 8080
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.Environment == "" {
		cfg.Environment = "local"
	}

	return &Server{
		cfg:      cfg,
		log:      logger.New(cfg.LogLevel, cfg.Environment),
		preHooks: make(map[string]hooks.PreOrderHook),
		feeCalcs: make(map[string]fees.FeeCalculator),
	}, nil
}

// SetPreOrderHook overrides the wallet hook for a specific market.
func (s *Server) SetPreOrderHook(marketID string, h hooks.PreOrderHook) {
	s.preHooks[marketID] = h
}

// SetFeeCalculator overrides the fee calculator for a specific market.
func (s *Server) SetFeeCalculator(marketID string, fc fees.FeeCalculator) {
	s.feeCalcs[marketID] = fc
}

// Start initialises all components and begins serving HTTP. Blocks until Close is called.
func (s *Server) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	pool, err := pgstore.Connect(ctx, s.cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("sdk: postgres: %w", err)
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		return fmt.Errorf("sdk: migrations: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: s.cfg.RedisAddr})
	walletStore := wallet.NewPgStore(pool, 2)
	orderStore := ordersstore.NewPgStore(pool)

	// Default hook if none set per-market.
	defaultHook := engineservice.NewPostgresWalletHook(walletStore, orderStore, rdb, s.log)

	s.multi = engine.NewMultiEngine()
	for _, mc := range s.cfg.Markets {
		h := hooks.PreOrderHook(defaultHook)
		if custom, ok := s.preHooks[string(mc.MarketID)]; ok {
			h = custom
		}
		fc := fees.FeeCalculator(fees.FlatRateFeeCalculator{})
		if custom, ok := s.feeCalcs[string(mc.MarketID)]; ok {
			fc = custom
		}
		if err := s.multi.CreateMarket(mc,
			engine.WithPreOrderHook(h),
			engine.WithFeeCalculator(fc),
		); err != nil {
			return fmt.Errorf("sdk: create market %s: %w", mc.MarketID, err)
		}
	}

	s.inMemBus = bus.NewInMemBus()
	go engineservice.NewEventPublisher(s.inMemBus, s.log).Run(ctx, s.multi.AllEvents())

	s.hub = ws.NewHub()
	go s.hub.Run()
	go ws.NewBroadcaster(s.inMemBus.NewConsumer(), s.hub, s.log).Run(ctx)

	mc := make(map[string]clobconfig.MarketConfig, len(s.cfg.Markets))
	for _, m := range s.cfg.Markets {
		mc[string(m.MarketID)] = m
	}

	settlementHandler := settlementworker.New(pool, orderStore, s.log)
	go workers.NewWorkerRunner("settlement", "market-events", pool, s.inMemBus.NewConsumer(), settlementHandler, s.log).Run(ctx)

	portfolioHandler := portfolioworker.New(pool, rdb, mc, s.log)
	go workers.NewWorkerRunner("portfolio", "market-events", pool, s.inMemBus.NewConsumer(), portfolioHandler, s.log).Run(ctx)

	lbHandler, err := lbworker.New(pool, rdb, mc, s.log)
	if err != nil {
		return fmt.Errorf("sdk: leaderboard: %w", err)
	}
	go workers.NewWorkerRunner("leaderboard", "market-events", pool, s.inMemBus.NewConsumer(), lbHandler, s.log).Run(ctx)

	feedHandler := feedworker.New(s.hub, rdb, s.log)
	go workers.NewWorkerRunner("feed", "market-events", pool, s.inMemBus.NewConsumer(), feedHandler, s.log).Run(ctx)

	engineAdapter := gatewayclient.NewDirectAdapter(s.multi, s.cfg.Markets)
	deps := &gateway.Deps{
		PG:          pool,
		Redis:       rdb,
		Engine:      engineAdapter,
		Hub:         s.hub,
		OrderStore:  orderStore,
		WalletStore: walletStore,
		Cfg: &srvconfig.Config{
			HTTPPort:       s.cfg.HTTPPort,
			RateLimitRPM:   300,
			RateLimitWSRPS: 50,
		},
		Log: s.log,
	}

	gateway.RunWithDeps(ctx, deps) // blocks until ctx cancelled
	return nil
}

// Close stops all components gracefully.
func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.multi != nil {
		s.multi.Close()
	}
	if s.inMemBus != nil {
		s.inMemBus.Close()
	}
	return nil
}
