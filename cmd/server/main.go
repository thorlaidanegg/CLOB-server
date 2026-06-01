package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/fees"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	"github.com/thorlaidanegg/clob-server/internal/engineservice"
	"github.com/thorlaidanegg/clob-server/internal/gateway"
	gatewayclient "github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ws"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	"github.com/thorlaidanegg/clob-server/internal/shared/logger"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	feedworker "github.com/thorlaidanegg/clob-server/internal/workers/feed"
	lbworker "github.com/thorlaidanegg/clob-server/internal/workers/leaderboard"
	portfolioworker "github.com/thorlaidanegg/clob-server/internal/workers/portfolio"
	settlementworker "github.com/thorlaidanegg/clob-server/internal/workers/settlement"
)

func main() {
	cfg := srvconfig.LoadFromEnv()
	log := logger.New(cfg.LogLevel, cfg.Environment)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cfg.Role {
	case "engine":
		engineservice.Run(ctx, cfg, log)
	case "gateway":
		gateway.Run(ctx, cfg, log)
	case "settlement":
		settlementworker.Run(ctx, cfg, log)
	case "portfolio":
		portfolioworker.Run(ctx, cfg, log)
	case "leaderboard":
		lbworker.Run(ctx, cfg, log)
	case "feed":
		feedworker.Run(ctx, cfg, log)
	case "all":
		runAll(ctx, cfg)
	default:
		log.Fatal().Str("role", cfg.Role).Msg("unknown ROLE — valid: engine, gateway, settlement, portfolio, leaderboard, feed, all")
	}
}

func runAll(ctx context.Context, cfg *srvconfig.Config) {
	log := logger.New(cfg.LogLevel, cfg.Environment)

	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("connect postgres")
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("run migrations")
	}

	rdb, err := redisstore.Connect(cfg.RedisAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("connect redis")
	}

	if cfg.AdminBootstrapKey != "" {
		gateway.BootstrapAdminKey(ctx, pool, cfg.AdminBootstrapKey, log)
	}

	marketCfgs, err := engineservice.LoadMarkets(ctx, pool)
	if err != nil {
		log.Fatal().Err(err).Msg("load markets")
	}

	walletStore := wallet.NewPgStore(pool, 2)
	orderStore := ordersstore.NewPgStore(pool)
	hook := engineservice.NewPostgresWalletHook(walletStore, orderStore, rdb, log)

	// V1 restart recovery: cancel open orders before accepting new commands.
	engineservice.RecoverOpenOrders(ctx, pool, marketCfgs, log)

	multi := engine.NewMultiEngine()
	for _, mc := range marketCfgs {
		if err := multi.CreateMarket(mc,
			engine.WithPreOrderHook(hook),
			engine.WithFeeCalculator(fees.FlatRateFeeCalculator{}),
		); err != nil {
			log.Fatal().Err(err).Str("market", string(mc.MarketID)).Msg("create market")
		}
	}

	// InMemBus: engine → all workers without Kafka.
	inMemBus := bus.NewInMemBus()
	go engineservice.NewEventPublisher(inMemBus, log).Run(ctx, multi.AllEvents())

	hub := ws.NewHub()
	go hub.Run()
	go ws.NewBroadcaster(inMemBus.NewConsumer(), hub, log).Run(ctx)

	marketCache := buildMarketCache(marketCfgs)

	settlementHandler := settlementworker.New(pool, orderStore, log)
	go workers.NewWorkerRunner("settlement", "market-events", pool, inMemBus.NewConsumer(), settlementHandler, log).Run(ctx)

	portfolioHandler := portfolioworker.New(pool, rdb, marketCache, log)
	go workers.NewWorkerRunner("portfolio", "market-events", pool, inMemBus.NewConsumer(), portfolioHandler, log).Run(ctx)

	lbHandler, err := lbworker.New(pool, rdb, marketCache, log)
	if err != nil {
		log.Fatal().Err(err).Msg("create leaderboard handler")
	}
	go workers.NewWorkerRunner("leaderboard", "market-events", pool, inMemBus.NewConsumer(), lbHandler, log).Run(ctx)

	feedHandler := feedworker.New(hub, rdb, log)
	go workers.NewWorkerRunner("feed", "market-events", pool, inMemBus.NewConsumer(), feedHandler, log).Run(ctx)

	engineAdapter := gatewayclient.NewDirectAdapter(multi, marketCfgs, orderStore)
	deps := &gateway.Deps{
		PG:          pool,
		Redis:       rdb,
		Engine:      engineAdapter,
		Hub:         hub,
		OrderStore:  orderStore,
		WalletStore: walletStore,
		Cfg:         cfg,
		Log:         log,
	}
	go gateway.RunWithDeps(ctx, deps)

	log.Info().Msg("clob-server running (ROLE=all)")
	<-ctx.Done()
	log.Info().Msg("shutting down")
	multi.Close()
	inMemBus.Close()
}

func buildMarketCache(cfgs []clobconfig.MarketConfig) map[string]clobconfig.MarketConfig {
	m := make(map[string]clobconfig.MarketConfig, len(cfgs))
	for _, c := range cfgs {
		m[string(c.MarketID)] = c
	}
	return m
}
