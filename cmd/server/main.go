package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
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
	booksnapshotworker "github.com/thorlaidanegg/clob-server/internal/workers/booksnapshot"
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
	case "booksnapshot":
		booksnapshotworker.Run(ctx, cfg, log)
	case "all":
		runAll(ctx, cfg)
	default:
		log.Fatal().Str("role", cfg.Role).Msg("unknown ROLE — valid: engine, gateway, settlement, portfolio, leaderboard, feed, booksnapshot, all")
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
	positions := engineservice.NewPgPositionReader(pool)
	hook := engineservice.NewPostgresWalletHook(walletStore, orderStore, positions, rdb, log)

	// Restart recovery. The book_snapshots checkpoint is durable in Postgres even
	// though the in-memory bus loses events, so ROLE=all rebuilds from the last
	// checkpoint (no Kafka tail available here). ENGINE_RECOVERY=cancel opts out.
	var recovered map[string][]engine.RecoveredOrder
	var initialEventSeq map[string]uint64
	if cfg.EngineRecovery == "cancel" {
		engineservice.RecoverOpenOrders(ctx, pool, marketCfgs, log)
	} else {
		recovered, initialEventSeq = engineservice.RecoverReplay(ctx, pool, marketCfgs, nil, log)
	}

	// Volume cache backs tiered fee markets.
	volumeCache := engineservice.NewVolumeCache(pool, marketCfgs, log)
	go volumeCache.Run(ctx, time.Minute)

	multi := engine.NewMultiEngine()
	for _, mc := range marketCfgs {
		id := string(mc.MarketID)
		if s, ok := initialEventSeq[id]; ok && s > 0 {
			mc.InitialEventSeq = s
		}
		opts := []engine.Option{
			engine.WithPreOrderHook(hook),
			engine.WithFeeCalculator(engineservice.FeeCalculatorFor(mc, volumeCache)),
		}
		if ro := recovered[id]; len(ro) > 0 {
			opts = append(opts, engine.WithInitialOrders(ro))
		}
		if err := multi.CreateMarket(mc, opts...); err != nil {
			log.Fatal().Err(err).Str("market", string(mc.MarketID)).Msg("create market")
		}
	}

	// InMemBus: engine → all workers without Kafka.
	inMemBus := bus.NewInMemBus()
	publisher := engineservice.NewEventPublisher(inMemBus, log)
	go publisher.Run(ctx, multi.AllEvents())
	marketCreator := engineservice.NewMarketCreator(ctx, multi, pool, hook, volumeCache, publisher, log)

	// Open markets flagged open in the DB (engine creates them in PreOpen).
	engineservice.ResumeOpenMarkets(ctx, multi, pool, log)

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

	// WS fan-out to clients is handled by the Broadcaster above; the separate feed
	// worker would double-deliver in this single-process mode, so it is omitted here.

	// Book-snapshot checkpoints. In ROLE=all the in-memory bus loses events on
	// restart, so recovery still falls back to cancel; the worker keeps the
	// book_snapshots table populated for parity with the split deployment.
	bsHandler := booksnapshotworker.New(log)
	if err := bsHandler.LoadSnapshots(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("load book snapshots")
	}
	go workers.NewWorkerRunner("booksnapshot", "market-events", pool, inMemBus.NewConsumer(), bsHandler, log).Run(ctx)

	createMarket := func(_ context.Context, req gatewayclient.CreateMarketRequest) (clobconfig.MarketConfig, gatewayclient.CreateMarketResponse, error) {
		cfg, state, err := marketCreator.Create(engineservice.CreateParams{
			MarketID:       req.MarketID,
			Auction:        req.Auction,
			PreOpen:        time.Duration(req.AuctionPreOpenMs) * time.Millisecond,
			ReferencePrice: req.ReferencePrice,
		})
		return cfg, gatewayclient.CreateMarketResponse{Created: state != "exists", State: state}, err
	}
	engineAdapter := gatewayclient.NewDirectAdapter(multi, marketCfgs, orderStore, createMarket)
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
