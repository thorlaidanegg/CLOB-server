package engineservice

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	"github.com/thorlaidanegg/clob/engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Run starts the engine service: loads markets, wires the hook, starts gRPC, publishes events.
func Run(ctx context.Context, cfg *srvconfig.Config, log zerolog.Logger) {
	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("engine: connect postgres")
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("engine: run migrations")
	}

	rdb, err := redisstore.Connect(cfg.RedisAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("engine: connect redis")
	}

	walletStore := wallet.NewPgStore(pool, 2)
	orderStore := ordersstore.NewPgStore(pool)
	positions := NewPgPositionReader(pool)
	hook := NewPostgresWalletHook(walletStore, orderStore, positions, rdb, log)

	marketCfgs, err := LoadMarkets(ctx, pool)
	if err != nil {
		log.Fatal().Err(err).Msg("engine: load markets")
	}

	// Restart recovery. Must run before the engine accepts new commands.
	// "replay" (default): rebuild each market's book from its event-log checkpoint,
	// folding the Kafka tail when brokers are configured. "cancel": drop open
	// orders and release reservations.
	var recovered map[string][]engine.RecoveredOrder
	var initialEventSeq map[string]uint64
	if cfg.EngineRecovery == "cancel" {
		RecoverOpenOrders(ctx, pool, marketCfgs, log)
	} else {
		var rc bus.Consumer
		if len(cfg.KafkaBrokers) > 0 {
			if c, err := bus.NewKafkaRecoveryConsumer(cfg.KafkaBrokers); err != nil {
				log.Error().Err(err).Msg("engine: recovery consumer; recovering from checkpoint only")
			} else {
				rc = c
			}
		}
		recovered, initialEventSeq = RecoverReplay(ctx, pool, marketCfgs, rc, log)
		if rc != nil {
			rc.Close()
		}
	}

	// Volume cache backs tiered fee markets; refreshed every minute from the trades table.
	volumeCache := NewVolumeCache(pool, marketCfgs, log)
	go volumeCache.Run(ctx, time.Minute)

	multi := engine.NewMultiEngine()
	for _, mc := range marketCfgs {
		id := string(mc.MarketID)
		// Continue the event sequence above the last recovered event so new events
		// never collide with worker idempotency (which skips seq <= last seen).
		if s, ok := initialEventSeq[id]; ok && s > 0 {
			mc.InitialEventSeq = s
		}
		opts := []engine.Option{
			engine.WithPreOrderHook(hook),
			engine.WithFeeCalculator(FeeCalculatorFor(mc, volumeCache)),
		}
		if ro := recovered[id]; len(ro) > 0 {
			opts = append(opts, engine.WithInitialOrders(ro))
		}
		if err := multi.CreateMarket(mc, opts...); err != nil {
			log.Fatal().Err(err).Str("market", string(mc.MarketID)).Msg("engine: create market")
		}
	}

	var producer bus.Producer
	if len(cfg.KafkaBrokers) > 0 {
		kp, err := bus.NewKafkaProducer(cfg.KafkaBrokers)
		if err != nil {
			log.Fatal().Err(err).Msg("engine: kafka producer")
		}
		producer = kp
	} else {
		producer = bus.NewInMemBus()
	}

	publisher := NewEventPublisher(producer, log)
	go publisher.Run(ctx, multi.AllEvents())

	// Lets markets be created at runtime (POST /v1/markets) and registered with
	// the live engine without a restart.
	creator := NewMarketCreator(ctx, multi, pool, hook, volumeCache, publisher, log)

	// Markets are created in PreOpen; open the ones marked open in the DB so
	// orders actually match (otherwise they only rest).
	ResumeOpenMarkets(ctx, multi, pool, log)

	// Start gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.EngineGRPCPort))
	if err != nil {
		log.Fatal().Err(err).Msg("engine: listen grpc")
	}
	var grpcOpts []grpc.ServerOption
	if cfg.GRPCTLSCertFile != "" && cfg.GRPCTLSKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(cfg.GRPCTLSCertFile, cfg.GRPCTLSKeyFile)
		if err != nil {
			log.Fatal().Err(err).Msg("engine: load gRPC TLS cert")
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
		log.Info().Msg("engine: gRPC TLS enabled")
	} else {
		log.Warn().Msg("engine: gRPC serving plaintext (no TLS cert configured)")
	}

	grpcSrv := grpc.NewServer(grpcOpts...)
	enginev1.RegisterEngineServiceServer(grpcSrv, NewEngineServer(multi, marketCfgs, creator, log))
	log.Info().Int("port", cfg.EngineGRPCPort).Msg("engine: gRPC server starting")

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Error().Err(err).Msg("engine: gRPC server stopped")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("engine: shutting down")
	grpcSrv.GracefulStop()
	multi.Close()
	producer.Close()
}
