package engineservice

import (
	"context"
	"fmt"
	"net"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/fees"
	"google.golang.org/grpc"
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	walletStore := wallet.NewPgStore(pool, 2)
	orderStore := ordersstore.NewPgStore(pool)
	hook := NewPostgresWalletHook(walletStore, orderStore, rdb, log)

	marketCfgs, err := LoadMarkets(ctx, pool)
	if err != nil {
		log.Fatal().Err(err).Msg("engine: load markets")
	}

	multi := engine.NewMultiEngine()
	for _, mc := range marketCfgs {
		opts := []engine.Option{
			engine.WithPreOrderHook(hook),
			engine.WithFeeCalculator(fees.FlatRateFeeCalculator{}),
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

	// Start gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.EngineGRPCPort))
	if err != nil {
		log.Fatal().Err(err).Msg("engine: listen grpc")
	}
	grpcSrv := grpc.NewServer()
	enginev1.RegisterEngineServiceServer(grpcSrv, NewEngineServer(multi, marketCfgs, log))
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
