package portfolio

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	"github.com/thorlaidanegg/clob-server/internal/engineservice"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/workers"
)

// Run starts the portfolio worker.
func Run(ctx context.Context, cfg *srvconfig.Config, log zerolog.Logger) {
	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("portfolio: connect postgres")
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("portfolio: run migrations")
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	consumer, err := bus.NewKafkaConsumer(cfg.KafkaBrokers, "portfolio")
	if err != nil {
		log.Fatal().Err(err).Msg("portfolio: kafka consumer")
	}
	defer consumer.Close()

	marketCfgs, err := engineservice.LoadMarkets(ctx, pool)
	if err != nil {
		log.Fatal().Err(err).Msg("portfolio: load markets")
	}

	mc := make(map[string]clobconfig.MarketConfig, len(marketCfgs))
	for _, m := range marketCfgs {
		mc[string(m.MarketID)] = m
	}

	handler := New(pool, rdb, mc, log)
	runner := workers.NewWorkerRunner("portfolio", "market-events", pool, consumer, handler, log)
	log.Info().Msg("portfolio worker starting")
	runner.Run(ctx)
}
