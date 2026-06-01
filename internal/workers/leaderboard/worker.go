package leaderboard

import (
	"context"

	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	"github.com/thorlaidanegg/clob-server/internal/engineservice"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/workers"
)

// Run starts the leaderboard worker.
func Run(ctx context.Context, cfg *srvconfig.Config, log zerolog.Logger) {
	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("leaderboard: connect postgres")
	}

	rdb, err := redisstore.Connect(cfg.RedisAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("leaderboard: connect redis")
	}

	consumer, err := bus.NewKafkaConsumer(cfg.KafkaBrokers, "leaderboard")
	if err != nil {
		log.Fatal().Err(err).Msg("leaderboard: kafka consumer")
	}
	defer consumer.Close()

	marketCfgs, err := engineservice.LoadMarkets(ctx, pool)
	if err != nil {
		log.Fatal().Err(err).Msg("leaderboard: load markets")
	}

	mc := make(map[string]clobconfig.MarketConfig, len(marketCfgs))
	for _, m := range marketCfgs {
		mc[string(m.MarketID)] = m
	}

	handler, err := New(pool, rdb, mc, log)
	if err != nil {
		log.Fatal().Err(err).Msg("leaderboard: init handler")
	}

	runner := workers.NewWorkerRunner("leaderboard", "market-events", pool, consumer, handler, log)
	log.Info().Msg("leaderboard worker starting")
	runner.Run(ctx)
}
