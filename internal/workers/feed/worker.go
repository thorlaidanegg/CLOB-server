package feed

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ws"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/workers"
)

// Run starts the feed worker (standalone mode — connects to gateway hub via Redis pub/sub in future).
// In ROLE=feed within the same process it shares a hub directly; standalone feed is for future use.
func Run(ctx context.Context, cfg *srvconfig.Config, log zerolog.Logger) {
	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("feed: connect postgres")
	}

	rdb, err := redisstore.Connect(cfg.RedisAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("feed: connect redis")
	}

	consumer, err := bus.NewKafkaConsumer(cfg.KafkaBrokers, "feed")
	if err != nil {
		log.Fatal().Err(err).Msg("feed: kafka consumer")
	}
	// WorkerRunner closes the consumer on shutdown.

	// Standalone feed creates its own hub; in a multi-gateway setup this would
	// publish to Redis pub/sub which gateway instances subscribe to.
	hub := ws.NewHub()
	go hub.Run()

	handler := New(hub, rdb, log)
	runner := workers.NewWorkerRunner("feed", "market-events", pool, consumer, handler, log)
	log.Info().Msg("feed worker starting")
	runner.Run(ctx)
}
