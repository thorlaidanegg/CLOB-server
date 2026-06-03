package booksnapshot

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/workers"
)

// Run starts the book-snapshot checkpoint worker.
func Run(ctx context.Context, cfg *srvconfig.Config, log zerolog.Logger) {
	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("booksnapshot: connect postgres")
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("booksnapshot: run migrations")
	}

	consumer, err := bus.NewKafkaConsumer(cfg.KafkaBrokers, "booksnapshot")
	if err != nil {
		log.Fatal().Err(err).Msg("booksnapshot: kafka consumer")
	}

	handler := New(log)
	if err := handler.LoadSnapshots(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("booksnapshot: load checkpoints")
	}

	runner := workers.NewWorkerRunner("booksnapshot", "market-events", pool, consumer, handler, log)
	log.Info().Msg("booksnapshot worker starting")
	runner.Run(ctx)
}
