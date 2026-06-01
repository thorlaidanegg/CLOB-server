package settlement

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	srvconfig "github.com/thorlaidanegg/clob-server/internal/shared/config"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/workers"
)

// Run starts the settlement worker for the given role configuration.
func Run(ctx context.Context, cfg *srvconfig.Config, log zerolog.Logger) {
	pool, err := pgstore.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("settlement: connect postgres")
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		log.Fatal().Err(err).Msg("settlement: run migrations")
	}

	consumer, err := bus.NewKafkaConsumer(cfg.KafkaBrokers, "settlement")
	if err != nil {
		log.Fatal().Err(err).Msg("settlement: kafka consumer")
	}
	defer consumer.Close()

	orderStore := ordersstore.NewPgStore(pool)
	handler := New(pool, orderStore, log)
	runner := workers.NewWorkerRunner("settlement", "market-events", pool, consumer, handler, log)
	log.Info().Msg("settlement worker starting")
	runner.Run(ctx)
}
