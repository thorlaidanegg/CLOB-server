CREATE TABLE dead_letter_events (
  id          BIGSERIAL PRIMARY KEY,
  worker_name TEXT NOT NULL,
  market_id   TEXT NOT NULL,
  seq_num     BIGINT NOT NULL DEFAULT 0,
  event_type  TEXT NOT NULL,
  payload     BYTEA NOT NULL,
  error       TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX dead_letter_worker ON dead_letter_events (worker_name, created_at DESC);
