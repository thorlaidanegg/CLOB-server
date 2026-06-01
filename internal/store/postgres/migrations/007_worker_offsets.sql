CREATE TABLE worker_offsets (
  worker_name     TEXT NOT NULL,
  market_id       TEXT NOT NULL,
  last_event_seq  BIGINT NOT NULL DEFAULT 0,
  kafka_offset    BIGINT NOT NULL DEFAULT 0,
  kafka_partition INT NOT NULL DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (worker_name, market_id)
);
