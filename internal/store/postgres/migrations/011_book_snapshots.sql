-- book_snapshots holds the latest compacted resting-book state per market, used
-- to bound crash-recovery replay. The state blob is the folded market-events log
-- (see internal/bookstate); kafka_offset/partition is the safe lower-bound seek
-- point for replaying the tail. One row per market (latest wins).
CREATE TABLE book_snapshots (
  market_id       TEXT PRIMARY KEY,
  last_event_seq  BIGINT NOT NULL DEFAULT 0,
  kafka_offset    BIGINT NOT NULL DEFAULT 0,
  kafka_partition INT NOT NULL DEFAULT 0,
  state           BYTEA NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
