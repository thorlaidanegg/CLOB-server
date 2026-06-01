CREATE TABLE positions (
  user_id         TEXT NOT NULL,
  market_id       TEXT NOT NULL,
  quantity        BIGINT NOT NULL DEFAULT 0,
  avg_entry_price BIGINT NOT NULL DEFAULT 0,
  realised_pnl    BIGINT NOT NULL DEFAULT 0,
  last_event_seq  BIGINT NOT NULL DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, market_id)
);
