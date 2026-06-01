CREATE TABLE trades (
  trade_id       TEXT NOT NULL,
  market_id      TEXT NOT NULL,
  maker_order_id TEXT NOT NULL,
  taker_order_id TEXT NOT NULL,
  maker_user_id  TEXT NOT NULL,
  taker_user_id  TEXT NOT NULL,
  maker_side     TEXT NOT NULL CHECK (maker_side IN ('bid','ask')),
  price          BIGINT NOT NULL,
  qty            BIGINT NOT NULL,
  maker_fee      BIGINT NOT NULL DEFAULT 0,
  taker_fee      BIGINT NOT NULL DEFAULT 0,
  fee_currency   TEXT,
  seq_num        BIGINT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (trade_id, market_id)
);
CREATE INDEX trades_market_created ON trades (market_id, created_at DESC);
