CREATE TABLE orders (
  order_id    TEXT PRIMARY KEY,
  user_id     TEXT NOT NULL,
  market_id   TEXT NOT NULL,
  side        TEXT NOT NULL CHECK (side IN ('bid','ask')),
  order_type  TEXT NOT NULL,
  price       BIGINT,
  stop_price  BIGINT,
  orig_qty    BIGINT NOT NULL,
  remain_qty  BIGINT NOT NULL,
  filled_qty  BIGINT NOT NULL DEFAULT 0,
  display_qty BIGINT,
  status      TEXT NOT NULL,
  tif         TEXT NOT NULL,
  flags       INT NOT NULL DEFAULT 0,
  expire_at   TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX orders_user_market   ON orders (user_id, market_id);
CREATE INDEX orders_market_status ON orders (market_id, status) WHERE status = 'rested';
