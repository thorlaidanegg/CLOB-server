CREATE TABLE wallets (
  user_id    TEXT PRIMARY KEY,
  available  BIGINT NOT NULL DEFAULT 0,
  reserved   BIGINT NOT NULL DEFAULT 0,
  precision  SMALLINT NOT NULL DEFAULT 2,
  version    BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT available_non_negative CHECK (available >= 0),
  CONSTRAINT reserved_non_negative  CHECK (reserved  >= 0)
);
