CREATE TABLE api_keys (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id      TEXT NOT NULL REFERENCES users(user_id),
  key_hash     TEXT NOT NULL UNIQUE,
  key_prefix   TEXT NOT NULL,
  name         TEXT,
  scopes       TEXT[] NOT NULL DEFAULT '{}',
  tier         TEXT NOT NULL DEFAULT 'standard',
  rate_limit   INT NOT NULL DEFAULT 300,
  last_used_at TIMESTAMPTZ,
  expires_at   TIMESTAMPTZ,
  revoked      BOOLEAN NOT NULL DEFAULT FALSE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX api_keys_user ON api_keys (user_id);
CREATE INDEX api_keys_hash ON api_keys (key_hash);
