-- Browser auth: email/password login issuing a JWT session cookie. Bots keep
-- using API keys; both resolve to the same AuthContext.
ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN is_admin       BOOLEAN NOT NULL DEFAULT false;
