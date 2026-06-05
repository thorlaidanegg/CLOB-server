#!/usr/bin/env bash
#
# Seeds a fresh exchange so the UI isn't empty: a couple of markets + a demo user.
#
# Markets are read from Postgres at ENGINE STARTUP, and the admin API only inserts
# the row — so after creating markets we restart the engine to load them live.
#
# Usage (with the stack already up via `docker compose up`):
#   ./scripts/seed.sh
#
# Env overrides: BASE (gateway URL), ADMIN_KEY (admin bootstrap key).
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
ADMIN_KEY="${ADMIN_KEY:-clob_admin_devbootstrapkeylocal}"
DEMO_EMAIL="${DEMO_EMAIL:-demo@clob.dev}"
DEMO_PASSWORD="${DEMO_PASSWORD:-demodemo}"

post() { # post <path> <json>
  curl -fsS -X POST "$BASE$1" \
    -H "Authorization: Bearer $ADMIN_KEY" \
    -H "Content-Type: application/json" \
    -d "$2" >/dev/null
}

# Market config notes:
#   tickSize/lotSize/fee rates are RAW integers at the field's precision.
#   tickSize=1 @ pricePrecision 2 => 0.01 ; lotSize=1 @ qtyPrecision 4 => 0.0001
#   features=127 = all order-type features (market,IOC,FOK,stop,iceberg,post_only,reduce_only); 128=auctions left off (needs auction cfg)
#   makerFeeRate=-10 => -0.10% rebate ; takerFeeRate=20 => 0.20%
market_json() { # market_json <id> <base> <quote>
  cat <<EOF
{"marketID":"$1","baseAsset":"$2","quoteAsset":"$3",
 "pricePrecision":2,"qtyPrecision":4,"tickSize":1,"lotSize":1,
 "minOrderQty":0,"maxOrderQty":0,"maxOrderValue":0,"maxDepth":0,
 "features":127,"stpMode":"","makerFeeRate":-10,"takerFeeRate":20,
 "feeCurrency":"$3","feeModel":"flat"}
EOF
}

echo "→ Creating markets…"
post /v1/admin/markets "$(market_json BTC-USD BTC USD)" && echo "  BTC-USD"
post /v1/admin/markets "$(market_json ETH-USD ETH USD)" && echo "  ETH-USD"
post /v1/admin/markets "$(market_json SOL-USD SOL USD)" && echo "  SOL-USD"

# Markets are cached at process start by the engine AND every projection worker,
# so restart them all to pick up the new markets (matches the engine's load-at-boot model).
echo "→ Restarting engine + workers to load the new markets…"
docker compose restart engine settlement portfolio leaderboard feed booksnapshot >/dev/null
sleep 4

echo "→ Creating demo user ($DEMO_EMAIL / $DEMO_PASSWORD) with starter credits…"
curl -fsS -X POST "$BASE/v1/auth/signup" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"$DEMO_EMAIL\",\"password\":\"$DEMO_PASSWORD\"}" >/dev/null \
  && echo "  created" || echo "  (already exists — skipping)"

cat <<EOF

✓ Seed complete.
  Markets:  BTC-USD, ETH-USD, SOL-USD
  Login:    $DEMO_EMAIL / $DEMO_PASSWORD  (frontend: http://localhost:5173)

  To make a user an admin (for the admin tab), flip the flag in Postgres:
    UPDATE users SET is_admin = true WHERE email = '$DEMO_EMAIL';
EOF
