# clob-server

A complete, self-hostable **paper-trading platform** built on top of the
[`clob`](https://github.com/thorlaidanegg/clob) matching-engine library.

It wraps the engine with everything a real backend needs — REST + WebSocket APIs,
API-key auth, virtual-credit wallets, position & PnL tracking, a leaderboard, a
market-data feed, Postgres persistence, Redis caching, and a Kafka event bus —
shipped as a **single binary** whose behaviour is selected by one environment
variable (`ROLE`).

```
ROLE=all docker compose up      # everything in one process, no Kafka needed
```

> **Scope:** virtual credits only. No real money, margin, or leverage. The
> architecture is deliberately designed so a real-money layer can be added later
> by swapping two components (the pre-order hook and the settlement handler)
> without touching the engine, the API, or any other service.

---

## Table of contents

- [Architecture](#architecture)
- [Features](#features)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API](#api)
- [Wallet & settlement model](#wallet--settlement-model)
- [Data stores](#data-stores)
- [Deployment models](#deployment-models)
- [Development](#development)
- [Project layout](#project-layout)
- [Tech stack](#tech-stack)
- [Roadmap](#roadmap)

---

## Architecture

The system is split into independently deployable **roles**. Commands flow one
way (client → gateway → engine); events flow the other way (engine → bus →
workers). **No worker ever calls another worker or the engine** — every
component communicates only through the event bus. This is what makes each piece
independently scalable and testable.

```
                ┌──────────────────────────────────────┐
                │   Clients (browser · bot · mobile)    │
                └──────────────────┬───────────────────┘
                                   │ REST + WebSocket
                ┌──────────────────▼───────────────────┐
                │              GATEWAY                  │
                │  auth · rate-limit · normalizer       │
                │  REST · WebSocket · admin · /metrics  │
                └──────────────────┬───────────────────┘
                                   │ gRPC (PlaceOrder, Cancel, GetDepth, …)
                ┌──────────────────▼───────────────────┐
                │           ENGINE SERVICE              │
                │  PreOrderHook → credit reservation    │
                │  ┌─────────────────────────────────┐  │
                │  │   clob library (in-memory book)  │  │
                │  └─────────────────────────────────┘  │
                │  Event publisher → bus                │
                └──────────────────┬───────────────────┘
                                   │ market-events  (Kafka, partitioned by marketID)
        ┌──────────────┬───────────┴──────────┬──────────────┐
        ▼              ▼                      ▼              ▼
  ┌───────────┐  ┌───────────┐         ┌────────────┐  ┌──────────┐
  │ Settlement│  │ Portfolio │         │ Leaderboard│  │   Feed   │
  │  wallets  │  │ positions │         │   Redis ZSET│  │  WebSocket│
  └─────┬─────┘  └─────┬─────┘         └─────┬──────┘  └────┬─────┘
     Postgres       Postgres               Redis        gateway WS
```

### Roles

| `ROLE`        | Responsibility |
|---------------|----------------|
| `engine`      | Runs the matching engine, the credit-reservation hook, the gRPC server, and publishes events to the bus. |
| `gateway`     | The only public process. REST + WebSocket API, auth, rate limiting, admin routes, Prometheus metrics. Talks to the engine over gRPC. |
| `settlement`  | Consumes fills/cancels → updates virtual-credit wallets (idempotently). |
| `portfolio`   | Consumes fills → updates positions, average entry price, realised PnL. |
| `leaderboard` | Consumes sell fills → maintains a Redis sorted set of realised PnL. |
| `feed`        | Consumes all events → fans them out to subscribed WebSocket clients. |
| `booksnapshot`| Folds the event log into a per-market resting-book **checkpoint** (durable in Postgres), bounding crash-recovery replay. |
| `all`         | Runs every role in one process using an **in-memory bus** (no Kafka). Ideal for local dev and small deployments. |

### Event flow & idempotency

The engine emits a sequence-numbered event stream. Each worker tracks the last
processed sequence per market in a `worker_offsets` table, **updated in the same
Postgres transaction as its business write**. A crash at any point is safe:
on restart the worker either replays an un-committed event (re-applied with the
same result) or skips an already-applied one (sequence guard). Settlement and
portfolio are therefore exactly-once in effect.

---

## Features

- **Order types:** limit, market, stop, stop-limit, iceberg; TIF GTC/IOC/FOK/GTD/DAY.
- **Fixed-point money everywhere** — `types.Decimal` in Go, `BIGINT` in Postgres,
  decimal strings on the wire. Never `float64`.
- **Virtual-credit wallets** with `available`/`reserved` balances and DB-level
  non-negativity constraints.
- **Long-only spot model** with a precise, documented settlement spec
  (see [`doc/WALLET_MODEL.md`](doc/WALLET_MODEL.md)).
- **Positions & PnL** — volume-weighted average entry, realised PnL on sells,
  unrealised PnL computed on read from the last trade price.
- **Leaderboard** by realised PnL (global + per-market), updated in real time.
- **API-key auth** — SHA-256 hashed keys, Redis-cached (60s) with Postgres
  fallback, scopes and per-key rate limits.
- **WebSocket** — single connection for both inbound order commands and outbound
  fills/depth/portfolio streams.
- **Tiered fees** — volume-based fee tiers backed by a 30-day-volume cache
  refreshed from the `trades` table (engine hot-path never hits the DB).
- **Crash recovery (event-sourced)** — on restart the engine rebuilds each
  market's resting book by folding its own `market-events` log from the last
  checkpoint, so open orders survive. See [Crash recovery](#crash-recovery).
- **Observability** — Prometheus metrics at `/metrics`, structured zerolog logs
  with request IDs.
- **Optional gRPC TLS** between gateway and engine; configurable CORS for browser
  frontends.

---

## Quick start

### Prerequisites

- [Docker](https://www.docker.com/) + Docker Compose
- (For local Go builds) Go 1.25+

### Run the whole stack

```bash
git clone https://github.com/thorlaidanegg/clob-server
cd clob-server
cp .env.example .env
docker compose up --build
```

The API is now on `http://localhost:8080`. A bootstrap admin key
(`ADMIN_BOOTSTRAP_KEY` in `.env`) is created on first run.

### Smoke test

```bash
ADMIN="clob_admin_devbootstrapkeylocal"

# Create a market
curl -X POST localhost:8080/v1/admin/markets -H "Authorization: Bearer $ADMIN" \
  -d '{"marketID":"BTC-USD","baseAsset":"BTC","quoteAsset":"USD",
       "pricePrecision":2,"qtyPrecision":2,"tickSize":1,"lotSize":1,"features":1}'

# Create users and grant credits
curl -X POST localhost:8080/v1/admin/users -H "Authorization: Bearer $ADMIN" \
  -d '{"userID":"alice","email":"alice@example.com"}'
curl -X POST localhost:8080/v1/admin/users/alice/credits -H "Authorization: Bearer $ADMIN" \
  -d '{"amount":"10000.00"}'

# Place an order (using alice's own API key — create one via POST /v1/apikeys)
curl -X POST localhost:8080/v1/orders -H "Authorization: Bearer clob_live_..." \
  -d '{"marketID":"BTC-USD","side":"bid","orderType":"limit","price":"100.00","qty":"5.00","tif":"GTC"}'
```

### Run it as a library (Go SDK)

```go
import "github.com/thorlaidanegg/clob-server/sdk"

srv, _ := sdk.New(sdk.Config{
    Markets:     []clobconfig.MarketConfig{ /* ... */ },
    PostgresDSN: "postgres://...",
    RedisAddr:   "localhost:6379",
    HTTPPort:    8080,
})
srv.SetFeeCalculator("BTC-USD", myFeeCalc) // optional per-market overrides
srv.Start() // blocks
```

---

## Configuration

All configuration is via environment variables (see [`.env.example`](.env.example)).

| Variable | Roles | Default | Description |
|----------|-------|---------|-------------|
| `ROLE` | all | — *(required)* | `engine`/`gateway`/`settlement`/`portfolio`/`leaderboard`/`feed`/`all` |
| `LOG_LEVEL` | all | `info` | `debug`/`info`/`warn`/`error` |
| `ENVIRONMENT` | all | `local` | `local`/`staging`/`production` |
| `POSTGRES_DSN` | most | — | Postgres connection string |
| `REDIS_ADDR` | gateway, workers | `localhost:6379` | Redis address |
| `KAFKA_BROKERS` | engine, workers | — | Comma-separated brokers. **Unset ⇒ in-memory bus** (single process). |
| `ENGINE_GRPC_PORT` | engine | `50051` | gRPC listen port |
| `ENGINE_GRPC_ADDR` | gateway | `localhost:50051` | Engine gRPC address |
| `HTTP_PORT` | gateway | `8080` | HTTP listen port |
| `MARKETS` | engine | — | Comma-separated market IDs to load at startup |
| `RATE_LIMIT_RPM` | gateway | `300` | REST requests/min per key |
| `RATE_LIMIT_WS_RPS` | gateway | `50` | WS messages/sec per connection |
| `ADMIN_BOOTSTRAP_KEY` | gateway | — | If set and no admin key exists, seeds a bootstrap admin key on first run |
| `GRPC_TLS_CERT_FILE` / `GRPC_TLS_KEY_FILE` | engine | — | Serve gRPC over TLS |
| `GRPC_TLS_CA_FILE` | gateway | — | Dial engine over TLS, verifying this CA |
| `CORS_ALLOWED_ORIGINS` | gateway | — | Comma-separated browser origins (`*` for any). Empty = disabled. |

---

## API

Full spec: [`api/openapi.yaml`](api/openapi.yaml). All money values are decimal
strings. Auth via `Authorization: Bearer <key>`.

### REST

| Method & path | Description |
|---------------|-------------|
| `POST /v1/orders` | Place an order (returns `202`; fill arrives via WebSocket) |
| `GET /v1/orders` · `GET /v1/orders/{id}` | List / get your orders |
| `DELETE /v1/orders/{id}` | Cancel an order |
| `GET /v1/markets` · `GET /v1/markets/{id}` | List / get markets *(public)* |
| `GET /v1/markets/{id}/depth` | Order-book snapshot *(public, `?levels=`)* |
| `GET /v1/markets/{id}/trades` | Recent trades *(public, `?limit=`)* |
| `GET /v1/portfolio` | Positions, PnL, wallet balance |
| `GET /v1/leaderboard` | Top traders by realised PnL *(public)* |
| `POST/GET/DELETE /v1/apikeys` | Manage your API keys |
| `GET /health` · `GET /metrics` | Health check · Prometheus metrics |

**Admin** (requires `admin:all` scope): create/halt/resume markets, market
engine stats, create users, grant credits, force-cancel any order.

### WebSocket — `ws://host/v1/stream`

Authenticate within 10s, then subscribe and trade over one connection:

```jsonc
// 1. authenticate
{"type": "auth", "apiKey": "clob_live_..."}        // → {"type":"auth_ok","userID":"..."}

// 2. subscribe to channels
{"type": "subscribe", "channel": "depth:BTC-USD"}  // depth:{m}, trades:{m}, orders:{u}, portfolio:{u}, markets

// 3. place / cancel orders inline
{"type": "place_order", "marketID": "BTC-USD", "side": "bid",
 "orderType": "limit", "price": "100.00", "qty": "5.00", "tif": "GTC"}
{"type": "cancel_order", "orderID": "ord_..."}
```

On auth the connection is auto-subscribed to `orders:{userID}` and
`portfolio:{userID}`.

### gRPC (internal only)

The gateway talks to the engine via `proto/engine/engine.proto`
(`PlaceOrder`, `CancelOrder`, `GetDepth`, `GetBBO`, `GetStats`, `StreamEvents`).
Not exposed publicly.

---

## Wallet & settlement model

The credit/position accounting is specified precisely in
[`doc/WALLET_MODEL.md`](doc/WALLET_MODEL.md). In short — **long-only spot**:

- **Buy** reserves credits (`available → reserved`); on fill the reservation is
  consumed and the real cost is paid, refunding any over-reservation.
- **Sell** reserves nothing but requires you to already hold the position; on
  fill you receive the proceeds.
- Settlement decides credit direction by **`fill.Side`**, never by maker/taker
  role, and every release path keys off `reserved_per_unit > 0`.

---

## Data stores

**Postgres** is the durable store (wallets, positions, orders, markets, users,
api_keys, trades, worker_offsets, dead_letter_events, book_snapshots). Schema is
applied by sequential migrations in `internal/store/postgres/migrations/`
(`001`–`011`) — append-only, never edit an existing migration.

**Redis** is fast/rebuildable cache: API-key auth cache (60s TTL), per-user rate
limits, BBO cache, last trade price, and the leaderboard sorted sets.

**Kafka** (`market-events`, partitioned by `marketID`) is the event log between
the engine and the workers — and the source of truth for crash recovery. In
`ROLE=all` it is replaced by an in-memory bus, so no Kafka is required for
single-process deployments.

---

## Crash recovery

The matching engine holds each market's order book in memory for speed. The
question that matters is what happens when the engine process dies. The answer is
**event-sourced replay**, consistent with the architecture's own rule that the
`market-events` log — not Postgres — is the source of truth:

1. **Checkpointing.** The `booksnapshot` worker consumes `market-events` and folds
   it into a per-market resting-book state (`internal/bookstate`), persisting a
   compacted **checkpoint** to the `book_snapshots` table. The fold is a pure
   function of the event stream and is replay-idempotent (events at or below the
   checkpoint's sequence are skipped). The checkpoint bounds how far recovery has
   to replay — the scalability knob.
2. **Recovery.** On startup the engine loads the latest checkpoint per market and,
   in the Kafka deployment, folds any events past it, then seeds the rebuilt
   resting orders into the book via the library's `WithInitialOrders` option —
   placed directly, without re-matching, without re-running the credit hook, and
   without re-emitting events. The engine's event sequence is resumed **above** the
   last recovered event so new events never collide with worker idempotency.
   **Reserved credits never moved during the crash, so recovery touches no wallets.**

Open orders survive a restart — no mass cancellation.

**Honest limits.** Recovery is consistent up to the last checkpointed (and
tail-folded) event; iceberg hidden-quantity internals are approximated from fill
remainders. `ROLE=all` has no durable event log, so it recovers from the Postgres
checkpoint alone (the `booksnapshot` worker checkpoints every event, so the gap is
negligible) and can be set to the old behaviour with `ENGINE_RECOVERY=cancel`. A
fully bit-exact rebuild would use a command write-ahead log (see [Roadmap](#roadmap)).

---

## Deployment models

| Mode | How | When |
|------|-----|------|
| **All-in-one** | `ROLE=all`, in-memory bus | Local dev, demos, small instances. Just Postgres + Redis. |
| **Single-VM split** | `docker-compose.prod.yml` — each role a container, one Redpanda/Postgres/Redis | Beta / early production on one box. |
| **Multi-VM** | Engine and gateway on separate hosts; gateways behind a sticky-session LB (WebSocket); managed Postgres/Redis/Kafka | Horizontal scale. |

> `ROLE=all` keeps the order book in memory with no Kafka. The book still recovers
> from the durable Postgres checkpoint on restart, but without a durable event log
> the last few un-checkpointed events can't be tail-folded — fine for dev, not prod.

---

## Development

### Build, test, lint

```bash
go build ./...        # build
go vet ./...          # static checks
go test ./...         # unit tests (+ no-infra engine integration tests)
```

Integration tests that need Postgres are **gated behind `TEST_POSTGRES_DSN`** and
skip cleanly when it is unset. To run them:

```bash
make test-all         # spins up an ephemeral Postgres, runs everything, tears down
# or manually:
make test-db-up
TEST_POSTGRES_DSN="postgres://clob:clob@localhost:55432/clob_test?sslmode=disable" go test ./...
make test-db-down
```

### CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push/PR:
`go vet` → `go build` → `go test -race`, with a Postgres service container so the
integration tests run for real. It pulls the published `clob` module from the Go
proxy — no special setup.

### Dependency on the `clob` library

`clob-server` depends on the **published** module
`github.com/thorlaidanegg/clob` (currently `v0.5.0`) — there is no `replace`
directive. To bump it:

```bash
go get github.com/thorlaidanegg/clob@vX.Y.Z && go mod tidy
```

For local co-development of both repos, add a temporary
`replace github.com/thorlaidanegg/clob => ../clob` to `go.mod` (and remove it
before committing).

### Regenerating gRPC code

Generated files (`proto/engine/v1/*.pb.go`) are committed. Regenerate only if you
change the proto:

```bash
protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative \
  --go-grpc_opt=paths=source_relative proto/engine/engine.proto
```

---

## Project layout

```
cmd/server/            ROLE switch, dependency wiring, startup
internal/
  engineservice/       gRPC server, PreOrderHook, event publisher, market loader,
                       volume cache + fee selection, restart recovery
  gateway/             HTTP/WS server, auth, rate limiting, normalizer,
                       REST + admin handlers, gRPC client / in-process adapter
  workers/             base runner + settlement / portfolio / leaderboard / feed
  wallet/              wallet Store interface + Postgres impl
  store/postgres/      connection, migrations, per-table queries
  store/redis/         cache, leaderboard, rate-limit, connection
  bus/                 Producer/Consumer interfaces, Kafka + in-memory impls
  shared/              config, logger, apierrors, metrics
  testsupport/         integration-test helpers (advisory-lock isolation)
sdk/                   programmatic Go embedding API
proto/engine/          gRPC contract + generated code
api/openapi.yaml       REST/WebSocket API spec
doc/WALLET_MODEL.md    credit/settlement accounting spec
```

---

## Tech stack

Go 1.25 · [clob](https://github.com/thorlaidanegg/clob) matching engine ·
pgx/v5 (Postgres) · go-redis/v9 · franz-go (Kafka) · gRPC + protobuf ·
chi/v5 (HTTP) · coder/websocket · zerolog · Prometheus client.

---

## Roadmap

- **Bounded-replay seek.** Today recovery folds the retained event log from the
  start (idempotent, but re-reads checkpointed events). Implementing a real
  per-partition Kafka seek to the checkpoint offset makes replay strictly bounded
  by the checkpoint interval at large log sizes.
- **Command write-ahead log / bit-exact recovery.** Commands are written to a
  `market-commands` Kafka topic *before* the engine processes them; on restart the
  engine deterministically replays commands to reconstruct exact book state
  (re-running matching). Heavier than the current event-replay recovery — changes
  the command-ingest path — but bit-exact. The library, workers, gateway, and API
  contract are unchanged.
- **Real-money path.** Replace the pre-order hook (credit check) and the
  settlement handler (credit movement) with custodian/payment-rail integrations;
  add on-ramp/withdrawal services. Everything else stays the same.

---

## License

See repository.
