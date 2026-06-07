# Deploying the CLOB platform

The cheapest setup that recruiters can actually click on, and that works well:

| Piece | Where | Cost |
|---|---|---|
| **Backend** (`ROLE=all`) + Postgres + Redis + Caddy + SPA | one small VM | ~€4/mo (Hetzner CX22) — or **free** (Oracle Cloud Always-Free ARM) |
| **Docs** (Mintlify) | Mintlify Cloud / Vercel | free |
| **Bots** (optional) | same VM | included |

Everything sits behind **one domain** with automatic HTTPS. No Kafka, no
multi-service orchestration — the whole backend runs as a single process
(`ROLE=all`: engine + gateway + settlement/portfolio/leaderboard workers + the
WebSocket broadcaster on an in-memory bus). That's the trick that makes it cheap.

> Trade-off: `ROLE=all` is single-process and the bus is in-memory, so events
> aren't durable across a restart (the book is rebuilt from the Postgres
> checkpoint; otherwise open orders are cancelled). Great for a portfolio demo.
> For the durable, horizontally-scalable version use [`../docker-compose.prod.yml`](../docker-compose.prod.yml)
> (adds Redpanda/Kafka and one container per role — needs more RAM).

---

## 1. One-VM deploy (recommended)

**Prereqs:** a VM with Docker + Docker Compose, a domain, and ports 80/443 open.

```bash
# On the VM:
git clone <your-repo> && cd clob/clob-server/deploy
cp .env.example .env
nano .env        # set SITE_ADDRESS=your.domain, JWT_SECRET, ADMIN_BOOTSTRAP_KEY, POSTGRES_PASSWORD

# Point your domain's DNS A record at the VM's IP first, then:
docker compose up -d --build
```

That's it. Caddy fetches a TLS cert automatically and serves:

- `https://your.domain` → the trading UI
- `https://your.domain/v1/*` → the REST API (same origin, so no CORS)
- `wss://your.domain/v1/stream` → the live WebSocket feed

Open the site, sign up, and use **Create market** to spin up a live, auto-seeded
market (continuous or opening-auction). Your `ADMIN_BOOTSTRAP_KEY` is the master
API key for scripted admin actions.

**No domain yet?** Set `SITE_ADDRESS=:80` in `.env` and browse to `http://<vm-ip>`
(plain HTTP, no cert).

### Keep it visibly alive (optional bots)

In the UI's **Developer** page mint two API keys (two different accounts so quotes
cross), put them in `.env` as `MAKER_KEY` / `TAKER_KEY`, then:

```bash
docker compose --profile bots up -d --build
```

A market maker quotes a two-sided book and a taker crosses it, so the chart and
tape keep moving while a recruiter watches.

### Updating

```bash
git pull && docker compose up -d --build
```

---

## 2. Split deploy (frontend on a CDN)

Want the UI on a global CDN and the API on the VM? Also fine:

1. **Backend on the VM** — run the same compose but only the backend + a TLS
   endpoint for the API (e.g. `api.your.domain`), and set
   `CORS_ALLOWED_ORIGINS=https://your-frontend-domain` on the `app` service.
2. **Frontend on Vercel / Cloudflare Pages / Netlify** (free):
   - Build command `npm run build`, output dir `dist`.
   - Env var `VITE_API_URL=https://api.your.domain`.
   - The app derives the WebSocket URL from `VITE_API_URL` automatically.

The one-VM setup in §1 is simpler (same origin → no CORS, no second TLS cert) and
is what I'd use for a demo.

---

## 3. Docs (Mintlify)

The docs live in [`../docs`](../docs) with [`../docs.json`](../docs.json).

- **Local preview:** `npm i -g mint && cd .. && mint dev`
- **Publish:** connect the repo on [mintlify.com](https://mintlify.com) (free) and
  point it at `docs.json`, or deploy the folder to Vercel. Add the published URL
  to your README and the site footer.

---

## Cost-conscious VM picks

- **Oracle Cloud Always-Free** — an Ampere ARM VM (up to 4 vCPU / 24 GB) free
  forever. Most generous; the images are multi-arch so they run on ARM.
- **Hetzner CX22** — 2 vCPU / 4 GB, ~€4/mo. Simple and reliable.
- **DigitalOcean / Vultr / Linode** — $6/mo 1 GB droplets work for a light demo
  (give Postgres a little swap).

A 1 GB box is enough for a quiet demo; 2 GB is comfortable with the bots running.
