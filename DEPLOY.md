# Deploying mikeos-trips-cloud

## Production: Hetzner 242 (since 2026-11-14)

- Live URL: **https://trips-api.osmike.com** (Caddy on 242 -> container `mikeos-trips:8000`).
- On 242: `/opt/mikeos-trips-cloud` = this repo + `docker-compose.yml` (copy of `deploy/docker-compose.yml`)
  + `.env` (mode 600, `TRIPS_DB_PASSWORD`, never in git). Containers `mikeos-trips` (app) and
  `mikeos-trips-db` (Postgres 18, data `/data/mikeos-trips/pg` on the RAID6), network `deploy_default`.
- Routing uses the local Valhalla over the docker network (`VALHALLA_URL=http://valhalla:8002`).
- `GET /api/health` shows `"served_by": "hetzner-242"`.
- Deploy a change: copy the repo to `/opt/mikeos-trips-cloud` (keep `.env`), then
  `docker compose build mikeos-trips && docker compose up -d mikeos-trips`.
- Backups: `/usr/local/sbin/osmike-pg-backup` (root cron 03:30) -> `/data/backups/trips/<date>.dump`, 14 days.
- The old Railway URL https://mikeos-trips-cloud-production.up.railway.app is now a forwarder
  (repo `milkosten/osmike-railway-forwarder`, `TARGET_URL=https://trips-api.osmike.com`) for old
  phone apps. Pushing to this repo does NOT deploy to Railway any more. The Railway Postgres is
  kept as fallback (secret `trips-railway-database-url`); do not delete it before Mike decides.

## Old: Railway (until 2026-11-14)

FastAPI + asyncpg + Postgres, **nixpacks-only** (no Dockerfile/Procfile, never
hardcode `$PORT`). Self-migrating from `migrations/*.sql`.

### Railway (workspace SaaSRyan)

```bash
cd mikeos-trips-cloud
railway init -n mikeos-trips-cloud
railway add --database postgres --json          # note the "Postgres-XXXX" service name
railway add -s mikeos-trips-cloud -r milkosten/mikeos-trips-cloud --branch main --json
railway variables --set 'DATABASE_URL=${{Postgres-XXXX.DATABASE_URL}}' -s mikeos-trips-cloud -e production
railway domain -s mikeos-trips-cloud -e production   # public URL
```

Auto-deploys from GitHub + on the var change. Health comes up ~2-4 min later;
poll `GET {url}/api/health` until 200.

## Env vars

- `DATABASE_URL` (required) — Railway Postgres.
- `MIKEOSCOMPUTERS_URL` (optional) — IdP; defaults to production.
- `OSRM_BASE_URL` (optional) — defaults to the free public OSRM demo.
- `TRIPS_SCHEMA` (optional) — defaults to `trips`.
