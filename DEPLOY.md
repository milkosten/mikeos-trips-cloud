# Deploying mikeos-trips-cloud

FastAPI + asyncpg + Postgres, **nixpacks-only** (no Dockerfile/Procfile, never
hardcode `$PORT`). Self-migrating from `migrations/*.sql`.

## Railway (workspace SaaSRyan)

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
