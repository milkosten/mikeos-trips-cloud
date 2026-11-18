# mikeos-trips-cloud

The MikeOS **trips** cloud (P14): a per-user, cross-device backend for **driving
journeys** and the **speed/GPS samples** recorded along them. It proxies the free
public **OSRM** demo for routing and aggregates a **congestion model**
("this stretch is slow at 18:00") from the samples it has learned.

FastAPI + asyncpg + Postgres, nixpacks-only, self-migrating. Every journey and
sample is scoped to the `user_id` resolved from the caller's hive agent key
(`X-API-KEY`) via mikeoscomputers (the IdP).

## Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET  | `/api/health` | no | `{status, db, version}` |
| POST | `/api/route` | no | OSRM route proxy → `{polyline, km, eta_min, ok}` |
| POST | `/api/trips` | key | start a journey → `{trip_id, started_at}` |
| POST | `/api/trips/{id}/samples` | key | batch add samples → `{stored: N}` |
| POST | `/api/trips/{id}/end` | key | end → `{trip_id, duration_min, sample_count, avg_kmh}` |
| GET  | `/api/trips/{id}` | key | a journey + `sample_count` |
| GET  | `/api/trips?limit=20` | key | recent journeys (`&before=<ISO>` pages, `&stats=1` adds per-journey sample stats) |
| GET  | `/api/trips/{id}/samples` | key | one journey's samples, oldest first (+ stats) |
| GET  | `/api/trips/months?tz=` | key | monthly totals: drives, driven km, minutes |
| GET  | `/api/congestion?lat=&lon=&radius_m=1500&hour=` | key | `{avg_kmh, sample_count, by_hour}` |

`/api/route` body: `{"from":{"lat":..,"lon":..}, "to":{"lat":..,"lon":..}}`.
Sample `ts` is parsed tolerantly — ISO-8601 string OR epoch-ms — so a raw Long
never silently drops.

## Run locally

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://user:pass@localhost:5432/mikeos_trips
uvicorn server.http_server:app --host 0.0.0.0 --port 8000
```

## Deploy

Railway (workspace SaaSRyan), nixpacks-only. See `DEPLOY.md`.

## House rules honoured

Parameterised SQL only; tolerant timestamp parsing; never-trust-200 (real
inserted counts); idempotent migrations; no reserved-keyword columns; no paid
APIs (OSRM demo is free/keyless).
