"""PostgreSQL storage manager for mikeos-trips-cloud.

Self-provisioning: reads a single DATABASE_URL env var (Railway auto-provides
it), creates the schema + a _migrations tracking table, and applies
migrations/*.sql idempotently on startup. No admin key required.

Holds the per-user log of driving journeys and an append-only stream of
speed/GPS samples along each journey, scoped by user_id. Powers the congestion
model ("this stretch is slow at 18:00").

House rules honoured:
  - Timestamps are parsed TOLERANTLY: sample `ts` accepts an ISO-8601 string
    OR epoch-ms / epoch-s numbers, so a client that sends a raw Long never
    silently drops (the FastAPI-200-but-0-rows bug class).
  - Parameterised queries only; never string-interpolate values into SQL.
  - No reserved keyword is used as a column name.
  - Never trust 200: batch inserts return the real inserted row count.
"""
import os
import glob
import logging
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union

import asyncpg

logger = logging.getLogger(__name__)

# Schema everything lives under (keeps the public schema clean).
SCHEMA = os.environ.get("TRIPS_SCHEMA", "trips")


def _parse_ts(value: Union[str, int, float, None]) -> Optional[datetime]:
    """Tolerantly parse a timestamp into an aware UTC datetime, or None.

    Accepts, in order:
      - None                       -> None (caller decides the default)
      - int/float                  -> epoch. Values that look like milliseconds
                                      (>= 1e12) are divided by 1000; otherwise
                                      treated as seconds.
      - ISO-8601 string            -> parsed (trailing 'Z' tolerated).
      - numeric string             -> treated as an epoch number.
    Anything unparseable returns None so the caller can decide (skip / now())
    rather than raising and dropping the whole write.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        num: Optional[float] = float(value)
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            num = float(s)
        except ValueError:
            num = None
        if num is None:
            # ISO-8601 string.
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except ValueError:
                logger.warning("unparseable ts %r; skipping", value)
                return None
    else:
        return None
    # Interpret the epoch magnitude: >= 1e12 is almost certainly milliseconds.
    if num >= 1e12:
        num = num / 1000.0
    try:
        return datetime.fromtimestamp(num, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        logger.warning("out-of-range ts %r; skipping", value)
        return None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class PostgreSQLManager:
    """Manages all PostgreSQL operations for the trips/samples store."""

    def __init__(self, schema: str = SCHEMA):
        self.schema = schema
        self._pool: Optional[asyncpg.Pool] = None

    async def initialize(self):
        """Initialize the connection pool from DATABASE_URL."""
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError(
                "DATABASE_URL is not set. mikeos-trips-cloud needs a Postgres "
                "database at runtime. On Railway: add a Postgres plugin and set "
                "DATABASE_URL=${{Postgres.DATABASE_URL}}."
            )
        logger.info("PostgreSQL: connecting via DATABASE_URL")
        self._pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=1,
            max_size=10,
            command_timeout=60,
            server_settings={"search_path": f"{self.schema},public"},
        )
        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        logger.info("Database connection pool initialized (schema: %s)", self.schema)

    async def run_migrations(self):
        """Apply migrations/*.sql in order, tracked in <schema>._migrations."""
        mig_dir = os.path.join(os.path.dirname(__file__), "..", "..", "migrations")
        files = sorted(glob.glob(os.path.join(mig_dir, "*.sql")))
        if not files:
            logger.warning("No migration files found at %s", mig_dir)
            return
        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self.schema}._migrations "
                "(filename text PRIMARY KEY, applied_at timestamptz DEFAULT now())"
            )
            applied = {
                r["filename"]
                for r in await conn.fetch(
                    f"SELECT filename FROM {self.schema}._migrations"
                )
            }
            for path in files:
                name = os.path.basename(path)
                if name in applied:
                    continue
                with open(path, "r", encoding="utf-8") as f:
                    sql = f.read()
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute(
                        f"INSERT INTO {self.schema}._migrations(filename) VALUES($1)",
                        name,
                    )
                logger.info("migration applied: %s", name)

    async def get_connection(self) -> asyncpg.Pool:
        if not self._pool:
            await self.initialize()
        return self._pool

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database connection pool closed")

    async def ping(self) -> bool:
        try:
            pool = await self.get_connection()
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error("Database ping failed: %s", e)
            return False

    # =========================================================================
    # Journeys
    # =========================================================================

    async def create_journey(
        self, user_id: str, body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Insert a new active journey; returns the saved row."""
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self.schema}.journeys
                    (user_id, device_id, dest_name, dest_lat, dest_lon,
                     origin_lat, origin_lon, polyline, km, eta_min, mode, status)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                     COALESCE($11, 'driving'), 'active')
                RETURNING *
                """,
                user_id,
                body.get("device_id"),
                body.get("dest_name"),
                body.get("dest_lat"),
                body.get("dest_lon"),
                body.get("origin_lat"),
                body.get("origin_lon"),
                body.get("polyline"),
                body.get("km"),
                body.get("eta_min"),
                body.get("mode"),
            )
        return self._row_to_journey(row)

    async def get_journey(
        self, user_id: str, journey_id: str
    ) -> Optional[Dict[str, Any]]:
        """One journey (scoped to the user) plus its sample_count, or None."""
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT * FROM {self.schema}.journeys "
                "WHERE id = $1::uuid AND user_id = $2",
                journey_id,
                user_id,
            )
            if row is None:
                return None
            count = await conn.fetchval(
                f"SELECT count(*) FROM {self.schema}.samples WHERE journey_id = $1::uuid",
                journey_id,
            )
        out = self._row_to_journey(row)
        out["sample_count"] = int(count or 0)
        return out

    async def list_journeys(
        self, user_id: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Recent journeys for the user, newest-started first."""
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM {self.schema}.journeys
                WHERE user_id = $1
                ORDER BY started_at DESC
                LIMIT $2
                """,
                user_id,
                max(1, min(limit, 200)),
            )
        return [self._row_to_journey(r) for r in rows]

    async def owns_journey(self, user_id: str, journey_id: str) -> bool:
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            found = await conn.fetchval(
                f"SELECT 1 FROM {self.schema}.journeys "
                "WHERE id = $1::uuid AND user_id = $2",
                journey_id,
                user_id,
            )
        return bool(found)

    async def end_journey(
        self, user_id: str, journey_id: str, avg_kmh: Optional[float]
    ) -> Optional[Dict[str, Any]]:
        """Mark a journey ended; compute avg_kmh from samples if not supplied.

        Returns {trip_id, duration_min, sample_count, avg_kmh} or None if the
        journey doesn't belong to the user.
        """
        pool = await self.get_connection()
        async with pool.acquire() as conn:
            async with conn.transaction():
                jrow = await conn.fetchrow(
                    f"SELECT id FROM {self.schema}.journeys "
                    "WHERE id = $1::uuid AND user_id = $2 FOR UPDATE",
                    journey_id,
                    user_id,
                )
                if jrow is None:
                    return None
                stats = await conn.fetchrow(
                    f"""
                    SELECT count(*) AS n, avg(speed_kmh) AS avg_speed
                    FROM {self.schema}.samples WHERE journey_id = $1::uuid
                    """,
                    journey_id,
                )
                sample_count = int(stats["n"] or 0)
                computed_avg = (
                    float(stats["avg_speed"]) if stats["avg_speed"] is not None else None
                )
                final_avg = avg_kmh if avg_kmh is not None else computed_avg
                updated = await conn.fetchrow(
                    f"""
                    UPDATE {self.schema}.journeys SET
                        status = 'ended',
                        ended_at = now(),
                        avg_kmh = $3
                    WHERE id = $1::uuid AND user_id = $2
                    RETURNING started_at, ended_at
                    """,
                    journey_id,
                    user_id,
                    final_avg,
                )
        started = updated["started_at"]
        ended = updated["ended_at"]
        duration_min = None
        if started and ended:
            duration_min = round((ended - started).total_seconds() / 60.0, 2)
        return {
            "trip_id": journey_id,
            "duration_min": duration_min,
            "sample_count": sample_count,
            "avg_kmh": round(final_avg, 2) if final_avg is not None else None,
        }

    # =========================================================================
    # Samples
    # =========================================================================

    async def add_samples(
        self, user_id: str, journey_id: str, samples: List[Dict[str, Any]]
    ) -> int:
        """Batch-insert samples for a journey the user owns.

        Returns the REAL number of rows inserted (never-trust-200). Samples that
        lack lat/lon are skipped; an unparseable ts falls back to now(). Returns
        -1 if the journey is not owned by the caller (caller maps to 404).
        """
        if not await self.owns_journey(user_id, journey_id):
            return -1
        if not samples:
            return 0
        pool = await self.get_connection()
        inserted = 0
        async with pool.acquire() as conn:
            async with conn.transaction():
                for s in samples:
                    lat = s.get("lat")
                    lon = s.get("lon")
                    if lat is None or lon is None:
                        continue
                    ts = _parse_ts(s.get("ts"))
                    if ts is None:
                        ts = datetime.now(tz=timezone.utc)
                    await conn.execute(
                        f"""
                        INSERT INTO {self.schema}.samples
                            (journey_id, lat, lon, speed_kmh, ts)
                        VALUES ($1::uuid, $2, $3, $4, $5)
                        """,
                        journey_id,
                        float(lat),
                        float(lon),
                        s.get("speed_kmh"),
                        ts,
                    )
                    inserted += 1
        return inserted

    # =========================================================================
    # Congestion model
    # =========================================================================

    async def congestion(
        self,
        user_id: str,
        lat: float,
        lon: float,
        radius_m: float = 1500.0,
        hour: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Aggregate speed samples near (lat,lon), optionally at a given hour.

        Uses a cheap bounding-box prefilter in SQL (scoped to the user's own
        journeys), then a precise python haversine cut. Returns overall avg_kmh,
        the sample_count, and an avg per hour-of-day. Zeros gracefully when no
        data matches.
        """
        radius_km = radius_m / 1000.0
        # Degrees per km ~ 1/111 for latitude; longitude scaled by cos(lat).
        dlat = radius_km / 111.0
        coslat = max(math.cos(math.radians(lat)), 1e-6)
        dlon = radius_km / (111.0 * coslat)

        pool = await self.get_connection()
        clauses = [
            "j.user_id = $1",
            "s.lat BETWEEN $2 AND $3",
            "s.lon BETWEEN $4 AND $5",
            "s.speed_kmh IS NOT NULL",
        ]
        args: List[Any] = [
            user_id,
            lat - dlat,
            lat + dlat,
            lon - dlon,
            lon + dlon,
        ]
        if hour is not None:
            args.append(int(hour))
            clauses.append(f"extract(hour FROM s.ts) = ${len(args)}")
        where = " AND ".join(clauses)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT s.lat, s.lon, s.speed_kmh,
                       extract(hour FROM s.ts)::int AS hod
                FROM {self.schema}.samples s
                JOIN {self.schema}.journeys j ON j.id = s.journey_id
                WHERE {where}
                """,
                *args,
            )

        speeds: List[float] = []
        by_hour_acc: Dict[int, List[float]] = {}
        for r in rows:
            d = _haversine_km(lat, lon, r["lat"], r["lon"])
            if d > radius_km:
                continue
            spd = float(r["speed_kmh"])
            speeds.append(spd)
            by_hour_acc.setdefault(int(r["hod"]), []).append(spd)

        avg_kmh = round(sum(speeds) / len(speeds), 2) if speeds else 0.0
        by_hour = {
            str(h): round(sum(v) / len(v), 2) for h, v in sorted(by_hour_acc.items())
        }
        return {
            "avg_kmh": avg_kmh,
            "sample_count": len(speeds),
            "by_hour": by_hour,
        }

    # =========================================================================
    # Row mappers
    # =========================================================================

    @staticmethod
    def _row_to_journey(r) -> Dict[str, Any]:
        return {
            "id": str(r["id"]),
            "user_id": r["user_id"],
            "device_id": r["device_id"],
            "dest_name": r["dest_name"],
            "dest_lat": r["dest_lat"],
            "dest_lon": r["dest_lon"],
            "origin_lat": r["origin_lat"],
            "origin_lon": r["origin_lon"],
            "polyline": r["polyline"],
            "km": r["km"],
            "eta_min": r["eta_min"],
            "mode": r["mode"],
            "status": r["status"],
            "started_at": r["started_at"].isoformat() if r["started_at"] else None,
            "ended_at": r["ended_at"].isoformat() if r["ended_at"] else None,
            "avg_kmh": r["avg_kmh"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }


# Singleton instance
postgres_manager: Optional[PostgreSQLManager] = None


async def get_postgres_manager() -> PostgreSQLManager:
    """Get or create the PostgreSQL manager singleton."""
    global postgres_manager
    if postgres_manager is None:
        postgres_manager = PostgreSQLManager()
        await postgres_manager.initialize()
    return postgres_manager
