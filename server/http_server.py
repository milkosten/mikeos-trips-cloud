"""mikeos-trips-cloud HTTP server.

The MikeOS trips cloud: a per-user, cross-device log of driving JOURNEYS
(origin -> destination, OSRM-routed) and the append-only stream of speed/GPS
SAMPLES recorded along each one. It proxies the free public OSRM demo for
routing and aggregates a congestion model ("this stretch is slow at 18:00")
from the samples it has learned.

Identity: apps present X-API-KEY: <hive agent key>, resolved to a user_id via
mikeoscomputers (the IdP). Every journey and sample is scoped to that user_id.
The routing helper (/api/route) and /api/health are keyless.

Timestamps: sample `ts` is parsed tolerantly — an ISO-8601 string OR epoch
millis/seconds both work — so a client sending a raw Long never silently drops.

Endpoints:
  GET  /api/health                       -> {status, db, version}                (no auth)
  POST /api/route                        -> OSRM route proxy {polyline,km,eta_min} (no auth)
  POST /api/trips                        -> start a journey  {trip_id, started_at}
  POST /api/trips/{id}/samples           -> batch add samples {stored: N}
  POST /api/trips/{id}/end               -> end a journey  {trip_id,duration_min,sample_count,avg_kmh}
  GET  /api/trips/{id}                   -> a journey + sample_count
  GET  /api/trips?limit=20               -> recent journeys for the user
  GET  /api/congestion?lat=&lon=&radius_m=&hour=  -> the congestion model
"""
import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field

from server.identity import resolve_agent_key
from server.storage.postgres_manager import get_postgres_manager

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

VERSION = "1.0.0"

# Free, keyless public OSRM demo server. No paid API.
OSRM_BASE = os.environ.get("OSRM_BASE_URL", "https://router.project-osrm.org")
# Bearer token for a gated OSRM (e.g. our self-hosted mikeos-osm at osm.osmike.com/route). Empty =
# no auth header (public demo). Set OSRM_BASE_URL + OSRM_TOKEN together at the routing cutover.
OSRM_TOKEN = os.environ.get("OSRM_TOKEN", "")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class LatLon(BaseModel):
    lat: float
    lon: float


class RouteIn(BaseModel):
    # `from` is a Python keyword, so bind the JSON field via an alias.
    from_: LatLon = Field(alias="from")
    to: LatLon

    model_config = {"populate_by_name": True}


class TripIn(BaseModel):
    dest_name: Optional[str] = None
    dest_lat: Optional[float] = None
    dest_lon: Optional[float] = None
    origin_lat: Optional[float] = None
    origin_lon: Optional[float] = None
    polyline: Optional[str] = None
    km: Optional[float] = None
    eta_min: Optional[float] = None
    mode: Optional[str] = None
    device_id: Optional[str] = None


class SampleIn(BaseModel):
    lat: float
    lon: float
    speed_kmh: Optional[float] = None
    # The app's live estimate at this sample — logged for later ETA-model analysis.
    eta_min: Optional[float] = None
    remaining_km: Optional[float] = None
    # Accept an ISO-8601 string OR epoch millis/seconds (parsed tolerantly).
    ts: Optional[Union[str, int, float]] = None


class SamplesIn(BaseModel):
    samples: List[SampleIn]


class EndIn(BaseModel):
    avg_kmh: Optional[float] = None


class SearchIn(BaseModel):
    query: str
    results: Optional[int] = None
    chosen_label: Optional[str] = None
    chosen_lat: Optional[float] = None
    chosen_lon: Optional[float] = None
    near_lat: Optional[float] = None
    near_lon: Optional[float] = None
    ts: Optional[Union[str, int, float]] = None


# ---------------------------------------------------------------------------
# Auth dependency: resolve X-API-KEY -> user_id
# ---------------------------------------------------------------------------
async def current_user_id(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-KEY"),
) -> str:
    """Resolve the caller's hive agent key to a user_id, or 401."""
    user_id = await resolve_agent_key(x_api_key)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-KEY")
    return user_id


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting mikeos-trips-cloud...")
    try:
        db = await get_postgres_manager()
        if await db.ping():
            logger.info("Database connection established")
        else:
            logger.error("Database connection failed!")
        await db.run_migrations()
    except Exception as e:
        logger.error("Startup error: %s", e)
        raise
    yield
    logger.info("Shutting down mikeos-trips-cloud...")
    try:
        db = await get_postgres_manager()
        await db.close()
    except Exception as e:
        logger.error("Shutdown error: %s", e)


app = FastAPI(
    title="mikeos-trips-cloud",
    description="Per-user driving journeys + speed/GPS samples, OSRM routing and a congestion model for MikeOS.",
    version=VERSION,
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Root / health
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {
        "service": "mikeos-trips-cloud",
        "health": "/api/health",
        "endpoints": {
            "route": "POST /api/route",
            "start_trip": "POST /api/trips",
            "add_samples": "POST /api/trips/{id}/samples",
            "end_trip": "POST /api/trips/{id}/end",
            "get_trip": "GET /api/trips/{id}",
            "list_trips": "GET /api/trips?limit=20",
            "congestion": "GET /api/congestion?lat=&lon=&radius_m=1500&hour=",
        },
    }


@app.get("/api/health")
async def health():
    db_ok = False
    try:
        db = await get_postgres_manager()
        db_ok = await db.ping()
    except Exception as e:
        logger.error("health db check failed: %s", e)
    return {
        "status": "healthy" if db_ok else "degraded",
        "db": "up" if db_ok else "down",
        "version": VERSION,
    }


# ---------------------------------------------------------------------------
# Routing (keyless helper): proxy the free public OSRM demo.
# ---------------------------------------------------------------------------
@app.post("/api/route")
async def route(body: RouteIn) -> Dict[str, Any]:
    """Proxy OSRM for a driving route. Best-effort: never raises 500.

    Returns {polyline, km, eta_min, ok:true} on success, or {ok:false, error}
    on any OSRM non-200 / timeout / malformed response.
    """
    frm = body.from_
    to = body.to
    url = (
        f"{OSRM_BASE}/route/v1/driving/"
        f"{frm.lon},{frm.lat};{to.lon},{to.lat}"
        "?overview=full&geometries=polyline&steps=true"
    )
    headers = {"Authorization": f"Bearer {OSRM_TOKEN}"} if OSRM_TOKEN else {}
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            return {"ok": False, "error": f"osrm http {resp.status_code}"}
        data = resp.json()
        routes = data.get("routes") or []
        if not routes:
            return {"ok": False, "error": data.get("message") or "no route"}
        r0 = routes[0]
        # Turn-by-turn maneuvers (OSRM legs[].steps[]). Flattened + defensive so a missing
        # field never breaks the response. The app builds the instruction text from type/modifier.
        steps_out = []
        for leg in (r0.get("legs") or []):
            for st in (leg.get("steps") or []):
                man = st.get("maneuver") or {}
                loc = man.get("location") or []
                steps_out.append({
                    "type": man.get("type"),
                    "modifier": man.get("modifier"),
                    "name": st.get("name") or "",
                    "distance_m": round(st.get("distance") or 0.0, 1),
                    "duration_s": round(st.get("duration") or 0.0, 1),
                    "lat": loc[1] if len(loc) > 1 else None,
                    "lon": loc[0] if len(loc) > 0 else None,
                })
        return {
            "ok": True,
            "polyline": r0.get("geometry"),
            "km": round((r0.get("distance") or 0) / 1000.0, 2),
            "eta_min": round((r0.get("duration") or 0) / 60.0, 1),
            "steps": steps_out,
        }
    except Exception as e:
        logger.warning("OSRM route failed: %s", e)
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Trips (journeys)
# ---------------------------------------------------------------------------
@app.post("/api/trips")
async def start_trip(
    body: TripIn,
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """Start (insert) an active journey. Returns {trip_id, started_at}."""
    db = await get_postgres_manager()
    journey = await db.create_journey(user_id, body.model_dump(exclude_none=False))
    return {"trip_id": journey["id"], "started_at": journey["started_at"]}


@app.post("/api/trips/{trip_id}/samples")
async def add_samples(
    trip_id: str,
    body: SamplesIn,
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """Batch-insert speed/GPS samples for a journey the caller owns.

    `ts` is parsed tolerantly (ISO-8601 or epoch-ms). Returns {stored: N} with
    the REAL number of rows inserted (never-trust-200). A journey that isn't the
    caller's -> 404.
    """
    db = await get_postgres_manager()
    items = [s.model_dump(exclude_none=False) for s in body.samples]
    stored = await db.add_samples(user_id, trip_id, items)
    if stored < 0:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"stored": stored}


@app.post("/api/searches")
async def log_search(
    body: SearchIn,
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """Log a destination search (query + result count + chosen place + where the user was)."""
    db = await get_postgres_manager()
    sid = await db.add_search(user_id, body.model_dump(exclude_none=False))
    return {"ok": True, "id": sid}


@app.post("/api/trips/{trip_id}/end")
async def end_trip(
    trip_id: str,
    body: EndIn,
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """End a journey. Computes avg_kmh from samples when not supplied."""
    db = await get_postgres_manager()
    result = await db.end_journey(user_id, trip_id, body.avg_kmh)
    if result is None:
        raise HTTPException(status_code=404, detail="Trip not found")
    return result


@app.get("/api/trips/{trip_id}")
async def get_trip(
    trip_id: str,
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """A journey plus its sample_count (scoped to the user)."""
    db = await get_postgres_manager()
    journey = await db.get_journey(user_id, trip_id)
    if journey is None:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"trip": journey}


@app.get("/api/trips")
async def list_trips(
    limit: int = Query(default=20, ge=1, le=200),
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """Recent journeys for the user, newest-started first."""
    db = await get_postgres_manager()
    journeys = await db.list_journeys(user_id, limit=limit)
    return {"trips": journeys, "count": len(journeys)}


# ---------------------------------------------------------------------------
# Congestion model
# ---------------------------------------------------------------------------
@app.get("/api/congestion")
async def congestion(
    lat: float = Query(...),
    lon: float = Query(...),
    radius_m: float = Query(default=1500.0, ge=1.0, le=50000.0),
    hour: Optional[int] = Query(default=None, ge=0, le=23),
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """The learned congestion model near a point (avg speed, per hour-of-day).

    Aggregates the user's own samples within `radius_m` of (lat,lon), optionally
    at a given hour-of-day. Returns {avg_kmh, sample_count, by_hour}, zeros when
    there's no data.
    """
    db = await get_postgres_manager()
    return await db.congestion(user_id, lat, lon, radius_m=radius_m, hour=hour)
