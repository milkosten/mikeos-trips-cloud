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

# Our self-hosted Valhalla (planet, on the 242 box) — the PRIMARY router now. Richer turn-by-turn
# (real street-name maneuvers) and it's ours (no OSRM-demo ToS/rate limits). OSRM stays as a fallback.
VALHALLA_URL = os.environ.get("VALHALLA_URL", "https://route.osmike.com")
VALHALLA_TOKEN = os.environ.get("VALHALLA_TOKEN", "")

# ---- polyline codec: Valhalla emits precision-6 shapes; the app decodes precision-5 (OSRM) ----------
def _decode_polyline(s: str, precision: int = 6):
    factor = 10 ** precision
    index = lat = lng = 0
    coords = []
    while index < len(s):
        for is_lat in (True, False):
            shift = result = 0
            while True:
                b = ord(s[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            d = ~(result >> 1) if (result & 1) else (result >> 1)
            if is_lat:
                lat += d
            else:
                lng += d
        coords.append((lat / factor, lng / factor))
    return coords


def _encode_polyline(coords, precision: int = 5) -> str:
    factor = 10 ** precision
    out = []
    plat = plng = 0

    def _enc(v: int) -> str:
        v = (v << 1) if v >= 0 else ~(v << 1)
        s = ""
        while v >= 0x20:
            s += chr((0x20 | (v & 0x1f)) + 63)
            v >>= 5
        return s + chr(v + 63)

    for lat, lng in coords:
        ilat = round(lat * factor)
        ilng = round(lng * factor)
        out.append(_enc(ilat - plat))
        out.append(_enc(ilng - plng))
        plat, plng = ilat, ilng
    return "".join(out)


# Valhalla maneuver.type (int enum) -> the app's OSRM-style (type, modifier). Unknown -> continue.
_VALHALLA_MANEUVER = {
    0: ("continue", "straight"),
    1: ("depart", None), 2: ("depart", "right"), 3: ("depart", "left"),
    4: ("arrive", None), 5: ("arrive", "right"), 6: ("arrive", "left"),
    7: ("new name", "straight"), 8: ("continue", "straight"),
    9: ("turn", "slight right"), 10: ("turn", "right"), 11: ("turn", "sharp right"),
    12: ("turn", "uturn"), 13: ("turn", "uturn"),
    14: ("turn", "sharp left"), 15: ("turn", "left"), 16: ("turn", "slight left"),
    17: ("on ramp", "straight"), 18: ("on ramp", "right"), 19: ("on ramp", "left"),
    20: ("off ramp", "right"), 21: ("off ramp", "left"),
    22: ("continue", "straight"), 23: ("fork", "right"), 24: ("fork", "left"),
    25: ("merge", "straight"),
    26: ("roundabout", None), 27: ("exit roundabout", None),
    28: ("continue", "straight"), 29: ("continue", "straight"),
}


async def _route_via_valhalla(frm, to):
    """Our Valhalla → the app's {ok,polyline(p5),km,eta_min,steps} shape. Returns None on any failure."""
    if not VALHALLA_URL:
        return None
    payload = {
        "locations": [{"lat": frm.lat, "lon": frm.lon}, {"lat": to.lat, "lon": to.lon}],
        "costing": "auto",
        "directions_options": {"units": "kilometers"},
    }
    headers = {"Authorization": f"Bearer {VALHALLA_TOKEN}"} if VALHALLA_TOKEN else {}
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            resp = await client.post(f"{VALHALLA_URL}/route", json=payload, headers=headers)
        if resp.status_code != 200:
            return None
        trip = (resp.json() or {}).get("trip") or {}
        legs = trip.get("legs") or []
        if not legs:
            return None
        # one combined shape + maneuvers across legs (single leg for A→B, but be general)
        coords = []
        steps_out = []
        for leg in legs:
            leg_coords = _decode_polyline(leg.get("shape") or "", 6)
            base = len(coords)
            coords.extend(leg_coords)
            for man in (leg.get("maneuvers") or []):
                typ, mod = _VALHALLA_MANEUVER.get(man.get("type"), ("continue", "straight"))
                bi = base + (man.get("begin_shape_index") or 0)
                loc = coords[bi] if 0 <= bi < len(coords) else (None, None)
                names = man.get("street_names") or man.get("begin_street_names") or []
                steps_out.append({
                    "type": typ,
                    "modifier": mod,
                    "name": names[0] if names else "",
                    "distance_m": round((man.get("length") or 0.0) * 1000.0, 1),
                    "duration_s": round(man.get("time") or 0.0, 1),
                    "lat": loc[0],
                    "lon": loc[1],
                })
        summary = trip.get("summary") or {}
        return {
            "ok": True,
            "polyline": _encode_polyline(coords, 5),
            "km": round(summary.get("length") or 0.0, 2),
            "eta_min": round((summary.get("time") or 0.0) / 60.0, 1),
            "steps": steps_out,
            "router": "valhalla",
        }
    except Exception as e:
        logger.warning("Valhalla route failed (falling back to OSRM): %s", e)
        return None


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


class InteractionIn(BaseModel):
    # A single map interaction. `type` is 'tap' | 'move'; `detail` is 'poi' | 'empty' for taps.
    type: str
    detail: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    zoom: Optional[float] = None
    bearing: Optional[float] = None
    feature_name: Optional[str] = None
    ts: Optional[Union[str, int, float]] = None


class InteractionsIn(BaseModel):
    events: List[InteractionIn]
    device_id: Optional[str] = None


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
    """Driving route. Best-effort: never raises 500.

    PRIMARY: our self-hosted Valhalla (planet, real street-name maneuvers, ours). FALLBACK: OSRM
    (public demo unless OSRM_BASE_URL is set) if Valhalla is unavailable — routing can't go dark.
    Returns {polyline(precision-5), km, eta_min, steps, ok:true} on success.
    """
    frm = body.from_
    to = body.to

    valhalla = await _route_via_valhalla(frm, to)
    if valhalla is not None and valhalla.get("polyline"):
        return valhalla

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


@app.post("/api/interactions")
async def log_interactions(
    body: InteractionsIn,
    user_id: str = Depends(current_user_id),
) -> Dict[str, Any]:
    """Batch-log map interactions (taps + pans/zooms). `ts` parsed tolerantly (ISO-8601 or epoch).

    Returns {stored: N} with the REAL rows inserted (never-trust-200). Best-effort analytics — the
    app fires these in the background; this endpoint must stay cheap and never 500.
    """
    db = await get_postgres_manager()
    items = [e.model_dump(exclude_none=False) for e in body.events]
    stored = await db.add_interactions(user_id, body.device_id, items)
    return {"stored": stored}


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
