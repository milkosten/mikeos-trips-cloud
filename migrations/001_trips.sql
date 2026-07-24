-- mikeos-trips-cloud: core schema (driving journeys + speed/GPS samples).
-- Applied idempotently on startup (tracked in trips._migrations).
--
-- A per-user, cross-device log of driving JOURNEYS and the append-only stream
-- of speed/GPS SAMPLES recorded along each one. This feeds the congestion model
-- ("this stretch is slow at 18:00") and the OSRM-routed trip records.
-- Every row is scoped to the user_id resolved from the caller's hive agent key
-- (X-API-KEY).
--
-- NOTE: no reserved SQL keyword is used as a column name.

-- ---------------------------------------------------------------------------
-- journeys: one driving trip (origin -> destination), OSRM-routed.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trips.journeys (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     text NOT NULL,
    device_id   text,
    dest_name   text,
    dest_lat    double precision,
    dest_lon    double precision,
    origin_lat  double precision,
    origin_lon  double precision,
    polyline    text,               -- encoded polyline (OSRM geometry)
    km          double precision,
    eta_min     double precision,
    mode        text DEFAULT 'driving',
    status      text DEFAULT 'active',
    started_at  timestamptz NOT NULL DEFAULT now(),
    ended_at    timestamptz,
    avg_kmh     double precision,
    created_at  timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_journeys_user_started
    ON trips.journeys (user_id, started_at DESC);

-- ---------------------------------------------------------------------------
-- samples: append-only speed/GPS points recorded along a journey.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trips.samples (
    id          bigserial PRIMARY KEY,
    journey_id  uuid NOT NULL REFERENCES trips.journeys (id) ON DELETE CASCADE,
    lat         double precision NOT NULL,
    lon         double precision NOT NULL,
    speed_kmh   double precision,
    ts          timestamptz NOT NULL,        -- when the sample was taken (tolerant parse)
    created_at  timestamptz DEFAULT now()    -- when the row was stored
);

CREATE INDEX IF NOT EXISTS idx_samples_journey
    ON trips.samples (journey_id);
CREATE INDEX IF NOT EXISTS idx_samples_geo
    ON trips.samples (lat, lon);
