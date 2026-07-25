-- Log map interactions (taps + pans/zooms) — non-invasive, best-effort product analytics so we can
-- see WHERE the user looks and HOW they move the map, to improve the map UX and learn Mike's areas
-- of interest. Batched, fire-and-forget from the app; never blocks the user.
CREATE TABLE IF NOT EXISTS trips.interactions (
    id            bigserial PRIMARY KEY,
    user_id       text NOT NULL,
    device_id     text,
    kind          text NOT NULL,           -- 'tap' | 'move'   (avoid the reserved word "type")
    detail        text,                     -- taps: 'poi' | 'empty' ; moves: NULL
    lat           double precision,
    lon           double precision,
    zoom          double precision,
    bearing       double precision,
    feature_name  text,                     -- for POI taps: the tapped label
    ts            timestamptz,
    created_at    timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_interactions_user ON trips.interactions (user_id, created_at DESC);
