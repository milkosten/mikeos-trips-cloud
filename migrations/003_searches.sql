-- Log every destination search (what the user typed, how many results, what they chose, and where
-- they were) so we can see failed/misspelled searches and improve suggestions + local caching.
CREATE TABLE IF NOT EXISTS trips.searches (
    id            bigserial PRIMARY KEY,
    user_id       text NOT NULL,
    query         text NOT NULL,
    results       integer,
    chosen_label  text,
    chosen_lat    double precision,
    chosen_lon    double precision,
    near_lat      double precision,
    near_lon      double precision,
    ts            timestamptz,
    created_at    timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_searches_user ON trips.searches (user_id, created_at DESC);
