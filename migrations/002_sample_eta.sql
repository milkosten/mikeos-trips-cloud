-- Log the app's live ETA estimate + remaining distance alongside each speed/GPS sample, so we can
-- analyse how the estimate behaves over a real drive and improve the ETA model later.
ALTER TABLE trips.samples ADD COLUMN IF NOT EXISTS eta_min      double precision;
ALTER TABLE trips.samples ADD COLUMN IF NOT EXISTS remaining_km double precision;
