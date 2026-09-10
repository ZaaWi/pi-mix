BEGIN;
SET LOCAL TIME ZONE 'UTC';
ALTER TABLE pi_sensor_data ADD COLUMN IF NOT EXISTS sum DOUBLE PRECISION;
ALTER TABLE pi_sensor_data ADD COLUMN IF NOT EXISTS last_ts TIMESTAMPTZ;
UPDATE pi_sensor_data SET sum=avg*count WHERE sum IS NULL;
-- Legacy rows did not record event time; bucket start is the conservative fallback.
UPDATE pi_sensor_data SET last_ts=ts WHERE last_ts IS NULL;
ALTER TABLE pi_sensor_data ALTER COLUMN sum SET NOT NULL;
ALTER TABLE pi_sensor_data ALTER COLUMN last_ts SET NOT NULL;
CREATE TABLE IF NOT EXISTS pi_sensor_ingest_batches(batch_id TEXT PRIMARY KEY, committed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS pi_sensor_ingest_events(event_id TEXT PRIMARY KEY, committed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS pi_sensor_ingest_events_age ON pi_sensor_ingest_events(committed_at);
-- Repair all previously persisted hourly averages, including old closed hours.
UPDATE pi_sensor_data h SET avg=f.total/f.samples,sum=f.total
FROM (SELECT sensor,metric,date_trunc('hour',ts) AS hour,sum(sum) total,sum(count) samples
FROM pi_sensor_data WHERE interval='15m' GROUP BY 1,2,3) f
WHERE h.interval='1h' AND h.sensor=f.sensor AND h.metric=f.metric AND h.ts=f.hour;
COMMIT;
