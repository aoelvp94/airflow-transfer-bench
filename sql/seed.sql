-- Rebuild bench.events with :rows rows.  Usage: psql -v rows=1000000 -f seed.sql
-- setseed() makes the data identical across reseeds of the same size.
\set ON_ERROR_STOP on
SELECT setseed(0.42);

DROP TABLE IF EXISTS bench.events;
CREATE TABLE bench.events AS
SELECT g                                                          AS id,
       timestamptz '2026-01-01 00:00:00+00' + g * interval '1 second' AS event_ts,
       (g % 100000)::int                                          AS user_id,
       (ARRAY['click', 'view', 'purchase', 'signup'])[1 + g % 4]  AS event_type,
       round((random() * 1000)::numeric, 2)                      AS amount,
       -- JSON with quotes and commas exercises CSV quoting in every language.
       jsonb_build_object('session', md5(g::text), 'tags', jsonb_build_array('a', 'b,c'),
                          'note', CASE WHEN g % 10 = 0 THEN 'say "hi"' END) AS payload
FROM generate_series(1, :rows) AS g;

ALTER TABLE bench.events ADD PRIMARY KEY (id);
VACUUM ANALYZE bench.events;
SELECT count(*) AS rows, pg_size_pretty(pg_total_relation_size('bench.events')) AS size FROM bench.events;
