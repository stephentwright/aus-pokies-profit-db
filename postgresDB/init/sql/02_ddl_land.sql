-- LAND schema: raw ingestion tables and minimal audit log
SET search_path = land, public;

CREATE TABLE IF NOT EXISTS land_ingest_log (
    ingest_id       SERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    rows_inserted   INTEGER,
    source          TEXT,
    loader          TEXT, -- which user/role performed the load
    note            TEXT
);