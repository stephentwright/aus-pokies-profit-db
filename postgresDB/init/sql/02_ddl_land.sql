-- LAND schema: raw ingestion tables and minimal audit log
SET search_path = land, public;

CREATE TABLE IF NOT EXISTS raw_pokies (
    id              SERIAL PRIMARY KEY,
    machine_id      TEXT,
    lga             TEXT,
    report_date     DATE,
    turnover        NUMERIC,
    profit          NUMERIC,
    operator_name   TEXT,
    ingest_ts       TIMESTAMPTZ DEFAULT now(),
    source          TEXT,
    raw_payload     JSONB
);

CREATE TABLE IF NOT EXISTS land_ingest_log (
    ingest_id       SERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    rows_inserted   INTEGER,
    source          TEXT,
    loader          TEXT, -- which user/role performed the load
    note            TEXT
);