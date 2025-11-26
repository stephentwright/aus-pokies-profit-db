-- STAGE schema: ephemeral staging area used by ETL (recreated each run if desired)
SET search_path = stage, public;

CREATE TABLE IF NOT EXISTS pokies_stage (
    id              SERIAL PRIMARY KEY,
    machine_id      TEXT,
    lga             TEXT,
    report_date     DATE,
    turnover        NUMERIC,
    profit          NUMERIC,
    operator_name   TEXT,
    transformed_ts  TIMESTAMPTZ DEFAULT now(),
    transform_meta  JSONB
);