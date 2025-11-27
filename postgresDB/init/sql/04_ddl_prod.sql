-- PROD schema: curated production tables and promotion log
SET search_path = prod, public;

CREATE TABLE IF NOT EXISTS promotion_log (
    promotion_id    SERIAL PRIMARY KEY,
    promoted_at     TIMESTAMPTZ DEFAULT now(),
    promoted_by     TEXT,       -- typically db_owner
    stage_source    TEXT,       -- name/identifier of staging run
    note            TEXT
);