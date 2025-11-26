-- PROD schema: curated production tables and promotion log
SET search_path = prod, public;

-- Example aggregated summary table by LGA / year
CREATE TABLE IF NOT EXISTS pokies_summary_by_lga_year (
    lga             TEXT NOT NULL,
    year            INTEGER NOT NULL,
    total_turnover  NUMERIC,
    total_profit    NUMERIC,
    machines_count  INTEGER,
    last_updated    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (lga, year)
);

CREATE TABLE IF NOT EXISTS promotion_log (
    promotion_id    SERIAL PRIMARY KEY,
    promoted_at     TIMESTAMPTZ DEFAULT now(),
    promoted_by     TEXT,       -- typically db_owner
    stage_source    TEXT,       -- name/identifier of staging run
    note            TEXT
);