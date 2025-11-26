"""
Promotion script to be run as db_owner:
- Aggregates stage.pokies_stage into prod.pokies_summary_by_lga_year
- Uses upsert (ON CONFLICT) to update existing rows
- Writes an entry to prod.promotion_log

Usage:
  export PGHOST=localhost
  export PGPORT=5432
  export PGDATABASE=aus_pokies
  export PGUSER=db_owner
  export PGPASSWORD=owner_pass

  python py/promote_db_owner.py
"""
import os
import psycopg2

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", 5432))
PGDATABASE = os.getenv("PGDATABASE", "aus_pokies")
PGUSER = os.getenv("PGUSER", "db_owner")
PGPASSWORD = os.getenv("PGPASSWORD", "owner_pass")

PROMOTE_SQL = """
WITH agg AS (
  SELECT
    lga,
    EXTRACT(YEAR FROM report_date)::int AS year,
    SUM(turnover) AS total_turnover,
    SUM(profit) AS total_profit,
    COUNT(DISTINCT machine_id) AS machines_count
  FROM stage.pokies_stage
  GROUP BY lga, year
)
INSERT INTO prod.pokies_summary_by_lga_year
  (lga, year, total_turnover, total_profit, machines_count, last_updated)
SELECT lga, year, total_turnover, total_profit, machines_count, now() FROM agg
ON CONFLICT (lga, year) DO UPDATE
  SET total_turnover = EXCLUDED.total_turnover,
      total_profit = EXCLUDED.total_profit,
      machines_count = EXCLUDED.machines_count,
      last_updated = now();
"""

LOG_SQL = """
INSERT INTO prod.promotion_log (promoted_by, stage_source, note) VALUES (%s, %s, %s);
"""

def main():
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM stage.pokies_stage")
                count = cur.fetchone()[0]
                print(f"Promoting data from stage: {count} rows")
                cur.execute(PROMOTE_SQL)
                cur.execute(LOG_SQL, ("db_owner", "promote_db_owner.py", f"Promoted {count} rows aggregated into prod"))
                print("Promotion complete; promotion_log updated.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()