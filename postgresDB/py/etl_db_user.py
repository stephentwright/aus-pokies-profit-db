"""
Minimal ETL script that runs as db_user:
- Reads rows from land.raw_pokies
- Applies a trivial transform
- Writes results into stage.pokies_stage (ETL staging)

Usage:
  # set env vars or rely on defaults (local docker)
  export PGHOST=localhost
  export PGPORT=5432
  export PGDATABASE=aus_pokies
  export PGUSER=db_user
  export PGPASSWORD=user_pass

  python py/etl_db_user.py
"""
import os
import psycopg2
from psycopg2.extras import execute_values

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", 5432))
PGDATABASE = os.getenv("PGDATABASE", "aus_pokies")
PGUSER = os.getenv("PGUSER", "db_user")
PGPASSWORD = os.getenv("PGPASSWORD", "user_pass")

SELECT_SQL = """
SELECT id, machine_id, lga, report_date, turnover, profit, operator_name, raw_payload
FROM land.raw_pokies
WHERE report_date IS NOT NULL
ORDER BY id
LIMIT 1000;
"""

INSERT_SQL = """
INSERT INTO stage.pokies_stage (machine_id, lga, report_date, turnover, profit, operator_name, transform_meta)
VALUES %s
RETURNING id;
"""

def transform_row(row):
    # trivial transform: copy fields, record original id in meta
    id_, machine_id, lga, report_date, turnover, profit, operator_name, raw_payload = row
    meta = {"source_id": id_}
    return (machine_id, lga, report_date, turnover, profit, operator_name, psycopg2.extras.Json(meta))

def main():
    conn = psycopg2.connect(host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SELECT_SQL)
                rows = cur.fetchall()
                if not rows:
                    print("No rows to ETL from land.raw_pokies")
                    return
                transformed = [transform_row(r) for r in rows]
                # bulk insert using execute_values for speed
                execute_values(cur, INSERT_SQL, transformed)
                print(f"Inserted {len(transformed)} rows into stage.pokies_stage")
    finally:
        conn.close()

if __name__ == "__main__":
    main()