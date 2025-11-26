#!/usr/bin/env bash
# Quick psql examples to exercise the permission model.
# Usage:
#   PGPASSWORD=load_pass ./scripts/psql_examples.sh load
#   PGPASSWORD=user_pass ./scripts/psql_examples.sh user
#   PGPASSWORD=owner_pass ./scripts/psql_examples.sh owner
#   PGPASSWORD=external_pass ./scripts/psql_examples.sh external

set -euo pipefail

HOST=localhost
PORT=5432
DB=aus_pokies

ROLE=${1:-help}

function run_as() {
  local role=$1
  local cmd=$2
  echo "---- running as ${role}: ${cmd}"
  psql -h $HOST -p $PORT -U "$role" -d $DB -c "$cmd"
}

if [ "$ROLE" = "help" ]; then
  cat <<EOF
Examples:
  PGPASSWORD=load_pass $0 load     # run load examples (INSERT into land)
  PGPASSWORD=user_pass $0 user     # run user examples (SELECT from land, write to stage)
  PGPASSWORD=owner_pass $0 owner   # run owner examples (promotion to prod)
  PGPASSWORD=external_pass $0 external  # run external examples (SELECT from prod)
EOF
  exit 0
fi

if [ "$ROLE" = "load" ]; then
  export PGPASSWORD=${PGPASSWORD:-load_pass}
  run_as db_load "INSERT INTO land.raw_pokies (machine_id,lga,report_date,turnover,profit,operator_name,source) VALUES ('M-200','Randwick','2025-01-01',1500,300,'Operator X','psql_load_example')"
  run_as db_load "SELECT id, machine_id, lga, report_date, turnover, profit FROM land.raw_pokies ORDER BY id DESC LIMIT 3"
fi

if [ "$ROLE" = "user" ]; then
  export PGPASSWORD=${PGPASSWORD:-user_pass}
  # read from LAND
  run_as db_user "SELECT id, machine_id, lga, report_date, turnover, profit FROM land.raw_pokies ORDER BY id DESC LIMIT 5"
  # write to STAGE (should be permitted)
  run_as db_user "INSERT INTO stage.pokies_stage (machine_id,lga,report_date,turnover,profit,operator_name,transform_meta) VALUES ('M-200','Randwick','2025-01-01',1500,300,'Operator X','{\"note\":\"psql user insert\"}')"
  run_as db_user "SELECT id, machine_id, lga, report_date FROM stage.pokies_stage ORDER BY id DESC LIMIT 3"
fi

if [ "$ROLE" = "owner" ]; then
  export PGPASSWORD=${PGPASSWORD:-owner_pass}
  # db_owner can read stage and prod and run DDL / promotions.
  run_as db_owner "SELECT count(*) FROM stage.pokies_stage"
  # Example promotion SQL (preview) - actually perform an aggregation/upsert
  run_as db_owner "WITH agg AS (SELECT lga, EXTRACT(YEAR FROM report_date)::int AS year, SUM(turnover) AS total_turnover, SUM(profit) AS total_profit, COUNT(DISTINCT machine_id) AS machines_count FROM stage.pokies_stage GROUP BY lga, year) INSERT INTO prod.pokies_summary_by_lga_year (lga, year, total_turnover, total_profit, machines_count, last_updated) SELECT lga, year, total_turnover, total_profit, machines_count, now() FROM agg ON CONFLICT (lga, year) DO UPDATE SET total_turnover = EXCLUDED.total_turnover, total_profit = EXCLUDED.total_profit, machines_count = EXCLUDED.machines_count, last_updated = now()"
  run_as db_owner "INSERT INTO prod.promotion_log (promoted_by, stage_source, note) VALUES ('db_owner','psql_examples','promotion from psql_examples.sh')"
  run_as db_owner "SELECT * FROM prod.pokies_summary_by_lga_year ORDER BY last_updated DESC LIMIT 5"
  run_as db_owner "SELECT * FROM prod.promotion_log ORDER BY promoted_at DESC LIMIT 5"
fi

if [ "$ROLE" = "external" ]; then
  export PGPASSWORD=${PGPASSWORD:-external_pass}
  # db_external is read-only on PROD; should be able to SELECT but NOT write.
  run_as db_external "SELECT lga, year, total_profit FROM prod.pokies_summary_by_lga_year ORDER BY year DESC LIMIT 5"
  echo "Attempting a write (should fail):"
  set +e
  run_as db_external "INSERT INTO prod.promotion_log (promoted_by, stage_source, note) VALUES ('db_external','attempt','should fail')"
  set -e
fi