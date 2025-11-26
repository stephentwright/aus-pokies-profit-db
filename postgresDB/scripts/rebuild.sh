#!/usr/bin/env bash
# Simple script to rebuild the schemas/tables by running the SQL files against the containerized DB.
# Assumes docker-compose up -d has been run and the service name is "db" with exposed port 5432.
# Usage: ./scripts/rebuild.sh
set -euo pipefail

# connection params for psql
HOST=localhost
PORT=5432
DB=aus_pokies
USER=postgres
PASS=postgres

export PGPASSWORD=$PASS

SQL_DIR=init/sql

echo "Running SQL scripts in order against ${USER}@${HOST}:${PORT}/${DB}"
psql -h $HOST -p $PORT -U $USER -d $DB -f ${SQL_DIR}/01_init_roles_and_schemas.sql
psql -h $HOST -p $PORT -U $USER -d $DB -f ${SQL_DIR}/02_ddl_land.sql
psql -h $HOST -p $PORT -U $USER -d $DB -f ${SQL_DIR}/03_ddl_stage.sql
psql -h $HOST -p $PORT -U $USER -d $DB -f ${SQL_DIR}/04_ddl_prod.sql
psql -h $HOST -p $PORT -U $USER -d $DB -f ${SQL_DIR}/05_grants.sql
psql -h $HOST -p $PORT -U $USER -d $DB -f ${SQL_DIR}/06_optional_seed.sql

echo "Rebuild complete."