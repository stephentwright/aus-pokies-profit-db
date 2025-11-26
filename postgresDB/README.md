# Local Postgres for aus-pokies-profit-db (LAND / STAGE / PROD)

This repo contains a small local Postgres setup for development and ETL testing. The DB uses three schemas:
- land — raw ingested data and ingest log
- stage — ephemeral staging area (ETL transforms live here)
- prod — curated production tables and promotion_log

Roles/users created for the local dev instance:
- db_owner — full admin; owns schemas and is the only role intended to perform DDL and promotions
- db_user — read LAND, read/write STAGE, read PROD
- db_load — read/write LAND only
- db_external — read-only PROD

Quick start:
1. Start Postgres with Docker Compose:
   docker compose up -d

   The first time Postgres starts it will execute the SQL files in `init/sql` to create schemas, users, tables, and grants.

2. Rebuild or re-run init SQL (after container is up):
   ./scripts/rebuild.sh

   This runs the SQL scripts in order against the running container. Adjust connection settings in the script if needed.

Credentials (local dev):
- Superuser: postgres / postgres (configured in docker-compose.yml)
- db_owner: db_owner / owner_pass
- db_user: db_user / user_pass
- db_load: db_load / load_pass
- db_external: db_external / external_pass

Change these passwords before sharing or deploying.

Notes and portability guidance:
- This setup is intentionally Postgres-first so you can test real GRANTs and role behavior. SQLite does not support roles/GRANTs so migrating a local SQLite approach would require enforcing role semantics in application code.
- If you later migrate to MySQL or a hosted Postgres, watch for small SQL dialect differences (serial/identity, jsonb availability, ALTER DEFAULT PRIVILEGES behavior).
- For schema evolution in production, consider a migration tool (Alembic, Flyway, or plain SQL scripts). Your chosen approach of plain SQL scripts that can rebuild the DB is supported by this layout.
- STAGE is designed to be ephemeral: your ETL should recreate/refresh stage tables each run. PROD is the curated dataset and should be updated only via db_owner promotions (or controlled migrations).
- LAND allows updates/deletes — you said updates/deletes are allowed for ingests. Consider adding a soft-delete column or retention policy if the dataset grows.
- Minimal audit tables are included: land_ingest_log and promotion_log. Capture loader/promotion metadata here.

## pgAdmin Web GUI

A web-based PostgreSQL management interface is included with Docker Compose.

**Access pgAdmin:**
- URL: `http://localhost:5050`
- Email: `admin@example.com`
- Password: `admin`

**Connect to the Database:**
1. Log in to pgAdmin
2. Right-click "Servers" in the left panel and select "Register" → "Server"
3. Enter connection details:
   - **Name:** `aus_pokies` (or any name you prefer)
   - **Host name/address:** `db`
   - **Port:** `5432`
   - **Username:** `postgres`
   - **Password:** `postgres`
   - **Save password?** Yes
4. Click "Save"

You can now browse tables, run queries, and manage the database from the web interface.