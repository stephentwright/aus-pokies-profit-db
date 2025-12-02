-- Grants to implement the permission model. Run after tables exist.
-- Privilege mapping (confirmed):
-- db_load: read/write only LAND
-- db_user: read LAND, read/write STAGE, read PROD
-- db_owner: full admin (all schemas); only role to perform DDL / promote STAGE -> PROD
-- db_external: read PROD only

-- 1) Schema usage
GRANT USAGE ON SCHEMA land TO db_load, db_user, db_owner;
GRANT USAGE ON SCHEMA stage TO db_user, db_owner;
GRANT USAGE ON SCHEMA prod TO db_user, db_owner, db_external;

-- 2) LAND: db_load: INSERT/UPDATE/DELETE/SELECT; db_user: SELECT; db_owner: ALL
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA land TO db_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA land TO db_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA land TO db_load;

-- 3) STAGE: db_user read/write, db_owner all
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stage TO db_owner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stage TO db_user;

-- 4) PROD: db_user and db_external read only, db_owner all
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA prod TO db_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA prod TO db_user;
GRANT SELECT ON ALL TABLES IN SCHEMA prod TO db_external;

-- 5) Allow db_owner to CREATE in schemas (so it can run DDL/migrations)
GRANT CREATE ON SCHEMA land TO db_owner;
GRANT CREATE ON SCHEMA stage TO db_owner;
GRANT CREATE ON SCHEMA prod TO db_owner;

-- 6) Default privileges for future tables created by db_owner
-- This makes it so when db_owner creates new tables, these grants are applied automatically.
-- Note: this statement must be run as the role that will own the tables (db_owner). Since this file runs as postgres superuser at init time, we'll set defaults for objects created by db_owner:
SET ROLE db_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA land GRANT SELECT ON TABLES TO db_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA land GRANT ALL ON TABLES TO db_load;
ALTER DEFAULT PRIVILEGES IN SCHEMA land GRANT ALL ON TABLES TO db_owner;

ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT ALL ON TABLES TO db_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT ALL ON TABLES TO db_owner;

ALTER DEFAULT PRIVILEGES IN SCHEMA prod GRANT SELECT ON TABLES TO db_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod GRANT SELECT ON TABLES TO db_external;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod GRANT ALL ON TABLES TO db_owner;
RESET ROLE;

-- 7) Default privileges for future tables created by db_load
-- This makes it so when db_owner creates new tables, these grants are applied automatically.
-- Note: this statement must be run as the role that will own the tables (db_load). Since this file runs as postgres superuser at init time, we'll set defaults for objects created by db_load:
SET ROLE db_load;
ALTER DEFAULT PRIVILEGES IN SCHEMA land GRANT SELECT ON TABLES TO db_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA land GRANT ALL ON TABLES TO db_load;
ALTER DEFAULT PRIVILEGES IN SCHEMA land GRANT ALL ON TABLES TO db_owner;
RESET ROLE;

-- 8) Default privileges for future tables created by db_user
-- This makes it so when db_owner creates new tables, these grants are applied automatically.
-- Note: this statement must be run as the role that will own the tables (db_owner). Since this file runs as postgres superuser at init time, we'll set defaults for objects created by db_owner:
SET ROLE db_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT ALL ON TABLES TO db_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT ALL ON TABLES TO db_owner;
RESET ROLE;