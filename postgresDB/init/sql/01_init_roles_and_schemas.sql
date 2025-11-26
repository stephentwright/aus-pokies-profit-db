-- Create schemas and users. Run as superuser (the docker entrypoint does this automatically).
-- Passwords are placeholders for local dev; change them before using in any shared environment.

CREATE SCHEMA IF NOT EXISTS land AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS stage AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS prod AUTHORIZATION postgres;

-- Create users (roles WITH LOGIN)
-- NOTE: replace passwords in production/use secrets.
CREATE ROLE db_owner WITH LOGIN PASSWORD 'owner_pass';
CREATE ROLE db_user WITH LOGIN PASSWORD 'user_pass';
CREATE ROLE db_load WITH LOGIN PASSWORD 'load_pass';
CREATE ROLE db_external WITH LOGIN PASSWORD 'external_pass';

-- Make db_owner the owner of the three schemas so it can do DDL there.
ALTER SCHEMA land OWNER TO db_owner;
ALTER SCHEMA stage OWNER TO db_owner;
ALTER SCHEMA prod OWNER TO db_owner;

-- Basic security: revoke PUBLIC default on schemas so privileges are explicit.
REVOKE ALL ON SCHEMA land FROM PUBLIC;
REVOKE ALL ON SCHEMA stage FROM PUBLIC;
REVOKE ALL ON SCHEMA prod FROM PUBLIC;