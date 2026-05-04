-- Runs once on first Postgres container boot.
-- Creates a separate database for the warehouse so Airflow metadata stays
-- isolated from application data. The schema (4 schemas, 17 application tables) is
-- deployed separately by setup.sh — see sql/01_schema.sql.
--
-- Note: scripts in /docker-entrypoint-initdb.d/ only execute if the data
-- volume is empty. If you change this file after first boot, you must
-- `docker compose down -v` to wipe the volume and re-init.

CREATE DATABASE pbtech_warehouse;
