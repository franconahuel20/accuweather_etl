#!/usr/bin/env bash
# Runs once, when the PostgreSQL volume is created (docker-entrypoint-initdb.d).
# Creates one database per concern, each with its own login role:
#   - weather  : the data warehouse (landing, stg_accuweather and analytics schemas), owned by the ETL role
#   - dagster  : Dagster run/event/schedule storage
#   - superset : Superset metadata (dashboards, charts, users), so dashboards survive restarts
# plus a read-only role that Superset uses to query the warehouse.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v warehouse_db="$WAREHOUSE_DB" -v warehouse_user="$WAREHOUSE_USER" -v warehouse_password="$WAREHOUSE_PASSWORD" \
  -v reader_user="$BI_READER_USER" -v reader_password="$BI_READER_PASSWORD" \
  -v dagster_db="$DAGSTER_PG_DB" -v dagster_user="$DAGSTER_PG_USER" -v dagster_password="$DAGSTER_PG_PASSWORD" \
  -v superset_db="$SUPERSET_PG_DB" -v superset_user="$SUPERSET_PG_USER" -v superset_password="$SUPERSET_PG_PASSWORD" <<'EOSQL'
CREATE ROLE :"warehouse_user" LOGIN PASSWORD :'warehouse_password';
CREATE ROLE :"reader_user" LOGIN PASSWORD :'reader_password';
CREATE ROLE :"dagster_user" LOGIN PASSWORD :'dagster_password';
CREATE ROLE :"superset_user" LOGIN PASSWORD :'superset_password';

CREATE DATABASE :"warehouse_db" OWNER :"warehouse_user";
CREATE DATABASE :"dagster_db" OWNER :"dagster_user";
CREATE DATABASE :"superset_db" OWNER :"superset_user";

REVOKE ALL ON DATABASE :"warehouse_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"warehouse_db" TO :"reader_user";
EOSQL

# Warehouse schemas: created by the ETL role so it owns every object dbt and Dagster create.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$WAREHOUSE_DB" \
  -v warehouse_user="$WAREHOUSE_USER" -v reader_user="$BI_READER_USER" <<'EOSQL'
SET ROLE :"warehouse_user";
CREATE SCHEMA IF NOT EXISTS landing;          -- raw files copied from MinIO / historical exports
CREATE SCHEMA IF NOT EXISTS stg_accuweather;  -- tables and view of the original accuweather.sql
CREATE SCHEMA IF NOT EXISTS analytics;        -- dashboard-ready marts
RESET ROLE;

-- Superset reads the modelled layers only; the raw landing zone stays private.
GRANT USAGE ON SCHEMA stg_accuweather, analytics TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"warehouse_user" IN SCHEMA stg_accuweather GRANT SELECT ON TABLES TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"warehouse_user" IN SCHEMA analytics GRANT SELECT ON TABLES TO :"reader_user";
EOSQL

echo "Databases ${WAREHOUSE_DB}, ${DAGSTER_PG_DB} and ${SUPERSET_PG_DB} initialised."
