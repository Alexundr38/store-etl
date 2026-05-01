#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

CREATE ROLE staging_adder_role;
GRANT INSERT ON ALL TABLES IN SCHEMA staging TO staging_adder_role;


CREATE USER ${DWH_LOAD_STAGING_USER} WITH LOGIN PASSWORD '${DWH_LOAD_STAGING_PASSWORD}';

GRANT USAGE ON SCHEMA staging TO staging_adder_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
   GRANT INSERT ON TABLES TO staging_adder_role;

GRANT INSERT ON ALL TABLES IN SCHEMA staging TO staging_adder_role;

GRANT staging_adder_role TO ${DWH_LOAD_STAGING_USER};
EOSQL