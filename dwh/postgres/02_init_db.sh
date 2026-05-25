#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

CREATE ROLE staging_adder_role;
CREATE USER ${DWH_LOAD_STAGING_USER} WITH LOGIN PASSWORD '${DWH_LOAD_STAGING_PASSWORD}';
GRANT USAGE ON SCHEMA staging TO staging_adder_role;
GRANT USAGE ON SCHEMA etl TO staging_adder_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
   GRANT INSERT ON TABLES TO staging_adder_role;
GRANT INSERT ON ALL TABLES IN SCHEMA staging TO staging_adder_role;
GRANT INSERT, UPDATE, SELECT ON etl.etl_dt TO staging_adder_role;
GRANT staging_adder_role TO ${DWH_LOAD_STAGING_USER};


CREATE ROLE staging_to_raw_vault_role;
GRANT INSERT, UPDATE, SELECT ON ALL TABLES IN SCHEMA raw_vault TO staging_to_raw_vault_role;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO staging_to_raw_vault_role;
GRANT USAGE ON SCHEMA staging TO staging_to_raw_vault_role;
GRANT USAGE ON SCHEMA raw_vault TO staging_to_raw_vault_role;
CREATE USER ${DWH_STAGING_TO_RAW_VAULT_USER} WITH LOGIN PASSWORD '${DWH_STAGING_TO_RAW_VAULT_PASSWORD}';
GRANT staging_to_raw_vault_role TO ${DWH_STAGING_TO_RAW_VAULT_USER};


CREATE ROLE raw_vault_to_data_mart_role;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_vault TO raw_vault_to_data_mart_role;
GRANT USAGE ON SCHEMA raw_vault TO raw_vault_to_data_mart_role;
CREATE USER ${DWH_PG_RAW_VAULT_TO_DATA_MART_USER} WITH LOGIN PASSWORD '${DWH_PG_RAW_VAULT_TO_DATA_MART_PASSWORD}';
GRANT raw_vault_to_data_mart_role TO ${DWH_PG_RAW_VAULT_TO_DATA_MART_USER};
EOSQL