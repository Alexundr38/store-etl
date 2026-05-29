#!/bin/bash
set -e

clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    -d "$CLICKHOUSE_DB" --multiquery <<-EOSQL
CREATE ROLE IF NOT EXISTS raw_vault_to_data_mart_role;
GRANT INSERT, SELECT ON dm_common.* TO raw_vault_to_data_mart_role;
GRANT INSERT, SELECT ON dm_consumer_change.* TO raw_vault_to_data_mart_role;
GRANT INSERT, SELECT ON dm_order_item.* TO raw_vault_to_data_mart_role;
CREATE USER IF NOT EXISTS $DWH_CH_RAW_VAULT_TO_DATA_MART_USER IDENTIFIED WITH sha256_password BY '$DWH_CH_RAW_VAULT_TO_DATA_MART_PASSWORD';
GRANT raw_vault_to_data_mart_role TO $DWH_CH_RAW_VAULT_TO_DATA_MART_USER;
GRANT CREATE TEMPORARY TABLE ON *.* TO raw_vault_to_data_mart_role;

CREATE ROLE IF NOT EXISTS data_mart_to_info_mart_role;
GRANT INSERT, SELECT ON info_mart.* TO data_mart_to_info_mart_role;
GRANT SELECT ON dm_common.* TO data_mart_to_info_mart_role;
GRANT SELECT ON dm_consumer_change.* TO data_mart_to_info_mart_role;
GRANT SELECT ON dm_order_item.* TO data_mart_to_info_mart_role;
CREATE USER IF NOT EXISTS $DWH_DATA_MART_TO_INFO_MART_USER IDENTIFIED WITH sha256_password BY '$DWH_DATA_MART_TO_INFO_MART_PASSWORD';
GRANT data_mart_to_info_mart_role TO $DWH_DATA_MART_TO_INFO_MART_USER;

CREATE ROLE IF NOT EXISTS info_mart_to_superset_role;
GRANT SELECT ON info_mart.* TO info_mart_to_superset_role;
CREATE USER IF NOT EXISTS $DWH_INFO_MART_TO_SUPERSET_USER IDENTIFIED WITH sha256_password BY '$DWH_INFO_MART_TO_SUPERSET_PASSWORD';
GRANT info_mart_to_superset_role TO $DWH_INFO_MART_TO_SUPERSET_USER;
EOSQL