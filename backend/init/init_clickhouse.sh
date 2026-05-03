#!/bin/bash
set -e

clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    -d "$CLICKHOUSE_DB" <<-EOSQL
CREATE TABLE IF NOT EXISTS $CLICKHOUSE_DB.logs (
    event_time DateTime DEFAULT now(),
    duration_ms Nullable(UInt64),
    event_type LowCardinality(String),
    consumer_id Nullable(UUID),
    endpoint LowCardinality(String),
    http_method LowCardinality(String),
    item_id Nullable(UUID),
    category_id Nullable(UUID),
    store_id Nullable(UUID),
    order_id Nullable(UUID),
    price Nullable(DECIMAL(8,2)),
    count_item Nullable(UInt32),
    error_message Nullable(String),
    status_code Nullable(UInt16)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_type)
TTL event_time + INTERVAL 10 DAY
SETTINGS index_granularity = 8192;

CREATE ROLE IF NOT EXISTS logger_role;
GRANT INSERT ON $CLICKHOUSE_DB.logs TO logger_role;
CREATE USER IF NOT EXISTS $LOGGER_USER IDENTIFIED WITH sha256_password BY '$LOGGER_PASSWORD';
GRANT logger_role TO $LOGGER_USER;

CREATE ROLE IF NOT EXISTS etl_user_role;
GRANT SELECT ON $CLICKHOUSE_DB.logs TO etl_user_role;
CREATE USER IF NOT EXISTS $ETL_USER IDENTIFIED WITH sha256_password BY '$ETL_PASSWORD';
GRANT etl_user_role TO $ETL_USER;
EOSQL