CREATE DATABASE dm_common;

CREATE TABLE dm_common.dim_consumer(
    dim_consumer_id UInt64,
    name String,
    lastname String,
    patronymic Nullable(String),
    email String,
    create_dt DateTime,
    valid_from DateTime,
    hub_consumer_hash_key UUID,
    consumer_id UUID,
) engine = MergeTree()
ORDER BY (create_dt, valid_from, dim_consumer_id)
SETTINGS index_granularity = 8192;


CREATE DATABASE dm_consumer_change;

CREATE TABLE dm_consumer_change.fact_consumer_change(
    fact_consumer_change_id UInt64,
    dim_consumer_id UInt64,
    changes_type LowCardinality(String),
    change_dt DateTime,
    load_dt DateTime
) engine MergeTree()
ORDER BY (change_dt, fact_consumer_change_id)
SETTINGS index_granularity = 8192;


CREATE DATABASE dm_order_item;

CREATE TABLE dm_order_item.dim_item(
    dim_item_id UInt64,
    name String,
    price DECIMAL(8,2),
    hub_item_hash_key UUID,
    load_dt DateTime
) engine = MergeTree()
ORDER BY dim_item_id
SETTINGS index_granularity = 8192;

CREATE TABLE dm_order_item.dim_store(
    dim_store_id UInt64,
    name String,
    address String,
    hub_store_hash_key UUID,
    load_dt DateTime
) engine MergeTree()
ORDER BY dim_store_id
SETTINGS index_granularity = 8192;

CREATE TABLE dm_order_item.fact_order_item(
    fact_order_item_id UInt64,
    dim_item_id UInt64,
    dim_store_id UInt64,
    dim_consumer_id UInt64,
    count_item UInt8,
    price_item DECIMAL(8,2),
    amount DECIMAL(8,2),
    order_dt DateTime,
    load_dt DateTime,
    link_order_item_hash_key UUID,
    order_id UUID
) engine MergeTree()
ORDER BY (fact_order_item_id, order_dt)
SETTINGS index_granularity = 8192;



CREATE DATABASE info_mart;

CREATE TABLE info_mart.consumer_info(
    time_start_interval DateTime,
    consumer_id UUID,
    total_quantity UInt8,
    total_amount DECIMAL(8,2),
    avg_amount DECIMAL(8,2),
    total_order UInt8,
    unique_item UInt8
) engine = ReplacingMergeTree()
ORDER BY (time_start_interval, consumer_id)
SETTINGS index_granularity = 8192;

CREATE TABLE info_mart.store_info(
    time_start_interval DateTime,
    dim_store_id UInt8,
    name String,
    total_quantity UInt8,
    total_amount DECIMAL(8,2),
    avg_amount DECIMAL(8,2),
    total_order UInt8,
    unique_item UInt8
) engine = ReplacingMergeTree()
ORDER BY (time_start_interval, dim_store_id)
SETTINGS index_granularity = 8192;

CREATE TABLE info_mart.item_info(
    time_start_interval DateTime,
    dim_item_id UUID,
    name String,
    total_quantity UInt8,
    total_amount DECIMAL(8,2),
    total_order UInt8,
    unique_consumer UInt8,
    unique_store UInt8
) engine = ReplacingMergeTree()
ORDER BY (time_start_interval, dim_item_id)
SETTINGS index_granularity = 8192;

CREATE TABLE info_mart.order_info(
    time_start_interval DateTime,
    total_quantity UInt8,
    total_amount DECIMAL(8,2),
    avg_amount DECIMAL(8,2),
    unique_consumer UInt8,
    unique_store UInt8,
    total_order UInt8
) engine = ReplacingMergeTree()
ORDER BY time_start_interval
SETTINGS index_granularity = 8192;

CREATE TABLE info_mart.consumer_update_info(
    time_start_interval DateTime,
    total_create UInt8,
    total_update UInt8,
    unique_updated_consumer UInt8
) engine = ReplacingMergeTree()
ORDER BY time_start_interval
SETTINGS index_granularity = 8192;