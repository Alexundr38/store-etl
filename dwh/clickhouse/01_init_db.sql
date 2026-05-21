CREATE DATABASE dm_common;

CREATE TABLE dm_common.dim_consumer(
    dim_consumer_id UInt64,
    name String,
    lastname String,
    patronymic String,
    email String,
    create_dt DateTime,
    valid_from DateTime,
    valid_to Nullable(DateTime),
    hub_consumer_hash_key UUID,
    consumer_id UUID,
    is_current BOOLEAN
) engine = MergeTree()
ORDER BY dim_consumer_id
SETTINGS index_granularity = 8192;


CREATE DATABASE dm_consumer_change;

CREATE TABLE dm_consumer_change.fact_consumer_change(
    fact_consumer_change_id UInt64,
    dim_consumer_id UInt64,
    changes_type LowCardinality(String),
    load_dt DateTime
) engine MergeTree()
ORDER BY fact_consumer_change_id
SETTINGS index_granularity = 8192;


CREATE DATABASE dm_order_item;

CREATE TABLE dm_order_item.dim_item(
    dim_item_id UInt64,
    name String,
    price DECIMAL(8,2),
    hub_item_hash_key UUID
) engine = MergeTree()
ORDER BY dim_item_id
SETTINGS index_granularity = 8192;

CREATE TABLE dm_order_item.dim_store(
    dim_store_id UInt64,
    name String,
    address String,
    hub_store_hash_key UUID
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
ORDER BY fact_order_item_id
SETTINGS index_granularity = 8192;