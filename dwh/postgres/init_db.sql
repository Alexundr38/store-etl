CREATE SCHEMA staging;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE TABLE staging.category(
    category_id UUID,
    name VARCHAR(255),
    load_id UUID,
    load_dt TIMESTAMP NOT NULL,
    source_system VARCHAR(50)
)
PARTITION BY RANGE (load_dt);

-- SELECT extname, extversion, extnamespace::regnamespace AS schema
-- FROM pg_extension
-- WHERE extname = 'pg_partman';

SELECT partman.create_partition(
    p_parent_table := 'staging.category',
    p_control := 'load_dt',
    p_interval := '1 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'off'
);

UPDATE partman.part_config
SET
    retention = '1 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.category';

-- SELECT * FROM partman.show_partitions('staging.category');

UPDATE partman.part_config
SET maintenance_last_run = NULL
WHERE parent_table = 'staging.category';

-- SELECT partman.run_maintenance('staging.category');

SELECT cron.schedule(
    'partman-category',
    '* * * * *',
    $$SELECT partman.run_maintenance('staging.category');$$
);


-- DROP TABLE IF EXISTS staging.category_default;



CREATE TABLE staging.item(
    item_id UUID,
    category_id UUID,
    name VARCHAR(255),
    price DECIMAL(8,2),
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE TABLE staging.consumer(
    consumer_id UUID,
    name VARCHAR(255),
    lastname VARCHAR(255),
    patronymic VARCHAR(255),
    email VARCHAR(255),
    password VARCHAR(255),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE TABLE staging.cart(
    cart_id UUID,
    consumer_id UUID,
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE TABLE staging.cart_item(
    cart_id UUID,
    item_id UUID,
    count_item INTEGER,
    add_dt TIMESTAMP,
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE TABLE staging.store(
    store_id UUID,
    name VARCHAR(255),
    address VARCHAR(255),
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE TABLE staging.store_item(
    store_id UUID,
    item_id UUID,
    count_item INT,
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE TABLE staging.orders(
    order_id UUID,
    consumer_id UUID,
    store_id UUID,
    order_dt TIMESTAMP,
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE TABLE staging.order_item(
    order_id UUID,
    item_id UUID,
    count_item INT,
    load_id UUID,
    load_dt TIMESTAMP,
    source_system VARCHAR(50)
);