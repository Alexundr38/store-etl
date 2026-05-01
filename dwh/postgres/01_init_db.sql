CREATE SCHEMA staging;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE TABLE staging.category(
    category_id UUID,
    name VARCHAR(255),
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
)
PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.category',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.category';



CREATE TABLE staging.item(
    item_id UUID,
    category_id UUID,
    name VARCHAR(255),
    price DECIMAL(8,2),
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.item',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.item';



CREATE TABLE staging.consumer(
    consumer_id UUID,
    name VARCHAR(255),
    lastname VARCHAR(255),
    patronymic VARCHAR(255),
    email VARCHAR(255),
    password VARCHAR(255),
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.consumer',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.consumer';



CREATE TABLE staging.cart(
    cart_id UUID,
    consumer_id UUID,
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.cart',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.cart';



CREATE TABLE staging.cart_item(
    cart_id UUID,
    item_id UUID,
    count_item INTEGER,
    add_dt TIMESTAMP,
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.cart_item',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.cart_item';



CREATE TABLE staging.store(
    store_id UUID,
    name VARCHAR(255),
    address VARCHAR(255),
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.store',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.store';



CREATE TABLE staging.store_item(
    store_id UUID,
    item_id UUID,
    count_item INT,
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.store_item',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.store_item';



CREATE TABLE staging.orders(
    order_id UUID,
    consumer_id UUID,
    store_id UUID,
    order_dt TIMESTAMP,
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.orders',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.orders';



CREATE TABLE staging.order_item(
    order_id UUID,
    item_id UUID,
    count_item INT,
    load_id UUID NOT NULL,
    load_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.order_item',
    p_control := 'load_dt',
    p_interval := '10 minute',
    p_type := 'range',
    p_premake := 4,
    p_default_table := true,
    p_automatic_maintenance := 'on'
);

UPDATE partman.part_config
SET
    retention = '10 minute',
    retention_keep_table = false,
    infinite_time_partitions = true
WHERE parent_table = 'staging.order_item';


SELECT cron.schedule(
    'partman-category',
    '* * * * *',
    $$SELECT partman.run_maintenance();$$
);