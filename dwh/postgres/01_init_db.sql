CREATE SCHEMA staging;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE TABLE IF NOT EXISTS staging.category(
    category_id     UUID,
    name            VARCHAR(255),
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.item(
    item_id         UUID,
    category_id     UUID,
    name            VARCHAR(255),
    price           DECIMAL(8,2),
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.consumer(
    consumer_id     UUID,
    name            VARCHAR(255),
    lastname        VARCHAR(255),
    patronymic      VARCHAR(255),
    email           VARCHAR(255),
    password        VARCHAR(255),
    create_dt       TIMESTAMP,
    update_dt       TIMESTAMP,
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.cart(
    cart_id         UUID,
    consumer_id     UUID,
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.cart_item(
    cart_id         UUID,
    item_id         UUID,
    count_item      INTEGER,
    add_dt          TIMESTAMP,
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.store(
    store_id        UUID,
    name            VARCHAR(255),
    address         VARCHAR(255),
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.store_item(
    store_id        UUID,
    item_id         UUID,
    count_item      INTEGER,
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.orders(
    order_id        UUID,
    consumer_id     UUID,
    store_id        UUID,
    order_dt        TIMESTAMP,
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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



CREATE TABLE IF NOT EXISTS staging.order_item(
    order_id        UUID,
    item_id         UUID,
    count_item      INTEGER,
    load_id         UUID NOT NULL,
    load_dt         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_system   VARCHAR(50)
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


CREATE TABLE IF NOT EXISTS staging.logs(
    event_time      TIMESTAMP,
    duration_ms     INTEGER,
    event_type      VARCHAR(255),
    consumer_id     UUID,
    endpoint        VARCHAR(255),
    http_method     VARCHAR(255),
    item_id         UUID,
    category_id     UUID,
    store_id        UUID,
    order_id        UUID,
    price           DECIMAL(8,2),
    count_item      INTEGER,
    error_message   VARCHAR(255),
    status_code     INTEGER,
    load_id         UUID,
    load_dt         TIMESTAMP NOT NULL,
    source_system   VARCHAR(50)
) PARTITION BY RANGE (load_dt);

SELECT partman.create_partition(
    p_parent_table := 'staging.logs',
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
WHERE parent_table = 'staging.logs';


SELECT cron.schedule(
    'partman-category',
    '2/10 * * * *',
    $$SELECT partman.run_maintenance();$$
);



CREATE SCHEMA etl;
CREATE TABLE etl.etl_dt(
    table_name      VARCHAR(100) NOT NULL,
    table_schema    VARCHAR(100) NOT NULL,
    source_system   VARCHAR(100) NOT NULL,
    etl_dt          TIMESTAMP NOT NULL,
    load_id         UUID NOT NULL,
    UNIQUE(table_name, table_schema, source_system)
);






CREATE SCHEMA raw_vault;
CREATE TABLE IF NOT EXISTS raw_vault.hub_category(
    hub_category_hash_key   UUID PRIMARY KEY,
    category_id             UUID NOT NULL,
    load_dt                 TIMESTAMP NOT NULL,
    record_source           VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_vault.hub_item(
    hub_item_hash_key   UUID PRIMARY KEY,
    item_id             UUID NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    record_source       VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_vault.hub_consumer(
    hub_consumer_hash_key   UUID PRIMARY KEY,
    consumer_id             UUID NOT NULL,
    load_dt                 TIMESTAMP NOT NULL,
    record_source           VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_vault.hub_store(
    hub_store_hash_key  UUID PRIMARY KEY,
    store_id            UUID NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    record_source       VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw_vault.hub_order(
    hub_order_hash_key  UUID PRIMARY KEY,
    order_id            UUID NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    record_source       VARCHAR(50)
);



CREATE TABLE IF NOT EXISTS raw_vault.link_category_item(
    hub_category_hash_key   UUID NOT NULL,
    hub_item_hash_key       UUID NOT NULL,
    load_dt                 TIMESTAMP NOT NULL,
    record_source           VARCHAR(50) NOT NULL,
    CONSTRAINT pk_link_category_item PRIMARY KEY (hub_category_hash_key, hub_item_hash_key)
);

CREATE TABLE IF NOT EXISTS raw_vault.link_consumer_item(
    link_consumer_item_hash_key UUID PRIMARY KEY,
    hub_consumer_hash_key       UUID NOT NULL,
    hub_item_hash_key           UUID NOT NULL,
    load_dt                     TIMESTAMP NOT NULL,
    record_source               VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_vault.link_order_item(
    link_order_item_hash_key    UUID PRIMARY KEY,
    hub_order_hash_key          UUID NOT NULL,
    hub_item_hash_key           UUID NOT NULL,
    load_dt                     TIMESTAMP NOT NULL,
    record_source               VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw_vault.link_order_consumer_store(
    hub_order_hash_key      UUID NOT NULL,
    hub_consumer_hash_key   UUID NOT NULL,
    hub_store_hash_key      UUID NOT NULL,
    load_dt                 TIMESTAMP NOT NULL,
    record_source           VARCHAR(50),
    CONSTRAINT pk_link_order_consumer_store PRIMARY KEY (hub_order_hash_key, hub_consumer_hash_key, hub_store_hash_key)
);



CREATE TABLE IF NOT EXISTS raw_vault.sat_category(
    hub_category_hash_key   UUID NOT NULL,
    load_dt                 TIMESTAMP NOT NULL,
    name                    VARCHAR(255) NOT NULL,
    record_source           VARCHAR(50) NOT NULL,
    CONSTRAINT pk_sat_category PRIMARY KEY (hub_category_hash_key, load_dt)
);

CREATE TABLE IF NOT EXISTS raw_vault.sat_item(
    hub_item_hash_key   UUID NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    name                VARCHAR(255) NOT NULL,
    price               DECIMAL(8,2) NOT NULL,
    record_source       VARCHAR(50) NOT NULL,
    CONSTRAINT pk_sat_item PRIMARY KEY (hub_item_hash_key, load_dt)
);

CREATE TABLE IF NOT EXISTS raw_vault.sat_order_item(
    link_order_item_hash_key    UUID NOT NULL,
    load_dt                     TIMESTAMP NOT NULL,
    count_item                  INTEGER NOT NULL,
    record_source               VARCHAR(50) NOT NULL,
    CONSTRAINT pk_sat_order_item PRIMARY KEY (link_order_item_hash_key, load_dt)
);

CREATE TABLE IF NOT EXISTS raw_vault.sat_consumer(
    hub_consumer_hash_key   UUID NOT NULL,
    load_dt                 TIMESTAMP NOT NULL,
    update_dt               TIMESTAMP NOT NULL,
    name                    VARCHAR(255) NOT NULL,
    lastname                VARCHAR(255) NOT NULL,
    patronymic              VARCHAR(255),
    email                   VARCHAR(255) NOT NULL,
    create_dt               TIMESTAMP NOT NULL,
    record_source           VARCHAR(50) NOT NULL,
    CONSTRAINT pk_sat_consumer PRIMARY KEY (hub_consumer_hash_key, load_dt, update_dt)
);

CREATE TYPE consumer_item_update_enum AS ENUM ('add', 'delete');

CREATE TABLE IF NOT EXISTS raw_vault.sat_consumer_item(
    link_consumer_item_hash_key UUID NOT NULL,
    load_dt                     TIMESTAMP NOT NULL,
    update_dt                   TIMESTAMP NOT NULL,
    count_item                  INTEGER NOT NULL,
    type_update                 consumer_item_update_enum NOT NULL,
    record_source               VARCHAR(50) NOT NULL,
    CONSTRAINT pk_sat_consumer_item PRIMARY KEY (link_consumer_item_hash_key, load_dt, update_dt)
);

CREATE TABLE IF NOT EXISTS raw_vault.sat_store(
    hub_store_hash_key  UUID NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    name                VARCHAR(255) NOT NULL,
    address             VARCHAR(255) NOT NULL,
    record_source       VARCHAR(255) NOT NULL,
    CONSTRAINT pk_sat_store PRIMARY KEY (hub_store_hash_key, load_dt)
);

CREATE TABLE IF NOT EXISTS raw_vault.sat_order(
    hub_order_hash_key  UUID NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    order_dt            TIMESTAMP NOT NULL,
    record_source       VARCHAR(50) NOT NULL,
    CONSTRAINT pk_sat_order PRIMARY KEY (hub_order_hash_key, load_dt)
);