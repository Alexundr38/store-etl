#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
CREATE TABLE category(
    category_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE INDEX category_name_index ON category(name);

CREATE TABLE item(
    item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_id UUID NOT NULL REFERENCES category(category_id),
    name VARCHAR(255) NOT NULL,
    price DECIMAL(8,2) NOT NULL
);

CREATE INDEX item_category_index ON item(category_id);
CREATE INDEX item_name_index ON item(name);
CREATE INDEX item_price_index ON item(price);

CREATE TABLE consumer(
    consumer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    lastname VARCHAR(255) NOT NULL,
    patronymic VARCHAR(255),
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    create_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    update_dt TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE INDEX consumer_email_index ON consumer(email);

CREATE TABLE cart(
    cart_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_id UUID NOT NULL REFERENCES consumer(consumer_id)
);

CREATE INDEX cart_consumer_id_index ON cart(consumer_id);

CREATE TABLE cart_item(
    cart_id UUID REFERENCES cart(cart_id),
    item_id UUID REFERENCES item(item_id),
    count_item INTEGER NOT NULL,
    add_dt TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (cart_id, item_id)
);

CREATE TABLE store(
    store_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL
);

CREATE TABLE store_item(
    store_id UUID REFERENCES store(store_id),
    item_id UUID REFERENCES item(item_id),
    count_item INT NOT NULL,
    PRIMARY KEY (store_id, item_id)
);

CREATE TABLE orders(
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_id UUID NOT NULL REFERENCES consumer(consumer_id),
    store_id UUID NOT NULL REFERENCES store(store_id),
    order_dt TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE INDEX order_dt_index ON orders(order_dt);
CREATE INDEX order_consumer_id_index ON orders(consumer_id);
CREATE INDEX order_store_id_index ON orders(store_id);

CREATE TABLE order_item(
    order_id UUID REFERENCES orders(order_id),
    item_id UUID REFERENCES item(item_id),
    count_item INT NOT NULL,
    PRIMARY KEY (order_id, item_id)
);



CREATE role store_consumer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO store_consumer_role;
GRANT INSERT ON consumer, orders, order_item, cart, cart_item TO store_consumer_role;
GRANT DELETE ON consumer, cart_item TO store_consumer_role;
GRANT UPDATE ON consumer, cart_item TO store_consumer_role;

CREATE role etl_processor_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public to etl_processor_role;

CREATE USER ${POSTGRES_CONSUMER_USER} WITH LOGIN PASSWORD '${POSTGRES_CONSUMER_PASSWORD}';
CREATE USER ${POSTGRES_ETL_USER} WITH LOGIN PASSWORD '${POSTGRES_ETL_PASSWORD}';

GRANT store_consumer_role TO ${POSTGRES_CONSUMER_USER};
GRANT etl_processor_role TO ${POSTGRES_ETL_USER};
EOSQL