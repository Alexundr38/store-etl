FROM postgres:18.3

RUN apt-get update && apt-get install -y \
    postgresql-18-partman \
    postgresql-18-cron \
    && rm -rf /var/lib/apt/lists/*

COPY 01_init_db.sql /docker-entrypoint-initdb.d/
COPY 02_init_db.sh /docker-entrypoint-initdb.d/