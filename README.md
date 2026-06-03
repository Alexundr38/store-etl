# Store ETL project — Data Vault 2.0 + ClickHouse + Airflow + SuperSet

## О проекте

Проект, имитирующий потоковую обработку данных интернет-магазина, включающий озеро данных (Data Lakehouse) с использованием **Data Vault 2.0** для хранения истории изменений, **ClickHouse** для логирования, аналитики и витрин данных, **Airflow** для оркестрации ETL и нагрузочных ботов, **FastAPI** для бэкенда и **Superset** для дашбордов.

### Ключевые особенности

- **Источник**: бэкенд на FastAPI с логированием в ClickHouse.
- **Staging**: PostgreSQL с партиционированием через `pg_partman`.
- **Raw Vault**: PostgreSQL, реализация Data Vault 2.0 (хабы, линки, сателлиты).
- **Data Mart**: ClickHouse, построены по схеме "Звезда".
- **Info Mart**: ClickHouse, витрины данных.
- **Оркестрация**: Airflow DAG для:
  - Ботов (имитация действий пользователей).
  - Загрузки из бэкенда в staging.
  - Перелива из staging в Raw Vault (инкрементально).
  - Построения витрин из Raw Vault в ClickHouse.
- **Визуализация**: Apache Superset (дашборды на основе Info Mart).

### Бэкенд

![backend_db.png](docs/images/db_schemes/backend_db.png)


### Логи

![logs_db.png](docs/images/db_schemes/logs_db.png)


### Staging

![staging_db.png](docs/images/db_schemes/staging_db.png)


### Raw Vault

![raw_vault_db.png](docs/images/db_schemes/raw_vault_db.png)


### Data Marts

**dm_common**

![dm_common_db.png](docs/images/db_schemes/dm_common_db.png)

**dm_order_item**

![dm_order_item_db.png](docs/images/db_schemes/dm_order_item_db.png)

**dm_consumer_change**

![dm_consumer_change_db](docs/images/db_schemes/dm_consumer_change_db.png)


### Info Marts

![info_mart_db.png](docs/images/db_schemes/info_mart_db.png)