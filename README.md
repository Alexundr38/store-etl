# Store ETL project — Data Vault 2.0 + ClickHouse + Airflow + SuperSet

## О проекте

Проект, имитирующий потоковую обработку данных интернет-магазина, включающий озеро данных (Data Lakehouse) с использованием **Data Vault 2.0** для хранения истории изменений, **ClickHouse** для логирования, аналитики и витрин данных, **Airflow** для оркестрации ETL и нагрузочных ботов, **FastAPI** для бэкенда и **Superset** для дашбордов.

### Ключевые особенности

- **Источник**: бэкенд на FastAPI с логированием в ClickHouse.
- **Staging**: PostgreSQL с партиционированием через `pg_partman`.
- **Raw Vault**: PostgreSQL, реализация Data Vault 2.0 (хабы, линки, сателлиты).
- **Data Mart**: ClickHouse, построена по схеме "звезда".
- **Info Mart**: ClickHouse, витрины данных.
- **Оркестрация**: Airflow DAG для:
  - Ботов (имитация действий пользователей).
  - Загрузки из бэкенда в staging.
  - Перелива из staging в Raw Vault (инкрементально).
  - Построения витрин из Raw Vault в ClickHouse.
- **Визуализация**: Apache Superset (дашборды на основе Info Mart).
