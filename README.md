# Store ETL project — Data Vault 2.0 + ClickHouse + Airflow + SuperSet

## О проекте

Проект, имитирующий потоковую обработку данных интернет-магазина, включающий озеро данных (Data Lakehouse) с использованием **Data Vault 2.0** для хранения истории изменений, **ClickHouse** для логирования, аналитики и витрин данных, **Airflow** для оркестрации ETL и нагрузочных ботов, **FastAPI** для бэкенда и **Superset** для дашбордов.

### Ключевые особенности

- **Источник**: бэкенд на FastAPI, с бд на PostgreSQL и логированием в ClickHouse.
- **DWH**
  - **Staging**: PostgreSQL с партиционированием через `pg_partman`.
  - **Raw Vault**: PostgreSQL, реализация Data Vault 2.0 (хабы, линки, сателлиты).
  - **Data Mart**: ClickHouse, построены по схеме "Звезда".
  - **Info Mart**: ClickHouse, витрины данных.
- **Оркестрация**: Airflow DAG для:
  - Ботов (имитация действий пользователей).
  - Загрузки из бэкенда и логов в staging.
  - Перелива из staging в Raw Vault (инкрементально).
  - Построения витрин из Raw Vault в ClickHouse.
- **Визуализация**: Apache Superset (дашборды на основе Info Mart).

---

## Бэкенд

![backend_db.png](docs/images/db_schemes/backend_db.png)

Бэкенд на **FastAPI** — генератор событий. 
Предоставляет REST API, логирует все действия в ClickHouse, данные отдаёт в Staging.
<br><br>Позволяет:
 - Создавать/удалять/изменять пользователей;
 - Добавлять предметы в корзину и удалять их;
 - Изменять количество предметов в корзине;
 - Создавать заказы;

### Стек
- **FastAPI** + **Uvicorn**
- **SQLAlchemy 2.0** (async, sync для генерации)
- **Pydantic** + **Pydantic Settings**
- **ClickHouse Connect** (асинхронная буферизация)
- **Faker** (генерация начальных данных)

---

## Логи

![logs_db.png](docs/images/db_schemes/logs_db.png)


---

## DWH

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

---

## ETL процессы

### Нагрузочные боты

![store_bot.png](docs/images/dags/store_bot.png)

### На слой Staging

![to_staging.png](/docs/images/dags/to_staging.png)


### На слой Raw Vault

![to_raw_vault.png](/docs/images/dags/to_raw_vault.png)

### На слой Data Mart

![to_data_mart.png](/docs/images/dags/to_data_mart.png)

### На слой Info Mart

![to_info_mart](/docs/images/dags/to_info_mart.png)

---

## Дашборды

### Метрики магазинов

![store_metrics.png](docs/images/dashboards/store_metrics.png)

### Метрики товаров

![item_metrics.png](docs/images/dashboards/item_metrics.jpg)

### Метрики заказов

![order_metrics.png](docs/images/dashboards/order_metrics.jpg)

### Метрики покупателей

![consumer_metrics.png](docs/images/dashboards/consumer_metrics.png)
![consumer_metrics_change.png](docs/images/dashboards/consumer_metrics_change.png)

---

## Тестовый запуск

Для быстрого развёртывания всех компонентов проекта предусмотрен **Makefile**.  
Все команды используют переменные окружения из файла `.test.env`

### Доступные команды

| Компонент               | Запуск                     | Остановка                   | Остановка + удаление данных |
|-------------------------|----------------------------|-----------------------------|------------------------------|
| **Backend + ClickHouse (логи)** | `make up-backend`    | `make down-backend`         | `make down-backend-v`        |
| **DWH**    | `make up-dwh`        | `make down-dwh`             | `make down-dwh-v`            |
| **Airflow**             | `make up-airflow`    | `make down-airflow`         | `make down-airflow-v`        |
| **Superset**            | `make up-superset`   | `make down-superset`        | `make down-superset-v`       |
