from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook
from airflow.sdk import dag

default_args = {
    "owner": "airflow"
}

@dag(
    schedule="7/10 * * * *",
    default_args=default_args,
    
)