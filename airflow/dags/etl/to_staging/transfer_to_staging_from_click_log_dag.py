import uuid

from airflow.sdk import dag, task, TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook
from datetime import datetime, timedelta
import io

SOURCE_SYSTEM = 'log-clickhouse'
ETL_TABLE = 'etl.etl_dt'

def build_table_task_group(table_name: str, source_schema: str, target_schema: str, load_id: str):
    @task(task_id=f'get_last_time_{table_name}')
    def get_last_time_task():
        dwh_hook = PostgresHook(postgres_conn_id='dwh_postgres_staging_load')
        result = dwh_hook.get_first(
            sql=f"""
                SELECT etl_dt
                FROM {ETL_TABLE}
                WHERE table_name = %(table_name)s AND
                    table_schema = %(source_schema)s AND
                    source_system = %(source_system)s
                """,
            parameters={
                'table_name': table_name,
                'source_schema': source_schema,
                'source_system': SOURCE_SYSTEM,
            }
        )
        if result:
            last_time = result[0]
        else:
            last_time = datetime.now() - timedelta(minutes=10)
        print(f'last time etl_dt for {table_name} was {last_time}')
        return last_time

    @task(task_id=f'transfer_data_{table_name}')
    def transfer_data_task(last_time: datetime, load_id: str):
        #extract
        source_hook = ClickHouseDbApiHook(clickhouse_conn_id='log_clickhouse_etl')
        extracted_data = source_hook.get_pandas_df(
            sql=f"""
                SELECT *
                FROM {table_name}
                WHERE event_time > %(last_time)s
                """,
            parameters={
                'last_time': last_time,
            }
        )
        extracted_time = datetime.now()
        print(f'Extracted {len(extracted_data)} rows from {table_name}')

        #transform
        extracted_data['load_id'] = load_id
        extracted_data['source_system'] = SOURCE_SYSTEM
        extracted_data['load_dt'] = extracted_time
        extracted_data['duration_ms'] = extracted_data['duration_ms'].astype('UInt64')
        extracted_data['count_item'] = extracted_data['count_item'].astype('UInt32')
        extracted_data['status_code'] = extracted_data['status_code'].astype('UInt16')
        print(f'Transformed {len(extracted_data)} rows from {table_name}')

        #load
        target_hook = PostgresHook(postgres_conn_id='dwh_postgres_staging_load')
        connection = target_hook.get_conn()
        cursor = connection.cursor()

        with io.StringIO() as buffer:
            extracted_data.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(
                sql=f"""
                    COPY {target_schema}.{table_name} ({', '.join(extracted_data.columns)})
                    FROM STDIN WITH (FORMAT CSV)
                    """,
                file=buffer
            )
        connection.commit()
        cursor.close()
        connection.close()
        print(f'Loaded {len(extracted_data)} rows from {table_name}')
        return extracted_time

    @task(task_id=f'write_last_time_{table_name}')
    def write_last_time_task(extracted_time: datetime, load_id):
        dwh_hook = PostgresHook(postgres_conn_id='dwh_postgres_staging_load')
        dwh_hook.run(
            sql=f"""
                INSERT INTO {ETL_TABLE} (table_name, table_schema, source_system, etl_dt, load_id)
                VALUES (%(table_name)s, %(table_schema)s, %(source_system)s, %(etl_dt)s, %(load_id)s)
                ON CONFLICT (table_name, table_schema, source_system)
                DO UPDATE SET etl_dt = EXCLUDED.etl_dt
            """,
            parameters={
                'table_name': table_name,
                'table_schema': 'etl',
                'source_system': SOURCE_SYSTEM,
                'etl_dt': extracted_time,
                'load_id': load_id,
            },
            autocommit=True,
        )

    last_time = get_last_time_task()
    extracted_time = transfer_data_task(last_time, load_id)
    write_last_time_task(extracted_time, load_id)


default_args = {
    'owner': 'airflow_administrator',
}

@dag(
    schedule='3/10 * * * *',
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    tags=['etl', 'to_staging', 'logs'],
)
def transfer_to_staging_from_click_log_dag():

    @task(task_id='generate_load_id')
    def generate_load_id_task():
        return str(uuid.uuid4())

    load_id = generate_load_id_task()

    with TaskGroup("logs_transfer") as logs_transfer:
        logs = build_table_task_group('logs', 'etl', 'staging', load_id)


transfer_to_staging_from_click_log_dag()