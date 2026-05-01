import uuid
import io
from airflow.sdk import dag, task, TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd


def build_table_tasks_with_dt(table_name: str, source_schema:str,
                              target_schema: str, dt_col: str, load_id: str):

    @task(task_id=f'get_last_time_{table_name}')
    def get_last_time():
        print(f'time: {datetime.now() - timedelta(minutes=10)}')
        return datetime.now() - timedelta(minutes=10)


    @task(task_id=f'transfer_data_{table_name}')
    def transfer_data(last_time: datetime, load_id: str):
        #extract
        source_hook = PostgresHook(postgres_conn_id='backend_postgres_etl')
        sql = f"""
            SELECT *
            FROM {source_schema}.{table_name}
            WHERE {dt_col} >= %(last_time)s
        """
        extracted_data = source_hook.get_pandas_df(sql, parameters={'last_time': last_time})
        print(f'Extracted {len(extracted_data)} rows')

        #transform
        extracted_data['load_id'] = load_id
        print(f'Transformed {len(extracted_data)} rows')

        #load
        target_hook = PostgresHook(postgres_conn_id='dwh_postgres_staging_load')
        connection = target_hook.get_conn()
        cursor = connection.cursor()

        with io.StringIO() as buffer:
            extracted_data.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(
                sql=f"""COPY {target_schema}.{table_name} ({', '.join(extracted_data.columns)})
                        FROM STDIN WITH (FORMAT CSV)""",
                file=buffer,
            )
        connection.commit()
        cursor.close()
        connection.close()
        print(f'Loaded {len(extracted_data)} rows')

    last_time = get_last_time()
    transfer_data(last_time, load_id)



def build_table_tasks(table_name: str, source_schema: str,
                     target_schema: str, load_id: str):

    @task(task_id=f'transfer_data_{table_name}')
    def transfer_data(load_id: str):
        #extract
        source_hook = PostgresHook(postgres_conn_id='backend_postgres_etl')
        extracted_data = source_hook.get_pandas_df(f"""
            SELECT *
            FROM {source_schema}.{table_name}
        """)
        print(f'Extracted {len(extracted_data)} rows')

        #transform
        extracted_data['load_id'] = load_id
        print(f'Transferred {len(extracted_data)} rows')

        #load
        target_hook = PostgresHook(postgres_conn_id='dwh_postgres_staging_load')
        connection = target_hook.get_conn()
        cursor = connection.cursor()
        with io.StringIO() as buffer:
            extracted_data.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(
                sql=f"""COPY {target_schema}.{table_name} ({', '.join(extracted_data.columns)})
                        FROM STDIN WITH (FORMAT CSV)""",
                file=buffer,
            )
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Loaded {len(extracted_data)} rows")

    transfer_data(load_id)


default_args = {
    'owner': 'airflow_administrator',
}

@dag(
    schedule='*/15 * * * *',
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    tags=['etl', 'to_staging'],
)
def transfer_to_staging_dag():

    @task
    def generate_load_id():
        return str(uuid.uuid4())

    load_id = generate_load_id()

    with TaskGroup("without_dt") as without_dt:
        category = build_table_tasks('category', 'public', 'staging', load_id)
        item = build_table_tasks('item', 'public', 'staging', load_id)
        store = build_table_tasks('store', 'public', 'staging', load_id)
        store_item = build_table_tasks('store_item', 'public', 'staging', load_id)
        cart = build_table_tasks('cart', 'public', 'staging', load_id)
        order_item = build_table_tasks('order_item', 'public', 'staging', load_id)

    with TaskGroup("with_dt") as with_dt:
        consumer = build_table_tasks_with_dt('consumer', 'public', 'staging', 'update_dt', load_id)
        cart_item = build_table_tasks_with_dt('cart_item', 'public', 'staging', 'add_dt' ,load_id)
        orders = build_table_tasks_with_dt('orders', 'public', 'staging', 'order_dt', load_id)

    without_dt >> with_dt

transfer_to_staging_dag()