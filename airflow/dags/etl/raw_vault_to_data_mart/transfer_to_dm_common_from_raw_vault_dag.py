from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.sdk import dag, task
from datetime import datetime
import math

default_args = {
    "owner": "airflow"
}

@dag(
    schedule="7/10 * * * *",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    tags=["etl", "to_data_mart", "from_raw_vault"],
)
def transfer_to_dm_common_from_raw_vault_dag():

    @task(task_id="get_last_seq_id")
    def get_last_seq_id():
        dwh_ch_hook = ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        connection = dwh_ch_hook.get_conn()
        max_id = connection.execute(
            """
                SELECT MAX(dim_consumer_id) AS dim_consumer_id
                FROM dm_common.dim_consumer
            """
        )
        print(f"Selected max_seq_id {max_id}")
        if max_id:
            return max_id[0][0]
        return 0

    @task(task_id="get_last_dt")
    def get_last_dt():
        dwh_ch_hook = ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        connection = dwh_ch_hook.get_conn()
        last_dt = connection.execute(
            """
                SELECT MAX(valid_from) AS last_dt
                FROM dm_common.dim_consumer;
            """
        )
        print(f"Selected last_dt {last_dt}")
        if last_dt:
            return last_dt[0][0]
        return datetime(1970, 1, 1)

    @task(task_id="transfer_data_to_dim_consumer")
    def transfer_data_to_dim_consumer(last_seq_id: int, last_dt: datetime):
        dwh_pg_hook=PostgresHook(postgres_conn_id="dwh_postgres_raw_vault_to_data_mart")
        dwh_ch_hook=ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        pg_data = dwh_pg_hook.get_pandas_df(
            sql="""
                SELECT
                    sat.name,
                    sat.lastname,
                    sat.patronymic,
                    sat.email,
                    sat.create_dt,
                    sat.update_dt as valid_from,
                    sat.hub_consumer_hash_key,
                    hub.consumer_id
                FROM 
                    raw_vault.sat_consumer sat 
                    JOIN raw_vault.hub_consumer hub USING (hub_consumer_hash_key) 
                WHERE
                    update_dt > %(last_dt)s
            """,
            parameters={"last_dt": last_dt},
        )
        print(f"Selected {len(pg_data)} rows")
        pg_data['dim_consumer_id'] = range(last_seq_id + 1, last_seq_id + len(pg_data) + 1)
        pg_data['hub_consumer_hash_key'] = pg_data['hub_consumer_hash_key'].astype(str)
        pg_data['consumer_id'] = pg_data['consumer_id'].astype(str)

        df_to_insert = pg_data[[
            'dim_consumer_id', 'name', 'lastname', 'patronymic', 'email',
            'create_dt', 'valid_from', 'hub_consumer_hash_key', 'consumer_id'
        ]]

        data = []
        for row in df_to_insert.itertuples(index=False):
            clean_row = tuple(
                None if isinstance(val, float) and math.isnan(val) else val
                for val in row
            )
            data.append(clean_row)

        insert_sql = """
            INSERT INTO dm_common.dim_consumer (dim_consumer_id, name, lastname, patronymic, email, create_dt, 
                                                valid_from, hub_consumer_hash_key, consumer_id)
            VALUES 
        """
        connection = dwh_ch_hook.get_conn()
        connection.execute(insert_sql, data)
        print(f"Inserted {len(pg_data)} rows")

    last_seq_id = get_last_seq_id()
    last_dt = get_last_dt()
    transfer_data_to_dim_consumer(last_seq_id, last_dt)

transfer_to_dm_common_from_raw_vault_dag()