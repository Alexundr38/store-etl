from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.sdk import dag, task
from datetime import datetime
import numpy as np


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
def transfer_to_consumer_change_mart_from_raw_vault_dag():

    @task(task_id="get_last_seq_id")
    def get_last_seq_id():
        dwh_ch_hook = ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        client = dwh_ch_hook.get_conn()
        max_id = client.execute(
            """
                SELECT MAX(fact_consumer_change_id) AS dim_consumer_id
                FROM dm_consumer_change.fact_consumer_change
            """
        )
        print(f"Selected max_seq_id {max_id}")
        if max_id:
            return max_id[0][0]
        return 0

    @task(task_id="get_last_dt")
    def get_last_dt():
        dwh_ch_hook = ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        client = dwh_ch_hook.get_conn()
        last_dt = client.execute(
            """
                SELECT MAX(change_dt) AS last_dt
                FROM dm_consumer_change.fact_consumer_change;
            """
        )
        print(f"Selected last_dt {last_dt}")
        if last_dt:
            return last_dt[0][0]
        return datetime(1970, 1, 1)

    @task(task_id="transfer_data_to_fact_consumer_change")
    def transfer_data_to_fact_consumer_change(last_seq_id: int, last_dt: datetime):
        dwh_pg_hook=PostgresHook(postgres_conn_id="dwh_postgres_raw_vault_to_data_mart")
        dwh_ch_hook=ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        pg_data = dwh_pg_hook.get_pandas_df(
            sql="""
                SELECT
                    sat.create_dt,
                    sat.update_dt as change_dt,
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
        if len(pg_data) == 0:
            return

        pg_data['hub_consumer_hash_key'] = pg_data['hub_consumer_hash_key'].astype(str)
        pg_data['consumer_id'] = pg_data['consumer_id'].astype(str)
        pg_data['changes_type'] = np.where(
            pg_data['change_dt'].dt.strftime('%Y-%m-%d %H:%M:%S') == pg_data['create_dt'].dt.strftime(
                '%Y-%m-%d %H:%M:%S'),
            'create', 'update'
        )
        pg_data['fact_consumer_change_id'] = range(last_seq_id + 1, last_seq_id + len(pg_data) + 1)

        client = dwh_ch_hook.get_conn()

        client.execute("""
            CREATE TEMPORARY TABLE tmp_changes(
                fact_consumer_change_id UInt64,
                hub_consumer_hash_key UUID,
                change_dt DateTime,
                consumer_id UUID,
                changes_type String,
            ) ENGINE = Memory
        """)

        print(f"Created temp table")

        insert_data = []
        for row in pg_data.itertuples(index=False):
            insert_data.append((
                row.fact_consumer_change_id,
                row.hub_consumer_hash_key,
                row.change_dt,
                row.consumer_id,
                row.changes_type
            ))

        client.execute(
            """
            INSERT INTO tmp_changes (fact_consumer_change_id, hub_consumer_hash_key, change_dt, consumer_id, changes_type) 
            VALUES
            """,
            insert_data
        )

        print(f"Inserted to temp table {len(insert_data)} rows")

        insert_sql = """
            INSERT INTO dm_consumer_change.fact_consumer_change (fact_consumer_change_id, dim_consumer_id, changes_type, change_dt, load_dt) 
               SELECT 
                   tmp.fact_consumer_change_id, 
                   dm.dim_consumer_id, 
                   tmp.changes_type, 
                   tmp.change_dt, 
                   now() 
               FROM 
                   tmp_changes tmp
               JOIN dm_common.dim_consumer dm ON tmp.hub_consumer_hash_key = dm.hub_consumer_hash_key AND
                                                 tmp.change_dt = dm.valid_from AND
                                                 tmp.consumer_id = dm.consumer_id
        """

        client.execute(insert_sql)

        print(f"Inserted {len(insert_data)} rows")

    last_seq_id = get_last_seq_id()
    last_dt = get_last_dt()
    transfer_data_to_fact_consumer_change(last_seq_id, last_dt)

transfer_to_consumer_change_mart_from_raw_vault_dag()