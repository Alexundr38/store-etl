from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.sdk import dag, task
from datetime import datetime

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
def transfer_to_order_item_mart_dag():

    @task
    def get_last_seq_id(column_name: str, db_name: str, table_name: str):
        dwh_ch_hook = ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        client = dwh_ch_hook.get_conn()
        max_id = client.execute(
            f"""
                SELECT MAX(`{column_name}`) AS `{column_name}`
                FROM `{db_name}`.`{table_name}`
            """
        )
        print(f"Selected max_seq_id {max_id}")
        if max_id:
            return max_id[0][0]
        return 0

    @task
    def get_last_dt(column_name: str, db_name: str, table_name: str):
        dwh_ch_hook = ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        client = dwh_ch_hook.get_conn()
        last_dt = client.execute(
            f"""
                SELECT MAX(`{column_name}`) AS last_dt
                FROM `{db_name}`.`{table_name}`
            """
        )
        print(f"Selected last_dt {last_dt}")
        if last_dt:
            return last_dt[0][0]
        return datetime(1970, 1, 1)

    @task
    def transfer_data_to_dim(
            source_db_name: str,
            source_table_name: str,
            selected_columns: list[str],
            id_column: str,
            hash_key_column: str,
            target_db_name: str,
            target_table_name: str,
            last_seq_id: int,
            last_dt: datetime
    ):
        dwh_pg_hook=PostgresHook(postgres_conn_id="dwh_postgres_raw_vault_to_data_mart")
        dwh_ch_hook=ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        select_cols_str = ', '.join(selected_columns)
        pg_data = dwh_pg_hook.get_pandas_df(
            sql=f"""
                SELECT
                    {hash_key_column},
                    {select_cols_str},
                    now() AS load_dt
                FROM 
                    "{source_db_name}"."{source_table_name}"
                WHERE
                    load_dt > %(last_dt)s
            """,
            parameters={
                "last_dt": last_dt,
            },
        )
        print(f"Selected {len(pg_data)} rows")
        pg_data[f'{id_column}'] = range(last_seq_id + 1, last_seq_id + len(pg_data) + 1)
        pg_data[f'{hash_key_column}'] = pg_data[f'{hash_key_column}'].astype(str)

        insert_columns = [id_column, 'load_dt', hash_key_column] + selected_columns
        df_to_insert = pg_data[insert_columns]

        data = df_to_insert.to_dict('records')

        insert_sql = f"""
            INSERT INTO `{target_db_name}`.`{target_table_name}` ({', '.join(insert_columns)})
            VALUES 
        """
        client = dwh_ch_hook.get_conn()
        client.execute(insert_sql, data)
        print(f"Inserted {len(pg_data)} rows")





    @task(task_id="transfer_data_to_fact_order_item")
    def transfer_data_to_fact_order_item(last_seq_id: int, last_dt: datetime):
        dwh_pg_hook = PostgresHook(postgres_conn_id="dwh_postgres_raw_vault_to_data_mart")
        dwh_ch_hook = ClickHouseHook(clickhouse_conn_id="dwh_clickhouse_raw_vault_to_data_mart")
        pg_data = dwh_pg_hook.get_pandas_df(
            sql="""
                SELECT
                    sat_oi.count_item,
                    sat_i.price AS price_item,
                    sat_oi.count_item * sat_i.price AS amount,
                    sat_o.order_dt,
                    link_oi.link_order_item_hash_key,
                    hub_i.hub_item_hash_key,
                    link_ocs.hub_store_hash_key,
                    hub_o.order_id,
                    link_ocs.hub_consumer_hash_key
                FROM raw_vault.link_order_item link_oi
                     JOIN raw_vault.sat_order_item sat_oi USING (link_order_item_hash_key)
                     JOIN raw_vault.hub_item hub_i USING (hub_item_hash_key)
                     JOIN raw_vault.sat_item sat_i USING (hub_item_hash_key)
                     JOIN raw_vault.hub_order hub_o USING (hub_order_hash_key)
                     JOIN raw_vault.sat_order sat_o USING (hub_order_hash_key)
                     JOIN raw_vault.link_order_consumer_store link_ocs USING (hub_order_hash_key)
                WHERE link_oi.load_dt > %(last_dt)s
                """,
            parameters={"last_dt": last_dt},
        )
        print(f"Selected {len(pg_data)} rows")
        if len(pg_data) == 0:
            return

        pg_data['link_order_item_hash_key'] = pg_data['link_order_item_hash_key'].astype(str)
        pg_data['order_id'] = pg_data['order_id'].astype(str)
        pg_data['fact_order_item_id'] = range(last_seq_id + 1, last_seq_id + len(pg_data) + 1)

        print(pg_data.iloc[0])
        client = dwh_ch_hook.get_conn()

        client.execute("""
            CREATE TEMPORARY TABLE tmp_order_item(
                fact_order_item_id UInt64,
                link_order_item_hash_key UUID,
                order_dt DateTime,
                order_id UUID,
                count_item UInt8,
                price_item DECIMAL(8,2),
                amount DECIMAL(8,2),
                hub_item_hash_key UUID,
                hub_store_hash_key UUID,
                hub_consumer_hash_key UUID
            ) ENGINE = Memory
        """)

        print(f"Created temp table")

        insert_data = []
        for row in pg_data.itertuples(index=False):
            insert_data.append((
                row.fact_order_item_id,
                row.link_order_item_hash_key,
                row.order_dt,
                row.order_id,
                row.count_item,
                row.price_item,
                row.amount,
                row.hub_item_hash_key,
                row.hub_store_hash_key,
                row.hub_consumer_hash_key
            ))

        client.execute(
            """
            INSERT INTO tmp_order_item (fact_order_item_id, link_order_item_hash_key, order_dt, 
                                     order_id, count_item, price_item, amount, 
                                     hub_item_hash_key, hub_store_hash_key, hub_consumer_hash_key)
            VALUES
            """,
            insert_data
        )

        print(f"Inserted to temp table {len(insert_data)} rows")

        insert_sql = """
                     INSERT INTO dm_order_item.fact_order_item
                     (fact_order_item_id, link_order_item_hash_key, order_dt, order_id, count_item, 
                      price_item, amount, load_dt, dim_item_id, dim_consumer_id, dim_store_id)
                     SELECT tmp.fact_order_item_id, 
                            tmp.link_order_item_hash_key,
                            tmp.order_dt,
                            tmp.order_id,
                            tmp.count_item,
                            tmp.price_item,
                            tmp.amount,
                            now() AS load_dt,
                            dm_i.dim_item_id,
                            dm_c.dim_consumer_id, 
                            dm_s.dim_store_id
                     FROM 
                         tmp_order_item tmp 
                         JOIN dm_order_item.dim_item dm_i USING (hub_item_hash_key)
                         JOIN dm_order_item.dim_store dm_s USING (hub_store_hash_key)
                         ASOF JOIN dm_common.dim_consumer AS dm_c
                                ON tmp.hub_consumer_hash_key = dm_c.hub_consumer_hash_key 
                                AND tmp.order_dt >= dm_c.valid_from;
                     """

        client.execute(insert_sql)

        print(f"Inserted {len(insert_data)} rows")

    store_seq = get_last_seq_id.override(task_id="get_last_seq_id_dim_store")(
        "dim_store_id", "dm_order_item", "dim_store"
    )
    store_dt = get_last_dt.override(task_id="get_last_dt_dim_store")(
        "load_dt", "dm_order_item", "dim_store"
    )
    store_task = transfer_data_to_dim.override(task_id='transfer_data_to_dim_store')(
        source_db_name='raw_vault',
        source_table_name='sat_store',
        selected_columns=['name', 'address'],
        id_column='dim_store_id',
        hash_key_column='hub_store_hash_key',
        target_db_name='dm_order_item',
        target_table_name='dim_store',
        last_seq_id=store_seq,
        last_dt=store_dt
    )

    item_seq = get_last_seq_id.override(task_id="get_last_seq_id_dim_item")(
        "dim_item_id", "dm_order_item", "dim_item"
    )
    item_dt = get_last_dt.override(task_id="get_last_dt_dim_item")(
        "load_dt", "dm_order_item", "dim_item"
    )
    item_task = transfer_data_to_dim.override(task_id='transfer_data_to_dim_item')(
        source_db_name='raw_vault',
        source_table_name='sat_item',
        selected_columns=['name', 'price'],
        id_column='dim_item_id',
        hash_key_column='hub_item_hash_key',
        target_db_name='dm_order_item',
        target_table_name='dim_item',
        last_seq_id=item_seq,
        last_dt=item_dt
    )

    fact_seq = get_last_seq_id.override(task_id="get_last_seq_id_fact_order_item")(
        "fact_order_item_id", "dm_order_item", "fact_order_item"
    )
    fact_dt = get_last_dt.override(task_id="get_last_dt_fact_order_item")(
        "load_dt", "dm_order_item", "fact_order_item"
    )
    fact_task = transfer_data_to_fact_order_item(last_seq_id=fact_seq, last_dt=fact_dt)

    [store_task, item_task] >> fact_task


transfer_to_order_item_mart_dag()