import uuid
from airflow.sdk import dag, task, task_group
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib
from typing import List, Union, Optional, Tuple


def compute_hash_key(id: Union[Tuple[str], str]) -> str:
    if isinstance(id, str):
        id_row = id.strip().lower().replace('-', '')
    else:
        id_row = '|'.join([idx.strip().lower().replace('-','') for idx in id])
    md5 = hashlib.md5(id_row.encode('utf-8')).digest()
    return str(uuid.UUID(bytes=md5))

default_args = {
    'owner': 'airflow_administrator'
}

@dag(
    default_args=default_args,
    schedule='5/10 * * * *',
    start_date=datetime.now(),
    catchup=False,
    tags=['etl', 'from_staging', 'to_raw_vault']
)
def transfer_to_raw_vault_from_staging_dag():


    @task(task_id='get_load_dt')
    def get_load_dt() -> str:
        return datetime.now().isoformat()


    @task
    def load_hub(
            load_dt: str,
            source_table: str,
            source_schema: str,
            target_table: str,
            target_schema: str,
            business_key_col: str,
            hash_key_col: str,
            source_system_col: str,
            conn_id: str
    ):
        dwh_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = dwh_hook.get_conn()
        cursor = connection.cursor()

        try:
            select_sql = f"""
                SELECT
                    {business_key_col},
                    {source_system_col}
                FROM
                    {source_schema}.{source_table}
            """
            cursor.execute(select_sql)

            staging_rows = cursor.fetchall()
            print(f'Selected {len(staging_rows)} rows from {source_schema}.{source_table}')

            raw_vault_data = []
            for row in staging_rows:
                business_key, source_system_name = row
                raw_vault_data.append((
                    compute_hash_key(business_key),
                    business_key,
                    load_dt,
                    source_system_name
                ))

            insert_sql = f"""
                INSERT INTO {target_schema}.{target_table} ({hash_key_col}, {business_key_col}, load_dt, record_source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT ({hash_key_col}) DO NOTHING
            """
            cursor.executemany(insert_sql, raw_vault_data)
            print(f'Inserted max {len(raw_vault_data)} rows to {target_schema}.{target_table}')
            connection.commit()
        finally:
            cursor.close()
            connection.close()


    @task
    def load_sat_from_hub(
            load_dt: str,
            source_table: str,
            source_schema: str,
            target_table: str,
            target_schema: str,
            business_key_col: str,
            hash_key_col: str,
            needed_cols: List[str],
            source_system_col: str,
            conn_id: str
    ):
        dwh_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = dwh_hook.get_conn()
        cursor = connection.cursor()
        try:
            columns_name = ', '.join(needed_cols)
            count_columns = len(needed_cols)

            select_sql = f"""
                SELECT
                    {business_key_col},
                    {columns_name},
                    {source_system_col}
                FROM
                    {source_schema}.{source_table}
            """
            cursor.execute(select_sql)

            staging_rows = cursor.fetchall()
            print(f'Selected {len(staging_rows)} rows from {source_schema}.{source_table}')

            raw_vault_data = []
            for row in staging_rows:
                business_key, *needed_data, source_system_name = row
                raw_vault_data.append((
                    compute_hash_key(business_key),
                    load_dt,
                    *needed_data,
                    source_system_name
                ))

            insert_sql = f"""
                INSERT INTO {target_schema}.{target_table} ({hash_key_col}, load_dt, {columns_name}, record_source)
                VALUES ({'%s, ' * (count_columns + 2)}%s)
                ON CONFLICT ({hash_key_col}, load_dt) DO NOTHING 
            """
            cursor.executemany(insert_sql, raw_vault_data)
            connection.commit()
        finally:
            cursor.close()
            connection.close()

    @task
    def load_sat_from_hub_with_update_dt(
            load_dt: str,
            source_table: str,
            source_schema: str,
            target_table: str,
            target_schema: str,
            business_key_col: str,
            hash_key_col: str,
            needed_cols: List[str],
            source_system_col: str,
            update_dt_col: str,
            conn_id: str
    ):
        dwh_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = dwh_hook.get_conn()
        cursor = connection.cursor()
        try:
            columns_name = ', '.join(needed_cols)
            count_columns = len(needed_cols)

            select_sql = f"""
                SELECT
                    {business_key_col},
                    {source_system_col},
                    {update_dt_col},
                    {columns_name}
                FROM
                    {source_schema}.{source_table}
            """
            cursor.execute(select_sql)

            staging_rows = cursor.fetchall()
            print(f"Selected {len(staging_rows)} rows from {source_schema}.{source_table}")

            raw_vault_data = []
            for row in staging_rows:
                business_key, source_system_name, update_dt, *needed_data = row
                raw_vault_data.append((
                    compute_hash_key(business_key),
                    load_dt,
                    update_dt,
                    *needed_data,
                    source_system_name
                ))

            insert_sql = f"""
                INSERT INTO {target_schema}.{target_table} ({hash_key_col}, load_dt, update_dt, {columns_name}, record_source)
                VALUES ({'%s, ' * (count_columns + 3)}%s)
                ON CONFLICT ({hash_key_col}, load_dt, update_dt) DO NOTHING
            """
            cursor.executemany(insert_sql, raw_vault_data)
            print(f"Inserted max {len(raw_vault_data)} rows to {target_schema}.{target_table}")

            connection.commit()

        finally:
            cursor.close()
            connection.close()

    @task
    def load_sat_from_link(
            load_dt: str,
            source_table: str,
            source_schema: str,
            target_table: str,
            target_schema: str,
            business_key_cols: List[str],
            hash_key_col: str,
            needed_cols: List[str],
            source_system_col: str,
            conn_id: str
    ):
        dwh_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = dwh_hook.get_conn()
        cursor = connection.cursor()
        try:
            columns_name = ', '.join(needed_cols)
            business_column_names = ', '.join(business_key_cols)
            count_columns = len(needed_cols)
            count_business_columns = len(business_key_cols)

            select_sql = f"""
                    SELECT
                        {business_column_names},
                        {columns_name},
                        {source_system_col}
                    FROM
                        {source_schema}.{source_table}
                """
            cursor.execute(select_sql)

            staging_rows = cursor.fetchall()
            print(f'Selected {len(staging_rows)} rows from {source_schema}.{source_table}')
            raw_vault_data = []
            for row in staging_rows:
                business_keys = row[:count_business_columns]
                needed_data = row[count_business_columns:-1]
                source_system_name = row[-1]
                raw_vault_data.append((
                    compute_hash_key(business_keys),
                    load_dt,
                    *needed_data,
                    source_system_name
                ))

            insert_sql = f"""
                    INSERT INTO {target_schema}.{target_table} ({hash_key_col}, load_dt, {columns_name}, record_source)
                    VALUES ({'%s, ' * (count_columns + 2)}%s)
                    ON CONFLICT ({hash_key_col}, load_dt) DO NOTHING 
                """
            cursor.executemany(insert_sql, raw_vault_data)
            connection.commit()
        finally:
            cursor.close()
            connection.close()

    @task
    def load_sat_consumer_item(
            load_dt: str,
            source_table: str,
            source_schema: str,
            target_table: str,
            target_schema: str,
            business_key_cols: List[str],
            hash_key_col: str,
            needed_cols: List[str],
            source_system_col: str,
            update_dt_col: str,
            conn_id: str,
            where_clause: Optional[str] = None
    ):
        dwh_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = dwh_hook.get_conn()
        cursor = connection.cursor()
        try:
            columns_name = ', '.join(needed_cols)
            business_column_names = ', '.join(business_key_cols)
            count_columns = len(needed_cols)
            count_business_columns = len(business_key_cols)

            if where_clause:
                select_sql = f"""
                    SELECT
                        {business_column_names},
                        {columns_name},
                        endpoint,
                        {update_dt_col},
                        {source_system_col}
                    FROM
                        {source_schema}.{source_table}
                    WHERE
                        {where_clause}
                """
            else:
                select_sql = f"""
                    SELECT
                        {business_column_names},
                        {columns_name},
                        endpoint,
                        {update_dt_col},
                        {source_system_col}
                    FROM
                        {source_schema}.{source_table}
                """
            cursor.execute(select_sql)

            staging_rows = cursor.fetchall()
            print(f'Selected {len(staging_rows)} rows from {source_schema}.{source_table}')

            raw_vault_data = []
            for row in staging_rows:
                business_keys = row[:count_business_columns]
                needed_data = row[count_business_columns:-3]
                endpoint = row[-3]
                update_dt = row[-2]
                source_system_name = row[-1]
                if 'delete' in endpoint:
                    needed_data = (0,)
                raw_vault_data.append((
                    compute_hash_key(business_keys),
                    load_dt,
                    update_dt,
                    'add' if 'add' in endpoint else 'delete',
                    *needed_data,
                    source_system_name
                ))

            insert_sql = f"""
                        INSERT INTO {target_schema}.{target_table} ({hash_key_col}, load_dt, update_dt, type_update, {columns_name}, record_source)
                        VALUES ({'%s, ' * (count_columns + 4)}%s)
                        ON CONFLICT ({hash_key_col}, load_dt, update_dt) DO NOTHING 
                    """
            cursor.executemany(insert_sql, raw_vault_data)
            connection.commit()
        finally:
            cursor.close()
            connection.close()



    @task
    def load_link(
            load_dt: str,
            source_table: str,
            source_schema: str,
            target_table: str,
            target_schema: str,
            business_key_cols: List[str],
            hash_key_cols: List[str],
            source_system_col: str,
            conn_id: str
    ):
        dwh_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = dwh_hook.get_conn()
        cursor = connection.cursor()
        try:
            business_column_names = ', '.join(business_key_cols)
            hash_column_names = ', '.join(hash_key_cols)
            count_columns = len(business_key_cols)

            select_sql = f"""
                SELECT 
                    {business_column_names},
                    {source_system_col}
                FROM
                    {source_schema}.{source_table}
            """
            cursor.execute(select_sql)
            staging_rows = cursor.fetchall()
            print(f"Selected {len(staging_rows)} rows from {source_schema}.{source_table}")

            raw_vault_data = []
            for row in staging_rows:
                *business_keys, source_system_name = row
                raw_vault_data.append((
                    *[compute_hash_key(key) for key in business_keys],
                    load_dt,
                    source_system_name
                ))

            insert_sql = f"""
                INSERT INTO {target_schema}.{target_table} ({hash_column_names}, load_dt, record_source)
                VALUES ({'%s, ' * (count_columns + 1)}%s)
                ON CONFLICT ({hash_column_names}) DO NOTHING
            """
            cursor.executemany(insert_sql, raw_vault_data)
            print(f"Inserted max {len(raw_vault_data)} rows to {target_schema}.{target_table}")
            connection.commit()

        finally:
            cursor.close()
            connection.close()

    @task
    def load_link_with_self_key(
            load_dt: str,
            source_table: str,
            source_schema: str,
            target_table: str,
            target_schema: str,
            business_key_cols: List[str],
            hash_key_cols: List[str],
            pk_hash_key_col: str,
            source_system_col: str,
            conn_id: str,
            where_clause: Optional[str] = None,
    ):
        dwh_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = dwh_hook.get_conn()
        cursor = connection.cursor()
        try:
            business_column_names = ', '.join(business_key_cols)
            hash_column_names = ', '.join(hash_key_cols)
            count_columns = len(business_key_cols)

            if where_clause:
                select_sql = f"""
                    SELECT 
                        {business_column_names},
                        {source_system_col}
                    FROM
                        {source_schema}.{source_table}
                    WHERE
                        {where_clause}
                """
            else:
                select_sql = f"""
                    SELECT 
                        {business_column_names},
                        {source_system_col}
                    FROM
                        {source_schema}.{source_table}
                """
            cursor.execute(select_sql)
            staging_rows = cursor.fetchall()
            print(f"Selected {len(staging_rows)} rows from {source_schema}.{source_table}")

            raw_vault_data = []
            for row in staging_rows:
                *business_keys, source_system_name = row
                raw_vault_data.append((
                    compute_hash_key(business_keys),
                    *[compute_hash_key(key) for key in business_keys],
                    load_dt,
                    source_system_name
                ))

            insert_sql = f"""
                    INSERT INTO {target_schema}.{target_table} ({pk_hash_key_col}, {hash_column_names}, load_dt, record_source)
                    VALUES ({'%s, ' * (count_columns + 2)}%s)
                    ON CONFLICT ({pk_hash_key_col}) DO NOTHING
                """
            cursor.executemany(insert_sql, raw_vault_data)
            print(f"Inserted max {len(raw_vault_data)} rows to {target_schema}.{target_table}")
            connection.commit()

        finally:
            cursor.close()
            connection.close()



    @task_group(group_id='load_hubs')
    def load_hubs(load_dt):

        load_hub.override(task_id='load_hub_category')(
            load_dt=load_dt,
            source_table='category',
            source_schema='staging',
            target_table='hub_category',
            target_schema='raw_vault',
            business_key_col='category_id',
            hash_key_col='hub_category_hash_key',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_hub.override(task_id='load_hub_item')(
            load_dt=load_dt,
            source_table='item',
            source_schema='staging',
            target_table='hub_item',
            target_schema='raw_vault',
            business_key_col='item_id',
            hash_key_col='hub_item_hash_key',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_hub.override(task_id='load_hub_order')(
            load_dt=load_dt,
            source_table='orders',
            source_schema='staging',
            target_table='hub_order',
            target_schema='raw_vault',
            business_key_col='order_id',
            hash_key_col='hub_order_hash_key',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_hub.override(task_id='load_hub_store')(
            load_dt=load_dt,
            source_table='store',
            source_schema='staging',
            target_table='hub_store',
            target_schema='raw_vault',
            business_key_col='store_id',
            hash_key_col='hub_store_hash_key',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_hub.override(task_id='load_hub_consumer')(
            load_dt=load_dt,
            source_table='consumer',
            source_schema='staging',
            target_table='hub_consumer',
            target_schema='raw_vault',
            business_key_col='consumer_id',
            hash_key_col='hub_consumer_hash_key',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

    @task_group(group_id='load_satellites')
    def load_satellites(load_dt):

        load_sat_from_hub.override(task_id='load_sat_category')(
            load_dt=load_dt,
            source_table='category',
            source_schema='staging',
            target_table='sat_category',
            target_schema='raw_vault',
            business_key_col='category_id',
            hash_key_col='hub_category_hash_key',
            needed_cols=['name'],
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_sat_from_hub.override(task_id='load_sat_item')(
            load_dt=load_dt,
            source_table='item',
            source_schema='staging',
            target_table='sat_item',
            target_schema='raw_vault',
            business_key_col='item_id',
            hash_key_col='hub_item_hash_key',
            needed_cols=['name', 'price'],
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_sat_from_hub.override(task_id='load_sat_order')(
            load_dt=load_dt,
            source_table='orders',
            source_schema='staging',
            target_table='sat_order',
            target_schema='raw_vault',
            business_key_col='order_id',
            hash_key_col='hub_order_hash_key',
            needed_cols=['order_dt'],
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_sat_from_hub.override(task_id='load_sat_store')(
            load_dt=load_dt,
            source_table='store',
            source_schema='staging',
            target_table='sat_store',
            target_schema='raw_vault',
            business_key_col='store_id',
            hash_key_col='hub_store_hash_key',
            needed_cols=['name', 'address'],
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_sat_from_hub_with_update_dt.override(task_id='load_sat_consumer')(
            load_dt=load_dt,
            source_table='consumer',
            source_schema='staging',
            target_table='sat_consumer',
            target_schema='raw_vault',
            business_key_col='consumer_id',
            hash_key_col='hub_consumer_hash_key',
            needed_cols=['name', 'lastname', 'patronymic', 'email', 'create_dt'],
            update_dt_col='update_dt',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_sat_from_link.override(task_id='load_sat_order_item')(
            load_dt=load_dt,
            source_table='order_item',
            source_schema='staging',
            target_table='sat_order_item',
            target_schema='raw_vault',
            business_key_cols=['order_id', 'item_id'],
            hash_key_col='link_order_item_hash_key',
            needed_cols=['count_item'],
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_sat_consumer_item.override(task_id='load_sat_consumer_item')(
                load_dt=load_dt,
                source_table='logs',
                source_schema='staging',
                target_table='sat_consumer_item',
                target_schema='raw_vault',
                business_key_cols=['consumer_id', 'item_id'],
                hash_key_col='link_consumer_item_hash_key',
                needed_cols=['count_item'],
                source_system_col='source_system',
                update_dt_col='event_time',
                conn_id='dwh_postgres_staging_to_raw_vault_transfer',
                where_clause="event_type IN ('delete_cart_item', 'add_item')"
        )


    @task_group(group_id='load_links')
    def load_links(load_dt):

        load_link.override(task_id='load_link_category_item')(
            load_dt=load_dt,
            source_table='item',
            source_schema='staging',
            target_table='link_category_item',
            target_schema='raw_vault',
            business_key_cols=['category_id', 'item_id'],
            hash_key_cols=['hub_category_hash_key', 'hub_item_hash_key'],
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_link.override(task_id='load_link_order_consumer_store')(
            load_dt=load_dt,
            source_table='orders',
            source_schema='staging',
            target_table='link_order_consumer_store',
            target_schema='raw_vault',
            business_key_cols=['order_id', 'consumer_id', 'store_id'],
            hash_key_cols=['hub_order_hash_key', 'hub_consumer_hash_key', 'hub_store_hash_key'],
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_link_with_self_key.override(task_id='load_link_order_item')(
            load_dt=load_dt,
            source_table='order_item',
            source_schema='staging',
            target_table='link_order_item',
            target_schema='raw_vault',
            business_key_cols=['order_id', 'item_id'],
            hash_key_cols=['hub_order_hash_key', 'hub_item_hash_key'],
            pk_hash_key_col='link_order_item_hash_key',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer'
        )

        load_link_with_self_key.override(task_id='load_link_consumer_item')(
            load_dt=load_dt,
            source_table='logs',
            source_schema='staging',
            target_table='link_consumer_item',
            target_schema='raw_vault',
            business_key_cols=['consumer_id', 'item_id'],
            hash_key_cols=['hub_consumer_hash_key', 'hub_item_hash_key'],
            pk_hash_key_col='link_consumer_item_hash_key',
            source_system_col='source_system',
            conn_id='dwh_postgres_staging_to_raw_vault_transfer',
            where_clause="event_type IN ('delete_cart_item', 'add_item')"
        )

    load_dt = get_load_dt()
    load_hubs(load_dt)
    load_satellites(load_dt)
    load_links(load_dt)

transfer_to_raw_vault_from_staging_dag()