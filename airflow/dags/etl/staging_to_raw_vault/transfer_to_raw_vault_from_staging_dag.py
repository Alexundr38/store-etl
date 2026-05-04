import uuid
from airflow.sdk import dag, task, task_group
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib

default_args = {
    'owner': 'airflow_administrator',
    'schedule': '5/10 * * * *',
}

@dag(
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    tags=['etl', 'from_staging', 'to_raw_vault']
)
def transfer_to_raw_vault_from_staging_dag():

    def compute_hash_key(id: str):
        id = id.strip().lower().replace('-', '')
        md5 = hashlib.md5(id.encode('utf-8')).digest()
        return uuid.UUID(bytes=md5)

    @task(task_id='get_load_dt')
    def get_load_dt() -> str:
        return datetime.now().isoformat()

    @task_group(group_id='load_hubs')
    def load_hubs(load_dt):
        @task(task_id='load_hub_category')
        def load_hub_category(load_dt):
            dwh_hook = PostgresHook(postgres_conn_id='dwh_postgres_staging_to_raw_vault_transfer')
            connection = dwh_hook.get_conn()
            cursor = connection.cursor()

            cursor.execute("""
                SELECT category_id
                FROM staging.category
            """)
            staging_category = [row[0] for row in cursor.fetchall()]
            print(f'Selected {len(staging_category)} rows from staging.category')

            raw_vault_data = []
            for category_id in staging_category:
                raw_vault_data.append((
                    compute_hash_key(category_id),
                    category_id,
                    load_dt,
                    'backend-postgres'      #TODO change const
                ))

            insert_sql = """
                INSERT INTO raw_vault.hub_category (hub_category_hash_key, category_id, load_dt, record_source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (hub_category_hash_key) DO NOTHING
            """
            cursor.executemany(insert_sql, raw_vault_data)
            print(f'Inserted max {len(raw_vault_data)} rows to raw_vault.hub_category')

            connection.commit()
            cursor.close()
            connection.close()

        load_hub_category(load_dt)

    @task_group(group_id='load_satellites')
    def load_satellites(load_dt):
        @task(task_id='load_sat_category')
        def load_sat_category(load_dt):
            dwh_hook = PostgresHook(postgres_conn_id='dwh_postgres_staging_to_raw_vault_transfer')
            connection = dwh_hook.get_conn()
            cursor = connection.cursor()

            cursor.execute("""
                SELECT category_id, name
                FROM staging.category
            """)
            staging_rows = cursor.fetchall()
            print(f'Selected {len(staging_rows)} rows from staging.category')

            raw_vault_data = []
            for category_id, name in staging_rows:
                raw_vault_data.append((
                    compute_hash_key(category_id),
                    load_dt,
                    name,
                    'backend-postgres'
                ))

            insert_sql = """
                INSERT INTO raw_vault.sat_category (hub_category_hash_key, load_dt, name, record_source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (hub_category_hash_key, load_dt) DO NOTHING
            """
            cursor.executemany(insert_sql, raw_vault_data)
            print(f'Inserted max {len(raw_vault_data)} rows to raw_vault.sat_category')

            connection.commit()
            cursor.close()
            connection.close()

        load_sat_category(load_dt)

    load_dt = get_load_dt()
    load_hubs(load_dt)
    load_satellites(load_dt)

transfer_to_raw_vault_from_staging_dag()