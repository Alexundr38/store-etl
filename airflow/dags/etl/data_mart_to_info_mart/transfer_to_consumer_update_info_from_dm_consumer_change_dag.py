from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.sdk import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
}

@dag(
    default_args=default_args,
    schedule='*/30 * * * *',
    start_date=datetime.now(),
    catchup=False,
    tags=['etl', 'from_data_mart', 'to_info_mart']
)
def transfer_to_consumer_update_info_from_dm_consumer_change_dag():

    @task(task_id='get_last_time')
    def get_last_time():
        dwh_hook = ClickHouseHook(clickhouse_conn_id='dwh_clickhouse_data_mart_to_info_mart')
        connection = dwh_hook.get_conn()
        last_time = connection.execute(
            """
            SELECT MAX(time_start_interval)
            FROM info_mart.consumer_update_info
            """
        )
        last_time = last_time[0][0]
        print(f"Selected last time {last_time}")
        if last_time == datetime(1970, 1, 1, 0, 0, 0):
            return last_time
        else:
            return last_time - timedelta(days=1)

    @task(task_id='transfer_data_to_consumer_update_info')
    def transfer_data_to_consumer_update_info(last_time: datetime):
        dwh_hook = ClickHouseHook(clickhouse_conn_id='dwh_clickhouse_data_mart_to_info_mart')
        connection = dwh_hook.get_conn()

        insert_sql = """
            INSERT INTO info_mart.consumer_update_info (time_start_interval, total_create, total_update, unique_updated_consumer)
                SELECT
                    toStartOfInterval(f_cc.change_dt, INTERVAL 10 MINUTE) AS time_start_interval,
                    SUM(IF(f_cc.changes_type = 'create', 1, 0)) AS total_create,
                    SUM(IF(f_cc.changes_type = 'update', 1, 0)) AS total_update,
                    COUNT(DISTINCT CASE WHEN f_cc.changes_type = 'update' THEN d_c.consumer_id END) AS unique_updated_consumer
                FROM
                    dm_consumer_change.fact_consumer_change f_cc
                    JOIN dm_common.dim_consumer d_c USING (dim_consumer_id)
                WHERE 
                    f_cc.change_dt > %(last_time)s
                GROUP BY
                    time_start_interval;
        """
        inserted_rows_count = connection.execute(insert_sql, params={'last_time': last_time})
        print(f"Inserted {inserted_rows_count} rows")

    last_time = get_last_time()
    transfer_data_to_consumer_update_info(last_time)

transfer_to_consumer_update_info_from_dm_consumer_change_dag()

