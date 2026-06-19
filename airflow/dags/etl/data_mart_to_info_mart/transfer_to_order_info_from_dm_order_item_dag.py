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
def transfer_to_order_info_from_dm_order_item_dag():

    @task(task_id='get_last_time')
    def get_last_time():
        dwh_hook = ClickHouseHook(clickhouse_conn_id='dwh_clickhouse_data_mart_to_info_mart')
        connection = dwh_hook.get_conn()
        last_time = connection.execute(
            """
            SELECT MAX(time_start_interval)
            FROM info_mart.order_info
            """
        )
        last_time = last_time[0][0]
        print(f"Selected last time {last_time}")
        if last_time == datetime(1970, 1, 1, 0, 0, 0):
            return last_time
        else:
            return last_time - timedelta(days=1)

    @task(task_id='transfer_data_to_order_info')
    def transfer_data_to_order_info(last_time: datetime):
        dwh_hook = ClickHouseHook(clickhouse_conn_id='dwh_clickhouse_data_mart_to_info_mart')
        connection = dwh_hook.get_conn()

        insert_sql = """
            INSERT INTO info_mart.order_info (time_start_interval, total_quantity, total_amount, 
                                              avg_amount, total_order, unique_consumer, unique_store)
                SELECT
                    toStartOfInterval(f_oi.order_dt, INTERVAL 10 MINUTE) AS time_start_inerval,
                    SUM(f_oi.count_item) AS total_quantity,
                    SUM(f_oi.amount) AS total_amount,
                    AVG(f_oi.amount) AS avg_amount,
                    COUNT(DISTINCT f_oi.order_id) AS total_order,
                    COUNT(DISTINCT f_oi.dim_consumer_id) AS unique_consumer,
                    COUNT(DISTINCT f_oi.dim_store_id) AS unique_store
                FROM
                    dm_order_item.fact_order_item f_oi
                WHERE 
                    f_oi.order_dt > %(last_time)s
                GROUP BY 
                    time_start_inerval
                ORDER BY 
                    time_start_inerval;
        """
        inserted_rows_count = connection.execute(insert_sql, params={'last_time': last_time})
        print(f"Inserted {inserted_rows_count} rows")

    last_time = get_last_time()
    transfer_data_to_order_info(last_time)

transfer_to_order_info_from_dm_order_item_dag()

