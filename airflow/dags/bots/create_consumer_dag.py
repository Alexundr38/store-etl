from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
from random import randint

default_args = {
    'owner': 'airflow_administrator',
}

with DAG(
    dag_id="crate_consumer_dag",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    schedule='*/5 * * * *',
    tags=['store_bot']
) as dag:

    def create_consumer():
        count_consumer = randint(0, 3)

        post_hook = HttpHook(http_conn_id="backend_api", method='POST')

        for i in range(count_consumer):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
            name = f"name_{timestamp}"
            last_name = f"last_name_{timestamp}"
            patronymic = f"patronymic_{timestamp}"
            email = f"email_{timestamp}@gmail.com"
            password = "111111"

            response = post_hook.run(
                endpoint='/consumer/create/',
                json={
                  "name": name,
                  "lastname": last_name,
                  "patronymic": patronymic if randint(0, 1) else None,
                  "email": email,
                  "password": password
                }
            )
            response.raise_for_status()

    create_consumer_task = PythonOperator(
        task_id="create_consumer",
        python_callable=create_consumer,
    )