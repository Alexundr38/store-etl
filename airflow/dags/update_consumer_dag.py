from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
from random import randint

default_args = {
    'owner': 'airflow_administrator'
}

def get_consumer():
    get_hook = HttpHook(http_conn_id='backend_api', method='GET')
    response = get_hook.run(
        endpoint='/consumer/'
    )
    response.raise_for_status()
    consumers = response.json()
    return consumers[randint(0, len(consumers) - 1)]['consumer_id']

def login_consumer(ti):
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    response = post_hook.run(
        endpoint='/consumer/login/',
        json={
            'consumer_id': consumer_id,
            'password': '111111'
        }
    )
    response.raise_for_status()

def logout_consumer(ti):
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    response = post_hook.run(
        endpoint='/consumer/logout/',
        json={
            'consumer_id': consumer_id
        }
    )
    response.raise_for_status()

def update_consumer(ti):
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    response = post_hook.run(
        endpoint='/consumer/update/',
        json={
            'consumer_id': consumer_id,
            'name': f'name_{timestamp}',
            'lastname': f'lastname_{timestamp}',
            'patronymic': f'patronymic_{timestamp}' if randint(0, 1) else None,
            'email': f'email_{timestamp}@gmail.com',
        }
    )
    response.raise_for_status()


with DAG(
    dag_id='update_consumer_dag',
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    schedule='*/15 * * * *',
    tags=['store_bot']
) as dag:

    get_consumer_task = PythonOperator(
        task_id='get_consumer',
        python_callable=get_consumer,
    )
    login_consumer_task = PythonOperator(
        task_id='login_consumer',
        python_callable=login_consumer,
    )
    logout_consumer_task = PythonOperator(
        task_id='logout_consumer',
        python_callable=logout_consumer,
    )
    update_consumer_task = PythonOperator(
        task_id='update_consumer',
        python_callable=update_consumer,
    )

    get_consumer_task >> login_consumer_task >> update_consumer_task >> logout_consumer_task