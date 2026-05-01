from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime
from random import randint

default_args = {
    'owner': 'airflow_administrator',
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
            'consumer_id': consumer_id,
        }
    )
    response.raise_for_status()

def get_cart_item(ti):
    get_hook = HttpHook(http_conn_id='backend_api', method='GET')
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    response = get_hook.run(
        endpoint='/cart/',
        data={
            'consumer_id': consumer_id,
        }
    )
    response.raise_for_status()
    cart_item = response.json()
    return cart_item

def delete_cart_item(ti):
    delete_hook = HttpHook(http_conn_id='backend_api', method='DELETE')
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    items = ti.xcom_pull(task_ids='get_cart_item')
    count_items = min(len(items), 3)
    deleted_items = set()
    for _ in range(count_items):
        item_id = items[randint(0, len(items) - 1)]['item_id']
        if item_id in deleted_items:
            continue
        deleted_items.add(item_id)
        response = delete_hook.run(
            endpoint='/cart/delete/',
            json={
                'consumer_id': consumer_id,
                'item_id': item_id,
            }
        )
        response.raise_for_status()



with DAG(
    dag_id='delete_cart_item_dag',
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    schedule='*/2 * * * *',
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
    get_cart_item_task = PythonOperator(
        task_id='get_cart_item',
        python_callable=get_cart_item,
    )
    delete_cart_item_task = PythonOperator(
        task_id='delete_cart_item',
        python_callable=delete_cart_item,
    )

    get_consumer_task >> login_consumer_task >> get_cart_item_task
    get_cart_item_task >> delete_cart_item_task >> logout_consumer_task