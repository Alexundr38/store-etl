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
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')
    response = post_hook.run(
        endpoint='/consumer/login/',
        json={
            'consumer_id': consumer_id,
            'password': '111111'
        }
    )
    response.raise_for_status()

def get_store(ti):
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    get_hook = HttpHook(http_conn_id='backend_api', method='GET')
    response = get_hook.run(
        endpoint='/store/',
        data={
            'consumer_id': consumer_id,
        }
    )
    response.raise_for_status()
    stores = response.json()
    return stores[randint(0, len(stores) - 1)]['store_id']

def get_items(ti):
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    get_hook = HttpHook(http_conn_id='backend_api', method='GET')
    response = get_hook.run(
        endpoint='/cart/',
        data={
            'consumer_id': consumer_id,
        }
    )
    response.raise_for_status()
    items = response.json()
    return items

def create_order(ti):
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    store_id = ti.xcom_pull(task_ids='get_store')
    items = ti.xcom_pull(task_ids='get_items')
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')

    if len(items) == 0:
        return

    count_items = randint(1, min(len(items), 3))
    selected_items = set()
    for _ in range(count_items):
        selected_items.add(items[randint(0, len(items) - 1)]['item_id'])
    selected_items = list(selected_items)
    response = post_hook.run(
        endpoint='/cart/create_order/',
        json={
            'consumer_id': consumer_id,
            'store_id': store_id,
            'items': selected_items,
        }
    )
    response.raise_for_status()

def logout_consumer(ti):
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')
    response = post_hook.run(
        endpoint='/consumer/logout/',
        json={
            'consumer_id': consumer_id,
        }
    )
    response.raise_for_status()


with (DAG(
    dag_id='create_order_dag',
    start_date=datetime.now(),
    catchup=False,
    schedule='*/2 * * * *',
    default_args=default_args,
    tags=['store_bot'],
) as dag):

    get_consumer_task = PythonOperator(
        task_id='get_consumer',
        python_callable=get_consumer,
    )
    login_consumer_task = PythonOperator(
        task_id='login_consumer',
        python_callable=login_consumer,
    )
    get_store_task = PythonOperator(
        task_id='get_store',
        python_callable=get_store,
    )
    get_items_task = PythonOperator(
        task_id='get_items',
        python_callable=get_items,
    )
    create_order_task = PythonOperator(
        task_id='create_order',
        python_callable=create_order,
    )
    logout_consumer_task = PythonOperator(
        task_id='logout_consumer',
        python_callable=logout_consumer,
    )
    get_consumer_task >> login_consumer_task
    login_consumer_task >> [get_items_task, get_store_task] >> create_order_task
    create_order_task >> logout_consumer_task
