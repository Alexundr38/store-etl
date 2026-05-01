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
    items = response.json()
    consumer_id = items[randint(0, len(items) - 1)]['consumer_id']
    return consumer_id


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


def logout_consumer(ti):
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')

    response = post_hook.run(
        endpoint='/consumer/logout/',
        json={
            'consumer_id': consumer_id
        }
    )


def get_category(ti):
    get_hook = HttpHook(http_conn_id='backend_api', method='GET')
    consumer_id = ti.xcom_pull(task_ids='get_consumer')

    response = get_hook.run(
        endpoint='/category/',
        data={
            'consumer_id': consumer_id
        }
    )
    response.raise_for_status()
    items = response.json()
    category_id = items[randint(0, len(items) - 1)]['category_id']
    return category_id


def check_page(ti):
    get_hook = HttpHook(http_conn_id='backend_api', method='GET')
    count_pages = randint(1, 5)
    consumer_id = ti.xcom_pull(task_ids='get_consumer')
    category_id = ti.xcom_pull(task_ids='get_category')
    select_items = []

    for i in range(count_pages):
        page_number = randint(0, 100)
        response = get_hook.run(
            endpoint='/item/',
            data={
                'page': page_number,
                'consumer_id': consumer_id,
                'category_id': category_id
            }
        )
        response.raise_for_status()
        items = response.json()  # получили список
        count_items = randint(0, min(3, len(items)))
        for _ in range(count_items):
            select_items.append(items[randint(0, len(items) - 1)])
    return select_items


def add_item_to_cart(ti):
    post_hook = HttpHook(http_conn_id='backend_api', method='POST')
    items = ti.xcom_pull(task_ids='check_page')
    consumer_id = ti.xcom_pull(task_ids='get_consumer')

    for item in items:
        response = post_hook.run(
            endpoint='/item/add/',
            json={
                'consumer_id': consumer_id,
                'item_id': item['item_id'],
                'count_items': randint(1, 3)
            }
        )
        response.raise_for_status()


with (DAG(
    dag_id="add_item_to_cart_dag",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    schedule='*/1 * * * *',
    tags=['store_bot']
) as dag):

    get_consumer_task = PythonOperator(
        task_id='get_consumer',
        python_callable=get_consumer,
    )
    login_consumer_task = PythonOperator(
        task_id='login_consumer',
        python_callable=login_consumer,
    )
    get_category_task = PythonOperator(
        task_id='get_category',
        python_callable=get_category,
    )
    check_page_task = PythonOperator(
        task_id='check_page',
        python_callable=check_page,
    )
    add_item_to_cart_task = PythonOperator(
        task_id='add_item_to_cart',
        python_callable=add_item_to_cart,
    )
    logout_consumer_task = PythonOperator(
        task_id='logout_consumer',
        python_callable=logout_consumer,
    )

    get_consumer_task >> login_consumer_task >> get_category_task >> check_page_task
    check_page_task >> add_item_to_cart_task >> logout_consumer_task