from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
import requests
from include.stock_market.tasks import _get_stock_prices, _store_prices, _transform_data


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_avaiable() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.status_code == 200
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={
            'url': '{{task_instance.xcom_pull(task_ids="is_api_avaiable")}}'}
    )

    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        op_kwargs={
            'stock': '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}
    )
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_data)
    is_api_avaiable() >> get_stock_prices >> store_prices >> transform_data


stock_market()
