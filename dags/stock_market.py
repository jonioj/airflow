from airflow.decorators import dag, task
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
import requests


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
    is_api_avaiable()


stock_market()
