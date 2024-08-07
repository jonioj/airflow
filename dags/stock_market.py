from airflow.decorators import dag, task
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
import requests
from include.stock_market.tasks import _get_stock_prices, _store_prices, _transform_data


@dag(
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock_market'],
)
def stock_market(stock="NVDA"):

    @task.sensor(poke_interval=5, timeout=10, mode='poke')
    def is_api_avaiable(stock) -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}/{stock}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.status_code == 200
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': 'stock-market/AAPL/'
        }
        
    )

    url = is_api_avaiable(stock)
    prices = _get_stock_prices(url)
    objw_url = _store_prices(prices) >> format_prices


stock_market()
