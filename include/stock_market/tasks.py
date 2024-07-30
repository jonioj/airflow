import requests
from airflow.hooks.base import BaseHook
import json


def _get_stock_prices(url):
    url = f"{url}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])
