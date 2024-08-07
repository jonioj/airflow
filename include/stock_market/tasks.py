from datetime import datetime
import pandas as pd
import requests
from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO
from airflow.decorators import task
import pandas as pd


@task
def get_stock_prices(url):
    url = f"{url}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])


@task()
def store_prices(stock):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}/'


@task
def _transform_data(objw_url):

    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    # Getting the obj
    bucket_name, ticker = objw_url.split('/')
    response = client.get_object(bucket_name, f'{ticker}/prices.json')
    data = response.read()
    content = data.decode('utf-8')
    data = json.loads(content)
    # Formatting the obj
    timestamps = data['timestamp']
    df = pd.DataFrame(data['indicators']['quote'][0])
    df['timestamp'] = list(map(lambda x: datetime.fromtimestamp(x,), timestamps))
    df['timestamp'] = df['timestamp'].dt.date
    # Writing the obj
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    client.put_object(bucket_name, f'{ticker}_formated.csv', csv_buffer, len(csv_buffer.getvalue()))
