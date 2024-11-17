from airflow.hooks.base import BaseHook
import requests, json
from minio import Minio
from io import BytesIO

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metric=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers = api.extra_dejson['headers'])

    return json.dumps(response.json()['chart']['result'][0])


def __store_prices(stock):
    minio = BaseHook.get_connection('minio')
    client = Minio(
    endpoint = minio.extra_dejson['enpoint_url'].split('//')[1],
    access_key = minio.login,
    secret_key = minio.password,
    secure = False
    )

    bucket_name = 'stockmarket'

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')

    objw = client.put_object(
        bucket_name = bucket_name,
        object_name = f"{symbol}/prices.json",
        # reading data in-memery so need to use bytesio
        data = BytesIO(data),
        length = len(data)


    )
    return f"{objw.bucket_name}/{symbol}"









