from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import requests

from include.stockmarket.tasks import _get_stock_prices, __store_prices

SYMBOL = 'AAPL'

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags = ['stock_market']

)
def stock_market():
    
    @task.sensor(poke_interval = 30, timeout = 300, mode = 'poke')
    def is_api_availble() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"

        response = requests.get(url, headers=api.extra_dejson['headers'])

        condition = response.json()['finance']['result'] is None

        print(url)

        return PokeReturnValue(is_done = condition, xcom_value = url)


    
    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable = _get_stock_prices,
        
        op_kwargs = {'url': '{{task_instance.xcom_pull(task_ids = "is_api_availble")}}', 'symbol': SYMBOL}
    )
    

    stock_prices = PythonOperator(
        task_id = 'stock_prices',
        python_callable = __store_prices,

        op_kwargs = {'stock': '{{task_instance.xcom_pull(task_ids = "get_stock_prices")}}'}

    )

    format_prices = DockerOperator(
        task_id = 'Format_prices',
        image = 'airflow/stock-app',
        container_name ='format_prices',
        api_version = 'auto',
        auto_remove = True,
        docker_url = 'tcp://docker-proxy:2375',
        network_mode = 'container:spark-master',
        tty = True,
        xcom_all = False,
        mount_tmp_dir = False ,
        environment = {
            'SPARK_APPLICATION_ARGS': '{{task_instance.xcom_pull(task_ids = "stock_prices")}}'
        } 

    )

    is_api_availble() >> get_stock_prices >> stock_prices >> format_prices
    



stock_market()

