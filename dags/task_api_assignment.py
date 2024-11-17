from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import random
 
# def generate_random_number(**context):
#     ti = context['ti']
#     number = random.randint(1, 100)
#     ti.xcom_push(key='random_number', value=number)
#     print(f"Generated random number: {number}")
 
# def check_even_odd(**context):
#     ti = context['ti']
#     number = ti.xcom_pull(task_ids='generate_number', key='random_number')
#     result = "even" if number % 2 == 0 else "odd"
#     print(f"The number {number} is {result}.")
 
@dag(
    
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
)
def random_number_checker():
 
    @task
    def generate_task():
        number = random.randint(1, 100)
        return number

       
    @task
    def check_task(value):
        result = "even" if value % 2 == 0 else "odd"
        print(f"The number {value} is {result}.")
 
    check_task(generate_task())

random_number_checker()