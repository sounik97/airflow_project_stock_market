from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags = ['taskflow']

)

def task_flow():

    @task
    def task_a():
        print("Task A")
        return 42
    
    @task 
    def task_b(value):
        print("Task B")
        print(value)

    task_b(task_a())

task_flow()