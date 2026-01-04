
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task
from pathlib import Path
from pprint import pprint

import pendulum

def print_date():
    print("---------------start printing current------------")
    for item in Path.cwd().iterdir():
        print(item)
    print("---------------start printing parent------------")
    for item in Path.cwd().parent.iterdir():
        print(item)
    return "-------------printed---------------"
    
with DAG(
    dag_id="my_test_dag",
    catchup=False,
    tags="stonks",
    schedule=None
) as dag:
    
    t1 = PythonOperator(
        task_id="print_date",
        python_callable=print_date,
        dag=dag,)