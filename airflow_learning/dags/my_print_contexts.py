
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task
from pathlib import Path
from pprint import pprint

import pendulum

def print_date():
    print("---------------start printing------------")
    for item in Path.cwd().iterdir():
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