import pendulum
import tasks_alpaca

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from minio import Minio
from pathlib import Path

# minio conn
login_minio, password_minio = tasks_alpaca.get_minio_credentials()
host = "localhost"
client = Minio(
    endpoint=f"{host}:9000",
    access_key=login_minio,
    secret_key=password_minio,
    secure=False
    )


with DAG(
    dag_id="get_latest_bars",
    catchup=False,
    tags="stonks",
    start_date=pendulum.now().set(second=0, microsecond=0).in_timezone("UTC").subtract(minutes=15),
    schedule=pendulum.duration(minutes=1)
) as dag:
    
    tickers = PythonOperator(
        task_id="get_ticker_list",
        python_callable=tasks_alpaca.get_tickers,
        op_kwargs={"path_to_json": Path().cwd() / "data" / "tickers.json"},
        dag=dag)
    bars = PythonOperator(
        task_id="get_latest_bars",
        python_callable=tasks_alpaca.get_latest_bars,
        op_kwargs={"tickers_to_search": tickers},
        dag=dag)
    save_data = PythonOperator(
        task_id="save_data_to_bronze_bucket",
        python_callable=tasks_alpaca.upload_to_bucket,
        op_kwargs={
            "source_file":bars,
            "bucket_name":"learning.bronze",
            "client": client,
            "destination_file":dag.start_date
        })
    