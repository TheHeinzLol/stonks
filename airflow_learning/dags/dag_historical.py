from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pathlib import Path

from tasks_alpaca import get_bars, get_minio_credentials, get_tickers, upload_to_bucket

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
    dag_id="get_historical_tickers",
    catchup=True,
    tags="stonks",
    schedule=None
) as dag:
    tickers = PythonOperator(
        task_id="get_tickers",
        python_callable=tasks_alpaca.get_tickers,
        op_kwargs={"path_to_json": Path().cwd() / "data" / "tickers.json"}
        dag=dag)
    bars = PythonOperator(
        task_id="get_bars",
        python_callable=tasks_alpaca.get_bars,
        op_kwargs={
            "tickers_to_search": tickers,
            "time_frame":"1Min",
            "date_start": "{{ds}}",
            "date_end": "{{ds}}"},
        dag=dag)
    save_response = PythonOperator(
        task_id="save_response",
        python_callable=tasks_alpaca.upload_to_bucket,
        op_kwargs={bucket_name, file_name},
        dag=dag)# these are placeholders