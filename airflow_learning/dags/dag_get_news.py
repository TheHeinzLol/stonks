import tasks_alpaca

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task
from pathlib import Path

login_minio, password_minio = tasks_alpaca.get_minio_credentials()
host = "localhost"
client = Minio(
    endpoint=f"{host}:9000",
    access_key=login_minio,
    secret_key=password_minio,
    secure=False
    )

bucket_name="bronze.news"
#check if bucket exists
tasks_alpaca.create_bucket(bucket_name, client)
#get pulled news today
prefix = pendulum.today().in_timezone("UTC").date().isoformat()+"/"
news_list = list_objects(bucket_name, prefix=prefix)
#check latest pulled ticker
if len(news_list)>0:
    latest_ticker = sorted(news_list, reverse=False)[-1]
#pull 30 tickers from latest for today
    
#save to bucket