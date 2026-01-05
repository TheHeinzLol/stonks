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
date_from = pendulum.today().date().subtract(days=7).isoformat()
date_to = pendulum.today().date().isoformat()
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
if ticker_list.index(latest_ticker)+30 < ticker_list:
    #get news for ticker_list[latest_ticker_index+1:]
    pass
else:
    for ticker in ticker_list[latest_ticker_index+1:latest_ticker_index+31]:
        requestResponse = requests.get(f"https://finnhub.io/api/v1/company-news?token={credentials['finnhub']}&symbol={ticker}&from={date_from}&to=2025-11-03")
#save to bucket