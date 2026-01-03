# realtime taskflow
import io
import json
import os
import pendulum 
import requests

from airflow.sdk import dag, task
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pathlib import Path
from tasks_alpaca import get_minio_credentials, create_bucket
from typing import Union, Tuple, Optional
from urllib.parse import urlencode

with open(Path("/config/credentials.json", "r") as credentials_json:
    credentials = json.load(credentials_json)
    
headers_alpaca = {
    "accept": "application/json",
    "APCA-API-KEY-ID": credentials["alpaca_key"],
    "APCA-API-SECRET-KEY": credentials["alpaca_secret_key"]
}
tickers_path = Path("config/tickers.json")

# minio conn
login_minio, password_minio = get_minio_credentials()
host = "minio"
client = Minio(
    endpoint=f"{host}:9001",
    access_key=login_minio,
    secret_key=password_minio,
    secure=False
    )


@dag(
    dag_id="get_latest_bars_taskflow",
    catchup=False,
    tags="stonks",
    start_date=pendulum.today(),
    schedule=pendulum.duration(minutes=1))
def get_realtime_tickers():
    @task
    def get_tickers(path_to_json: Optional[Path] = None) -> list:
        if path_to_json is None:
            return ['aapl', 'nvda']
        with open(path_to_json, "r") as tickers_file:
            tickers = json.load(tickers_file)
        return tickers
    @task
    def get_latest_bars(tickers_to_search):
        print(tickers_to_search)
        params = {
            'symbols': ",".join(tickers_to_search),
            }
        url = f"https://data.alpaca.markets/v2/stocks/bars/latest?{urlencode(params)}"
        response = requests.get(url, headers=headers_alpaca)
        return response.text
    @task    
    def upload_to_bucket(source_file: Union[str, Path, bytes], bucket_name: str, client: Minio, destination_file: str=None) -> None:
        # Make the bucket if it doesn't exist.
        create_bucket(bucket_name, client)
        # Encode str to bytes
        file_encoded = source_file.encode("utf-8")
        file_to_upload = io.BytesIO(file_encoded)
        # Upload file
        client.put_object(
            data=file_to_upload,
            bucket_name=bucket_name,
            object_name=destination_file,
            length=len(file_encoded)
        )
        print("Successfully uploaded object", destination_file, "to bucket", bucket_name)

    tickers = get_tickers(tickers_path)
    bars = get_latest_bars(tickers)
    upload_to_bucket(source_file=bars, bucket_name="bronze.latest", client=client, destination_file="{{dag_run.start_date}}")
get_realtime_tickers()