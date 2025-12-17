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

credentials = {
        "key": "PKCTYL4MO5SA2QEIZL2TDEJRTA",
        "secret_key": "DKL2eLVYFcxKzMJtDYbuG3zppWjpwHzsTS1DtHVwc9Cz"
    }
headers_alpaca = {
    "accept": "application/json",
    "APCA-API-KEY-ID": credentials["key"],
    "APCA-API-SECRET-KEY": credentials["secret_key"]
}

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
    dag_id="minio_check",
    catchup=False,
    tags="stonks",
    start_date=pendulum.now().set(second=0, microsecond=0).in_timezone("UTC").subtract(minutes=15),
    schedule=pendulum.duration(minutes=1))
def check_minio():
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

    upload_to_bucket(source_file="asss", bucket_name="airflow.learn", client=client, destination_file="{{dag_run.start_date}}")
check_minio()