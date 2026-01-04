from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pathlib import Path

from tasks_alpaca import get_bars, get_minio_credentials, get_tickers, upload_to_bucket

next_page_token=""

login_minio, password_minio = tasks_alpaca.get_minio_credentials()
host = "localhost"
client = Minio(
    endpoint=f"{host}:9000",
    access_key=login_minio,
    secret_key=password_minio,
    secure=False
    )

@dag(
    dag_id="get_historical_bars_today_taskflow",
    catchup=False,
    tags="stonks",
    start_date=pendulum.today(),
    schedule=None)
def get_historical_bars_today():
    @task
    def get_tickers(path_to_json: Optional[Path] = None) -> list:
        if path_to_json is None:
            return ['aapl', 'nvda']
        with open(path_to_json, "r") as tickers_file:
            tickers = json.load(tickers_file)
        return tickers
    @task
    def get_todays_bars(tickers_to_search,
                        next_page_token=next_page_token):
        tickers = tickers_to_search
        time_frame = "1Min"
        date_start = pendulum.today().in_timezone("UTC").date().isoformat()
        date_end = date_start
        response = get_bars(tickers_to_search=tickers,
                            time_frame=time_frame, 
                            date_start=date_start, 
                            date_end=date_end,
                            page_token=next_page_token)
        return response['bars'], response['next_page_token']
    @task    
    def upload_to_bucket(source_file: dict,
                         bucket_name: str,
                         client: Minio,
                         destination_file: str=None) -> None:
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
    tickers = get_tickers()
    while next_page_token is not None:
        bars, next_page_token = get_today_bars(tickers, next_page_token)
        upload_to_bucket(source_file=bars,
                         bucket_name="bronze.realtime",
                         client=client,
                         destnation_file=pendulum.now()\
                             .set(second=0, microsecond=0)\
                             .in_timezone("UTC")\
                             .subtract(minutes=15)\
                             .isoformat() # this is current utc time 15 minutes ago
get_historical_bars_today()