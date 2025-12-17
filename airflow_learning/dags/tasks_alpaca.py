import io
import os
import json

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pathlib import Path
from typing import Union, Tuple, Optional

credentials = {
        "key": "PKCTYL4MO5SA2QEIZL2TDEJRTA",
        "secret_key": "DKL2eLVYFcxKzMJtDYbuG3zppWjpwHzsTS1DtHVwc9Cz"
    }
headers_alpaca = {
    "accept": "application/json",
    "APCA-API-KEY-ID": credentials["key"],
    "APCA-API-SECRET-KEY": credentials["secret_key"]
}

def get_tickers(path_to_json: Optional[Path] = None) -> list:
    if path_to_json is None:
        return {"tickers_to_search": ['aapl', 'nvda']}
    with open(path_to_json, "r") as tickers_file:
        tickers = json.load(tickers_file)
    return {"tickers_to_search": tickers}


def get_minio_credentials(path_to_env_file: Union[str, Path] = "config/minio.env") -> str:
    """This function returns login and password for root user of minio server, getting those from 'minio.env' file.
    'minio.env' should have MINIO_ROOT_USER and MINIO_ROOT_PASSWORD variables. If there are no such variables, asks user to provide those via input.
    Args:
        path_to_env_file: either string or pathlib.Path object leading to minio.env file."""
    load_dotenv(path_to_env_file)
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
    if (MINIO_ROOT_USER and MINIO_ROOT_PASSWORD):
        return MINIO_ROOT_USER, MINIO_ROOT_PASSWORD 
    else:
        print(f"There are no MINIO_ROOT_USER and/or MINIO_ROOT_PASSWORD variables in {path_to_env_file}. Returning 'admin', 'password_admin'")
        return "admin", "admin_password"


def create_bucket(bucket_name: str, client: Minio) -> None:
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")


def upload_to_bucket(source_file: Union[str, Path, bytes], bucket_name: str, client: Minio, destination_file: str=None) -> None:
    # Make the bucket if it doesn't exist.
    create_bucket(bucket_name, client)
    # upload file
    try:
        client.put_object(
            bucket_name=bucket_name,
            object_name=destination_file,
            length=-1,
            file_path=source_file,
        )
        print("Successfully uploaded object", destination_file, "to bucket", bucket_name)
    except:
        print("Failed to upload file to bucket")


def get_latest_bars(tickers_to_search):
    print(tickers_to_search)
    params = {
        'symbols': ",".join(tickers_to_search),
        }
    url = f"https://data.alpaca.markets/v2/stocks/bars/latest?{urlencode(params)}"
    response = requests.get(url, headers=headers_alpaca)
    return response.text

    
def get_historical_bars(
    tickers_to_search: list[str],
    time_frame: str = "1D",
    date_start: str = None,
    date_end: str = None,
    limit: int = 10000,
    page_token: str = "") -> Tuple[str, bool]:
    """ 
        Saves json in file or prints errors made in arguments.
        Args:
            tickers_to_search:
                non-empty list of stock tickers.
            time_frame:
                [1-59]Min or [1-59]T, e.g. 5Min or 5T creates 5-minute aggregations.
                [1-23]Hour or [1-23]H, e.g. 12Hour or 12H creates 12-hour aggregations.
                1Day or 1D creates 1-day aggregations.
                1Week or 1W creates 1-week aggregations.
                [1,2,3,4,6,12]Month or [1,2,3,4,6,12]M, e.g. 3Month or 3M creates 3-month aggregations.
            date_start and date-end:
                string in YYYY-MM-DD or rfc-3339 format. date_start should be an earlier date than date_end.
            limit: 
                number between 1 and 10000.
            page_token:
                a string you get from 'next_page_token' field of response.
                If this is the first page of your call, then leave it as empty string.
                If there is no next page for your call, 'next_page_token' field will return None value. This behavior is expected.
        Returns:
            Tuple(bars, is_there_next_page) 
                """
        
    # # Validate arguments
    # is_valid, error_messages = validate_arguments(tickers_to_search, date_start, date_end, time_frame, limit)
    # if not is_valid:
    #     for error in error_messages:
    #         print(error)
    #     return False
    
    params = {
    'symbols': ",".join(tickers_to_search),
    'timeframe': time_frame, 
    'start': date_start,
    'end': date_end,
    'limit': limit,
    'adjustment': 'raw',
    'feed': 'sip', 
    'page_token': "",
    'sort': "asc"
    }
    while params['page_token'] is not None: # while there is next page
        url = f"https://data.alpaca.markets/v2/stocks/bars?{urlencode(params)}"
        response = requests.get(url, headers=headers_alpaca)
        parsed = json.loads(response.text)
        params['page_token'] = parsed['next_page_token']
    return True