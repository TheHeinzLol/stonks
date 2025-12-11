import json
import logging
import os
import re
import requests

from datetime import datetime, timedelta, timezone, UTC
from pathlib import Path
from typing import Union, Tuple
from urllib.parse import urlencode

# These are dummy. They were changed long before this notebook got near the internet, so they are safe to show. 
credentials = {
        "key": "PKCTYL4MO5SA2QEIZL2TDEJRTA",
        "secret_key": "DKL2eLVYFcxKzMJtDYbuG3zppWjpwHzsTS1DtHVwc9Cz",
        "finnhub": "d4ofku9r01quuso9dtkgd4ofku9r01quuso9dtl0"
    }
# Used in:
    #get_most_active_stocks
    #get_top_movers
    #get_historical_bars
headers_alpaca = {
    "accept": "application/json",
    "APCA-API-KEY-ID": credentials["key"],
    "APCA-API-SECRET-KEY": credentials["secret_key"]
}
def get_tickers() -> None:
    # get S&P 500 tickers
    # no arguments, returns nothing, creates json list with tickers
    from bs4 import BeautifulSoup
    
    response = requests.get('https://stockanalysis.com/list/sp-500-stocks/')
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all <td> elements with the specific class
    td_elements = soup.find_all('td', class_='sym svelte-1ro3niy')
    
    # Extract text from <a> tags inside those <td> elements
    tickers = []
    for td in td_elements:
        a_tag = td.find('a')
        if a_tag and a_tag.text.strip():
            tickers.append(a_tag.text.strip())
            
    # Write those into json file
    with open("tickers.json", "w") as json_file:
        json.dump(tickers, json_file)


# get top active stocks
def get_most_active_stocks(by: str = "volume", top: int = 100) -> str:
    """Calls API and returns pretty formated json string of most active stocks at the moment of last update. 
    Args:
        by: either 'volume' or 'trades'
        top: a number between 1 and 100"""
    
    url = f"https://data.alpaca.markets/v1beta1/screener/stocks/most-actives?by={by}&top={top}"
    response = requests.get(url, headers=headers_alpaca)
    
    return response.text


# get top market movers
def get_top_movers(top: int = 100) -> str:
    """Calls API and returns pretty formated json string of top market movers, both gainers and losers, at the moment of last update. 
    Unfortunately, it doesn't filter out penny stocks, so we can't look at stocks worth, say, $5 and more.
    Args:
        top: a number between 1 and 50. How many of each gainers and losers to return."""
    
    url = f"https://data.alpaca.markets/v1beta1/screener/stocks/movers?top={top}"
    response = requests.get(url, headers=headers_alpaca)
    
    return response.text

# Used in validate_arguments
def validate_time_frame(time_frame: str) -> Union[bool, str]:
    """
    Validate if time_frame satisfies the aggregation format:
    - [1-59]Min or [1-59]T for minutes
    - [1-23]Hour or [1-23]H for hours  
    - 1Day or 1D for days
    - 1Week or 1W for weeks
    - [1,2,3,4,6,12]Month or [1,2,3,4,6,12]M for months
    """
    # ^(\d+) - Capture digits at start
    # (Min|T|Hour|H|Day|D|Week|W|Month|M)$ - Specific suffixes
    match = re.match(r'^(\d+)(Min|T|Hour|H|Day|D|Week|W|Month|M)$', str(time_frame))
    
    if not match:
        return False, "Invalid format. Must be: [number][unit]"
    
    value = int(match.group(1))
    unit = match.group(2)
    
    # Validate based on unit
    if unit in ['Min', 'T']:
        if not (1 <= value <= 59):
            return False, "Minutes must be between 1-59"
            
    elif unit in ['Hour', 'H']:
        if not (1 <= value <= 23):
            return False, "Hours must be between 1-23"
            
    elif unit in ['Day', 'D']:
        if value != 1:
            return False, "Days must be exactly 1"
            
    elif unit in ['Week', 'W']:
        if value != 1:
            return False, "Weeks must be exactly 1"
            
    elif unit in ['Month', 'M']:
        valid_months = [1, 2, 3, 4, 6, 12]
        if value not in valid_months:
            return False, f"Months must be one of {valid_months}"
    
    return True, None

# Used in validate_arguments
def validate_datetime(date_string: str) -> Union[str, str]:
    logging.basicConfig(filename='datetime.log', level=logging.ERROR)
    
    if not isinstance(date_string, str):
        logging.info((f"date_string is of type '{type(date_string)}'"))
        return None, "Input must be a string"
    
    # Pattern for YYYY-MM-DD
    yyyy_mm_dd_pattern = r'^\d{4}-\d{2}-\d{2}$'
    # Pattern for YYYY-MM-DDThh:mm:ss (with optional timezone)
    datetime_pattern = r'^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}'
    
    date_string = date_string.strip()
    try:
        if re.match(yyyy_mm_dd_pattern, date_string):
            logging.info(("yyyy_mm_dd matched"))
            date_string = str(datetime.strptime(date_string, "%Y-%m-%d").isoformat())
            logging.info((f"Before: {date_string}\nAfter: {date_string}\n-------------------------------"))
            return date_string, None
        elif re.match(datetime_pattern, date_string):
            logging.info(("datetime matched"))
            # Replace space with T
            if ' ' in date_string:
                date_string = date_string.replace(' ', 'T')
            # Handle different timezone cases
            if date_string.upper().endswith('Z'):
                # Already has Z timezone
                date_string = str(datetime.fromisoformat(date_string.replace('Z', '+00:00')).isoformat())
                logging.info((f"Before: {date_string}\nAfter: {date_string}\n-------------------------------"))
                return date_string, None
            elif '+' in date_string or '-' in date_string[10:]:
                # Has timezone offset
                date_string = str(datetime.fromisoformat(date_string).isoformat())
                logging.info((f"Before: {date_string}\nAfter: {date_string}\n-------------------------------"))
                return date_string, None
            else:
                date_string = date_string[:19]
                date_string = str(datetime.fromisoformat(date_string).replace(tzinfo=timezone.utc).isoformat())
                logging.info((f"Before: {date_string}\nAfter: {date_string}\n-------------------------------"))
                return date_string, None
        else:
           return date_string, ("Unsupported date format. Use YYYY-MM-DD, YYYY-MM-DDThh:mm:ss or ime expressed in RFC-3339 format")
            
    except ValueError as e:
        return date_string, (f"Invalid date: {str(e)}")
    except Exception as e:
        return date_string, (f"Error processing date: {str(e)}")


# Used in get_historical_bars
def validate_arguments(
    tickers_to_search: list[str],
    date_start: str,
    date_end: str,
    time_frame: str = "1D",
    limit: int = 10000,) -> Tuple[bool, list[str]]:
    """ 
        This function validate arguments before passing those into API call.
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
        Returns:
            Tuple of (is_valid, error_messages)
                """
        
    error_messages = []
    # Validate tickers list
    if len(tickers_to_search) < 1:
        error_messages.append("'tickers_to_search' must be a non-emplty list of tickers")
        
    # Validate limit
    if not (1 <= limit <= 10000):
        error_messages.append(f"'limit' must be between 1 and 10000. Your input: {limit}")
        
    # Validate timeframe
    is_time_frame_valid, error_message = validate_time_frame(time_frame)
    if not is_time_frame_valid:
        error_messages.append(f"{error_message}. Your input: {time_frame}")
        
    # Validate dates
    dates_legit = True
    for date in [date_start, date_end]:
        date, error_message = validate_datetime(date)
        if error_message is not None:
            error_messages.append(f"{error_message}. Your input: {date}")
            dates_legit = False
            
    # If dates are valid, check if order is valid, too. 
    if dates_legit == True:
        if datetime.fromisoformat(date_start) > datetime.fromisoformat(date_end):
            error_messages.append(f"date_start should be an earlier date than date_end. Your date_start: {date_start} and date_end: {date_end}")

    return len(error_messages) == 0, error_messages


# Used in get_historical_bars
def write_into_json_file(data: str,
                         dates: Tuple[str, str],
                         i: int = 0,
                         path: Path = Path.cwd()) -> None:
    #make a folder to save to
    save_dir = Path.cwd()/ "data" 
    os.makedirs(save_dir, exist_ok=True)
    
    if 'bars' not in data:
        file_name = f"{dates[0]}.json"
        path = save_dir / file_name
        with open(path, "w") as json_file:
            json.dump(data, json_file)
        return f"json with error saved at {path}"
    first_ticker = list(data['bars'].keys())[0]
    first_date = data['bars'][first_ticker][0]['t']
    last_ticker = list(data['bars'].keys())[-1]
    last_date = data['bars'][first_ticker][-1]['t']
    file_name = f"{first_ticker}_{first_date}-{last_ticker}_{last_date}.json"
    path = Path.cwd().parent / "data" / file_name
    with open(path, "w") as json_file:
        json.dump(data, json_file)
    return f"json with tickers saved at {path}"

def get_historical_bars(
    tickers_to_search: list[str],
    time_frame: str = "1D",
    date_start: str = None,
    date_end: str = None,
    limit: int = 10000,) -> bool:
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
        Returns:
            bool. 
                """
        
    # Set defaults properly (evaluated at call time)
    if date_start is None:
        date_start = (datetime.now().date() - timedelta(days=1)).isoformat()
    if date_end is None:
        date_end = datetime.now().date().isoformat()
        
    # Validate arguments
    is_valid, error_messages = validate_arguments(tickers_to_search, date_start, date_end, time_frame, limit)
    if not is_valid:
        for error in error_messages:
            print(error)
        return False
    
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
        write_into_json_file(parsed, (date_start, date_end))
        params['page_token'] = parsed['next_page_token']
    return True
