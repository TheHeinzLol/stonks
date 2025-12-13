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