import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from utils_alpaca import get_historical_bars

with DAG(
    dag_id="get_historical_data_tickers",
    catchup=False,
    tags="stonks",
    schedule=None,
    start_date=pendulum.today().subtract(days=1)
) as dag:
    
    t1 = PythonOperator(
        task_id="get_and_save_tickers",
        python_callable=get_historical_bars,
        op_kwargs={"tickers_to_search":["AAPL", "NVDA"],
            "time_frame": "1Min",
            "date_start": "{{ds}}",
            "date_end": "{{ds}}",
            "limit":10000},
        dag=dag)