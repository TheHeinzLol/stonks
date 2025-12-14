from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pathlib import Path

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
        python_callable=tasks_alpaca.save_response,
        op_kwargs={bucket_name, file_name},
        dag=dag)# these are placeholders