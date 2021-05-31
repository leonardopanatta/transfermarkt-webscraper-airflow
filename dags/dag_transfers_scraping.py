# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from etl_scripts.transfers_scraping_etl import TransfersScrapingETL

transfers_scraping_etl = TransfersScrapingETL()

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="dag_transfers_scraping",
    default_args=default_args,
    schedule_interval="@daily",
    tags=['transfermarkt'],
) as dag:

    start_pipeline = DummyOperator(task_id="start_pipeline")

    scrape_transfers = PythonOperator(
        task_id='scrape_transfers',
        python_callable=transfers_scraping_etl.scrape_transfers
    )

    load_transfers_CSV_to_S3 = PythonOperator(
        task_id='load_transfers_CSV_to_S3',
        python_callable=transfers_scraping_etl.load_CSV_on_S3,
        op_kwargs={
            'bucket_name': 'datalake-transfermarkt-sa-east-1'
        }
    )

    delete_local_CSV_files = PythonOperator(
        task_id='delete_local_CSV_files',
        python_callable=transfers_scraping_etl.delete_local_CSV
    )

    end_pipeline = DummyOperator(task_id="end_pipeline")

    start_pipeline >> scrape_transfers >> load_transfers_CSV_to_S3 >> delete_local_CSV_files >> end_pipeline
