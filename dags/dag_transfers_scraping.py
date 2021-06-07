# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
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

    start_scraping = DummyOperator(task_id='start_scraping')

    countries_with_letter_a = DummyOperator(task_id='countries_with_letter_a')

    countries_with_letter_b = DummyOperator(task_id='countries_with_letter_b')

    countries_with_letter_e = DummyOperator(task_id='countries_with_letter_e')

    countries_with_letter_s = DummyOperator(task_id='countries_with_letter_s')

    countries_with_letter_u = DummyOperator(task_id='countries_with_letter_u')

    scrape_latest_transfers_from_afghanistan = PythonOperator(
        task_id="scrape_latest_transfers_from_afghanistan",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "afghanistan"
        }
    )

    scrape_latest_transfers_from_albania = PythonOperator(
        task_id="scrape_latest_transfers_from_albania",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "albania"
        }
    )

    scrape_latest_transfers_from_andorra = PythonOperator(
        task_id="scrape_latest_transfers_from_andorra",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "andorra"
        }
    )

    scrape_latest_transfers_from_angola = PythonOperator(
        task_id="scrape_latest_transfers_from_angola",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "angola"
        }
    )

    scrape_latest_transfers_from_brazil = PythonOperator(
        task_id="scrape_latest_transfers_from_brazil",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "brazil"
        }
    )

    scrape_latest_transfers_from_england = PythonOperator(
        task_id="scrape_latest_transfers_from_england",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "england"
        }
    )

    scrape_latest_transfers_from_saudi_arabia = PythonOperator(
        task_id="scrape_latest_transfers_from_saudi_arabia",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "saudi_arabia"
        }
    )

    scrape_latest_transfers_from_south_africa = PythonOperator(
        task_id="scrape_latest_transfers_from_south_africa",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "south_africa"
        }
    )

    scrape_latest_transfers_from_usa = PythonOperator(
        task_id="scrape_latest_transfers_from_usa",
        python_callable=transfers_scraping_etl.scrape_latest_transfers,
        op_kwargs={
            "clubs_from_country": "usa"
        }
    )

    load_transfers_CSV_to_S3 = PythonOperator(
        task_id="load_transfers_CSV_to_S3",
        python_callable=transfers_scraping_etl.load_CSV_on_S3,
        op_kwargs={
            "bucket_name": "datalake-transfermarkt-sa-east-1"
        }
    )

    delete_local_CSV_files = PythonOperator(
        task_id="delete_local_CSV_files",
        python_callable=transfers_scraping_etl.delete_local_CSV
    )

    join_scraping = DummyOperator(task_id='join_scraping', trigger_rule=TriggerRule.ALL_DONE)

    end_pipeline = DummyOperator(task_id="end_pipeline")

    start_pipeline >> start_scraping
    start_scraping >> [countries_with_letter_a, countries_with_letter_b, countries_with_letter_e, countries_with_letter_s, countries_with_letter_u]
    countries_with_letter_a >> [scrape_latest_transfers_from_afghanistan, scrape_latest_transfers_from_albania, scrape_latest_transfers_from_andorra, scrape_latest_transfers_from_angola] >> join_scraping
    countries_with_letter_b >> scrape_latest_transfers_from_brazil >> join_scraping
    countries_with_letter_e >> scrape_latest_transfers_from_england >> join_scraping
    countries_with_letter_s >> [scrape_latest_transfers_from_saudi_arabia, scrape_latest_transfers_from_south_africa] >> join_scraping
    countries_with_letter_u >> scrape_latest_transfers_from_usa >> join_scraping
    join_scraping >> load_transfers_CSV_to_S3 >> delete_local_CSV_files >> end_pipeline
