# -*- coding: utf-8 -*-
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 5, 28, 16, 10)
}

def new_file_detection():
    print("A new file has arrived in s3 bucket")

with DAG(
    dag_id="dag_transfers_load_dw",
    default_args=default_args,
    schedule_interval="@daily",
    tags=['transfermarkt']
) as dag:

    #wait_for_CSV_load_onto_S3 = S3PrefixSensor(
    #    task_id="wait_for_CSV_load_onto_S3",
    #    aws_conn_id = "aws_s3_airflow_user",
    #    bucket_name="datalake-transfermarkt-sa-east-1",
    #    prefix="transfers/"
    #)

    wait_for_CSV_load_onto_S3 = S3KeySensor(
        task_id="wait_for_CSV_load_onto_S3",
        bucket_key="s3://datalake-transfermarkt-sa-east-1/transfers/transfers_2021_05_28.csv",
        aws_conn_id = "aws_s3_airflow_user"
    )

    print_message = PythonOperator(
        task_id='print_message',
        python_callable=new_file_detection
    )

    end_pipeline = DummyOperator(task_id="end_pipeline")

    wait_for_CSV_load_onto_S3 >> print_message >> end_pipeline
