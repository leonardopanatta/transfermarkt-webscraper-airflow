import pandas as pd

from airflow.hooks.S3_hook import S3Hook

class TransfersDWETL():

    def __init__(self):
        self.s3 = S3Hook('aws_s3_airflow_user')
        self.s3_transfers_csv_file = self.s3.get_key(key="latest_transfers/latest_transfers_brazil_2021_06_07.csv", bucket_name="datalake-transfermarkt-sa-east-1")
        self.s3_bucket_name = "datalake-transfermarkt-sa-east-1"
        self.s3_bucket_folder = "latest_transfers/"
        self.s3_file_name_prefix = "transfers_"

    def clean_currency(x):
        """ If the value is a string, then remove currency symbol and delimiters
        otherwise, the value is numeric and can be converted
        """
        if isinstance(x, str):
            return(x.replace('€', '').replace(',', ''))
        return(x)

    def etl_clubs(self):
        df = pd.read_csv(self.s3_transfers_csv_file.get()["Body"])
        print(df["transfer_fee"])

    #def etl_seasons(self):

    #def etl_players(self):