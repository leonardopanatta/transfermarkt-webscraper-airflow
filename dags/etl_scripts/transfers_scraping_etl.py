# -*- coding: utf-8 -*-
import glob
import os
import pandas as pd
import requests

from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

from airflow.hooks.S3_hook import S3Hook

class TransfersScrapingETL():

    def __init__(self):
        self.local_data_path = "./dags/data/"
        self.s3 = S3Hook('aws_s3_airflow_user')
        self.s3_bucket_name = "datalake-transfermarkt-sa-east-1"
        self.s3_bucket_folder = "transfers/"
        self.s3_file_name_prefix = "transfers_"

    def scrape_transfers(self):
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

        page = "https://www.transfermarkt.co.uk/transfers/transferrekorde/statistik?saison_id=2019&land_id=0&ausrichtung=&spielerposition_id=&altersklasse=&leihe=&w_s=&plus=1"
        pageTree = requests.get(page, headers=headers)
        pageSoup = BeautifulSoup(pageTree.content, 'html.parser')

        players_names = pageSoup.select("table.items > tbody> tr > td:nth-of-type(2) > table > tr:nth-of-type(1) > td:nth-of-type(2) > a")
        players_positions = pageSoup.select("table.items > tbody > tr > td:nth-of-type(2) > table > tr:nth-of-type(2) > td")
        players_ages = pageSoup.select("table.items > tbody > tr > td:nth-of-type(3)")
        market_values_at_time = pageSoup.select("table.items > tbody > tr > td:nth-of-type(4)")
        seasons = pageSoup.select("table.items > tbody > tr > td:nth-of-type(5) > a")
        players_nationalities = pageSoup.select("table.items > tbody > tr > td:nth-of-type(6) > img")

        clubs_left = pageSoup.select("table.items > tbody > tr > td:nth-of-type(7) > table > tr:nth-of-type(1) > td:nth-of-type(2) > a")
        clubs_left_nationalities = pageSoup.select("table.items > tbody > tr > td:nth-of-type(7) > table > tr:nth-of-type(2) > td > img")
        clubs_left_leagues = pageSoup.select("table.items > tbody > tr > td:nth-of-type(7) > table > tr:nth-of-type(2) > td > a")

        clubs_joined = pageSoup.select("table.items > tbody > tr > td:nth-of-type(8) > table > tr:nth-of-type(1) > td:nth-of-type(2) > a")
        clubs_joined_nationalities = pageSoup.select("table.items > tbody > tr > td:nth-of-type(8) > table > tr:nth-of-type(2) > td > img")
        clubs_joined_leagues = pageSoup.select("table.items > tbody > tr > td:nth-of-type(8) > table > tr:nth-of-type(2) > td > a")
            
        fees = pageSoup.select("table.items > tbody > tr > td:nth-of-type(9) > a")

        players_names_list = []
        players_positions_list = []
        players_ages_list = []
        market_values_at_time_list = []
        seasons_list = []
        players_nationalities_list = []
        clubs_left_list = []
        clubs_left_nationalities_list = []
        clubs_left_leagues_list = []
        clubs_joined_list = []
        clubs_joined_nationalities_list = []
        clubs_joined_leagues_list = []
        transfers_fees_list = []

        for i in range(len(players_names)):
            players_names_list.append(players_names[i].text)
            players_positions_list.append(players_positions[i].text)
            players_ages_list.append(players_ages[i].text)
            market_values_at_time_list.append(market_values_at_time[i].text)
            seasons_list.append(seasons[i].text)
            players_nationalities_list.append(players_nationalities[i].get("title"))
            clubs_left_list.append(clubs_left[i].text)
            clubs_left_nationalities_list.append(clubs_left_nationalities[i].get("title"))
            clubs_left_leagues_list.append(clubs_left_leagues[i].text)
            clubs_joined_list.append(clubs_joined[i].text)
            clubs_joined_nationalities_list.append(clubs_joined_nationalities[i].get("title"))
            clubs_joined_leagues_list.append(clubs_joined_leagues[i].text)
            transfers_fees_list.append(fees[i].text)

        df = pd.DataFrame({
            "player_name":players_names_list,
            "player_position":players_positions_list,
            "player_age":players_ages_list,
            "market_value_at_time":market_values_at_time_list,
            "season":seasons_list,
            "player_nationality":players_nationalities_list,
            "club_left":clubs_left_list,
            "club_left_nationality":clubs_left_nationalities_list,
            "club_left_league":clubs_left_leagues_list,
            "club_joined":clubs_joined_list,
            "club_joined_nationality":clubs_joined_nationalities_list,
            "club_joined_league":clubs_joined_leagues_list,
            "transfer_fee":transfers_fees_list
        })

        self.write_to_CSV(df)

    def write_to_CSV(self, df):
        output_dir = Path(self.local_data_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        file_name = self.s3_file_name_prefix + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".csv"
        file_path = self.local_data_path + file_name

        df.to_csv(file_path, index = False)

    def load_CSV_on_S3(self):
        file_path_pattern = self.local_data_path + "*.csv"

        for f in glob.glob(file_path_pattern):
            key = self.s3_bucket_folder + f.split('/')[-1]
            self.s3.load_file(filename=f, bucket_name=self.s3_bucket_name, replace=True, key=key)

    def delete_local_CSV(self, filepath='./dags/data/*.csv'):
        file_path_pattern = self.local_data_path + "*.csv"

        files = glob.glob(file_path_pattern)
        for f in files:
            os.remove(f)