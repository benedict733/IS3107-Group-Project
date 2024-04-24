import datetime
from airflow.decorators import dag, task
import kaggle
import zipfile
import pandas as pd
import requests
import json
import mysql.connector
from sqlalchemy import create_engine
from airflow.providers.mysql.hooks.mysql import MySqlHook
import urllib
import bs4
from bs4 import BeautifulSoup
from urllib.request import urlopen
import requests,json,os,sys,time,re
import numpy as np


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2024,4,1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
}

def get_reviews(game_id):
    review = requests.get(f'https://store.steampowered.com/appreviews/{game_id}',params={'language' : 'english','json':1}).json()['reviews']
    for i in review:
        i['author'] = i['author']['steamid']
        i['app_id'] = game_id
    return review

def map_os(row):
    if row['win'] and row['linux'] and row['mac']:
        return 'wml'
    elif row['win'] and row['linux']:
        return 'wl'
    elif row['win'] and row['mac']:
        return 'wm'
    elif row['mac'] and row['linux']:
        return 'ml'
    elif row['win']:
        return 'w'
    elif row['mac']:
        return 'm'
    elif row['linux']:
        return 'l'

@dag(dag_id = 'IS3107_Proj',schedule=None, default_args=default_args, tags=['project'], catchup=False)
def dag():

    @task
    def extract_games_list():
        kaggle.api.dataset_download_file(dataset='antonkozyriev/game-recommendations-on-steam',file_name = 'games.csv')
        with zipfile.ZipFile('./games.csv.zip', 'r') as zip_ref:
            zip_ref.extractall('./')

        games = pd.read_csv('./games.csv')

        return games
    
    @task 
    def extract_game_descriptions(data):
        data['app_id'] = data['app_id'].apply(str)
        data['links'] = ('https://store.steampowered.com/app/'+ data['app_id']).astype(str)
        Game_description = pd.Series([]) 
    
        for i in range(500):
            url = data['links'][i]
            url_contents = urllib.request.urlopen(url).read()
            soup = bs4.BeautifulSoup(url_contents, 'html.parser')
            div = soup.find("div", {"id": "game_area_description"})
            Game_description[i]=div

        soup = bs4.BeautifulSoup(url_contents, 'html.parser')
        div = soup.find("div", {"id": "game_area_description"})

        content = str(div)
        data['Game_description'] = Game_description
        data['Game_description'] = data['Game_description'].apply(str)

        # Drop columns containing NaN values
        data.replace('nan', np.nan, inplace=True)
        data = data.dropna(subset=['Game_description'])

        for i in range(0,100):
            data['Game_description'][i] = re.sub('<.*?>', ' ', data['Game_description'][i])
            data['Game_description'][i] = re.sub('\\n.*\\n', ' ', data['Game_description'][i])
            data['Game_description'][i] = data['Game_description'][i].replace("\t","")
            data['Game_description'][i] = data['Game_description'][i].translate(data['Game_description'][i].maketrans(' ',' ',string.punctuation))

        return data

    @task
    def extract_reviews(games):
        reviews = []
        skip = []
        
        try:
            for i in games['app_id'].unique():
                reviews += get_reviews(i)
        except json.JSONDecodeError:
            skip.append(i)
            pass

        reviews = pd.DataFrame(reviews)

        return reviews

    
    @task
    def merge_dataset(games, reviews):
        # merge games and reviews
        result = reviews.merge(games,on='app_id',how = 'left')

        return result
    
    @task
    def transform_data(df):
        df['timestamp_created'] = df['timestamp_created'].apply(lambda x: dt.datetime.fromtimestamp(x))
        df['timestamp_updated'] = df['timestamp_updated'].apply(lambda x: dt.datetime.fromtimestamp(x))
        df['os'] = df.apply(map_os,axis=1)
        df = df.dropna(subset=['os'])
        df = df[['recommendationid', 'author', 'review', 'timestamp_created', 'timestamp_updated', 'voted_up', 'votes_up',
       'weighted_vote_score', 'app_id', 'title', 'date_release', 'rating', 'positive_ratio', 'user_reviews', 'price_final', 'price_original',
       'os']]

        return df

    @task
    def load(games, reviews, steam):

        connection = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='iloveSCnb2018!'
        )

        # create schema
        cursor = connection.cursor()
        schema_name = 'batchdata'
        cursor.execute(f'CREATE SCHEMA {schema_name}')
        connection.commit()
        cursor.close()
        connection.close()

        # connect to database
        batchdata = create_engine('mysql://root:iloveSCnb2018!@localhost:3306/batchdata', echo=False)

        # push into database
        games.to_sql(name='games', con=batchdata, if_exists='replace')
        reviews.to_sql(name='reviews', con=batchdata, if_exists='replace')
        steam.to_sql(name='steam', con=batchdata, if_exists='replace') 



    games = extract_games_list()
    reviews = extract_reviews(games)
    updated_games = extract_game_descriptions(games)
    merged_df = merge_dataset(updated_games, reviews)
    transformed_df = transform_data(merged_df)
    load(updated_games, reviews, transformed_df)

dag = dag()