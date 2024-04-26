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


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2024,4,1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
}

def get_reviews(game_id):
    review = requests.get(f'https://store.steampowered.com/appreviews/{game_id}',params={'day_range':'1','language' : 'english','json':1}).json()['reviews']
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

@dag(dag_id = 'IS3107_Stream',schedule=None, default_args=default_args, tags=['project'], catchup=False)
def dag():

    @task
    def extract_games_list():
        # connect to database
        batchdata = MySqlHook(mysql_conn_id = 'batchdata')
        # extract games
        str_sql = '''
            SELECT * FROM batchdata.games;
        '''
        games = batchdata.get_pandas_df(sql=str_sql)
        return games

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
       'os', 'Game_description']]

        return df
    
    @task 
    def load(reviews, steam):

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
        reviews.to_sql(name='reviews', con=batchdata, if_exists='append')
        steam.to_sql(name='steam', con=batchdata, if_exists='append') 
    
    games = extract_games_list()
    reviews = extract_reviews(games)
    merged_df = merge_dataset(games, reviews)
    transformed_df = transform_data(merged_df)
    load(reviews, transformed_df)

dag = dag()
