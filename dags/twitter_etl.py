# Libraries
import pip

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install('tweepy')
install('s3fs')
install('psycopg2')

import pandas as pd
from datetime import datetime
import os.path
import re
import csv
import psycopg2 as pg
from pathlib import Path

import tweepy
import s3fs

# csv_path = Path("/opt/airflow/data/tweets.csv")

# access key dari Twitter API
a_key = 'qUnv0UK4ePe0qgMYZp0jV72YI'
a_secret = 'ghN87rNJQIwR7c9cBRHROA4gwNDRN6aBJWL1u7JDMgojNwr0Hj'
c_key = '1448947389847404550-3sj9PK07d1NWj8OStIuXWK4M8W1HCa'
c_secret = 'nISY2fPiEzI5gNs1DyxyaGTbH2dqFrDQNGH9wJRHr33hF'

def extract_tweet():
    # autentikasi ke twitter
    auth = tweepy.OAuthHandler(a_key, a_secret)
    auth.set_access_token(c_key, c_secret)

    # Buat objek API
    api = tweepy.API(auth)
    tweets = api.search_tweets(
                            q="ganjar",             # keyword yang dicari
                            lang="id",              # bahasa yang dicari (Indonesia)
                            result_type="recent",   # tweet yang diambil yang terkini
                            count=100                # max = 100 baris tweets
                        )
    # list untuk menampung data dari API
    list = []
    for tweet in tweets:
        search_data = {
                    # # scrape username, text, dan waktu dibuat saja agar tidak perlu cleaning berlebihan
                    'user': tweet.user.screen_name,
                    'text': tweet.text
                    }
        list.append(search_data)
    return list


def clean_tweet(tweets):
    """bersihkan tweets dari simbol-simbol yang tidak diperlukan, seperti tag dengan @, hashtag #, link htttp
    Retweet (RT), username, dll"""
    tweets = re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", tweets)
    tweets = re.sub(r"USERNAME", "", tweets)
    tweets = re.sub(r"#[A-Za-z0-9]+", "", tweets)
    return tweets

def transform_tweet(**kwargs):
    # Xcoms to get the data from previous task
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='extract_tweet')
    df_tweets = pd.DataFrame(value)
    df_clean_tweets = df_tweets.dropna()
    df_clean_tweets = df_clean_tweets.drop_duplicates('text', keep='first')

    df_clean_tweets['text'] = df_clean_tweets['text'].apply(clean_tweet)

    try:
        # df_clean_tweets = df_clean_tweets.set_index("id")
        df_clean_tweets.to_csv('tweets.csv', index=False, header=True)
        return True
    except OSError as e:
        print(e)
        return False

def load_to_db():
    try:
        conn = pg.connect(
            "dbname='airflow' user='airflow' host='postgres' password='airflow'"
        )
    except Exception as error:
        print(error)

    #Checking database establishment
    cursor = conn.cursor()
    cursor.execute("""
    create table if not exists tweets_tbl(
        users varchar,
        texts varchar
    );
    """)

    conn.commit()

    with open('tweets.csv', 'r') as data:
        reader = csv.reader(data)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO tweets_tbl VALUES (%s, %s)",
                row
            )
    conn.commit()

# # testing
# load_to_db()
