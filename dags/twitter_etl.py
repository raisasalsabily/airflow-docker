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
install('instaloader')

import instaloader
import tweepy
import s3fs

from datetime import datetime
from pathlib import Path
import pandas as pd
import csv
import os.path
import re
import psycopg2 as pg

""" CATATAN: 
-   Pada file twitter_etl.py ini terdiri atas ETL untuk API dari twitter DAN Instagram,
    meskipun nama file-nya twitter_etl.py
-   Table database tweets_tbl juga berisi data dari twitter DAN Instagram.
"""

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

    L = instaloader.Instaloader()

    # Login dengan akun instagram
    L.login("USERNAME","PASSWORD")

    # Menentukan target akun instagram
    target_id='ganjar_pranowo'

    # ambil semua post dari akun target
    posts = instaloader.Profile.from_username(L.context, target_id).get_posts()

    # simpan data komentar dari post terbaru ke list
    i=0
    for post in posts:
        if i>100: # ambil 100 komentar terbaru
            break
        for comment in post.get_comments():
            search_data = {
                # scrape username dan text saja agar tidak perlu cleaning berlebihan
                'user': comment.owner.username,
                'text': comment.text
            }
            list.append(search_data)
            i+=1
            if i>100: # ambil 100 komentar terbaru
                break
    return list


def clean_tweet(tweets):
    """bersihkan tweets dari simbol-simbol yang tidak diperlukan, seperti tag dengan @, hashtag #, link htttp
    Retweet (RT), username, dll"""
    tweets = re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", tweets)
    tweets = re.sub(r"USERNAME", "", tweets)
    tweets = re.sub(r"#[A-Za-z0-9]+", "", tweets)
    return tweets

def transform_tweet(**kwargs):
    # import data dari task lain dengan xcom
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='extract_tweet')
    df_tweets = pd.DataFrame(value) # masukkan value ke dataframe
    # lakukan penghilangan simbol-simbol unik
    df_fin_tweets['text'] = df_tweets['text'].apply(clean_tweet)
    # load ke csv
    try:
        df_fin_tweets.to_csv('social.csv', index=False, header=True)
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

    # buat tabel
    cursor = conn.cursor()
    cursor.execute("""
    create table if not exists tweets_tbl(
        users varchar,
        texts varchar
    );
    """)

    conn.commit()

    # load data dari csv ke postgresql
    with open('social.csv', 'r') as data:
        reader = csv.reader(data)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO tweets_tbl VALUES (%s, %s)",
                row
            )
    conn.commit()

