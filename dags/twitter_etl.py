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

csv_path = Path("/opt/airflow/data/tweets.csv")

# access key dari Twitter API
a_key = 'cyXcjxUc1ZlLD2v1DjaJrLiVI'  
a_secret = 'pD3mk041ROBiSSwt2C3kytR9eF1zYrX1vBD8whpDln5Zq4y1jX' 
c_key = '1279253779540410373-8ODCqefNcIvSFXSrwKH7DPhtQTda3i'  
c_secret = 'nVGU8WyEdGCQ2gU1qQ57NXxLpYQts4Emt1UPM6GVlYaav' 
 
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
            "dbname='airflow' user='postgres' host='localhost' password='raisa10112001'"
        )
    except Exception as error:
        print(error)

    #Checking database establishment
    cursor = conn.cursor()
    cursor.execute("""
    create table if not exists tweets_tbl(
        users varchar(256),
        texts varchar(256)
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

load_to_db()

#     new_tweets_df = pd.DataFrame(list) # # masukkan ke dataframe 
 
 
#     # # import ke csv - cek apakah file sudah ada/belum 
#     file_exists = os.path.exists('tweets.txt') 
#     if(not file_exists): 
#         # # buat file baru 
#         new_tweets_df.to_csv('tweets.csv', mode='a', index=False) 
#     else: 
#         # # append ke file lama 
#         new_tweets_df.to_csv('tweets.csv', mode='a', index=False, header=False) 
 
 
# # # untuk testing 
# run_twitter_etl()