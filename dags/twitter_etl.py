import pip

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install('tweepy')      

import pandas as pd 
from datetime import datetime

import tweepy
# import s3fs 

def run_twitter_etl():

    # access key dari twitter
    a_key = 'cyXcjxUc1ZlLD2v1DjaJrLiVI' 
    a_secret = 'pD3mk041ROBiSSwt2C3kytR9eF1zYrX1vBD8whpDln5Zq4y1jX'
    c_key = '1279253779540410373-8ODCqefNcIvSFXSrwKH7DPhtQTda3i' 
    c_secret = 'nVGU8WyEdGCQ2gU1qQ57NXxLpYQts4Emt1UPM6GVlYaav'

    # autentikasi ke twitter
    auth = tweepy.OAuthHandler(a_key, a_secret)   
    auth.set_access_token(c_key, c_secret) 

    # Buat objek API 
    api = tweepy.API(auth)
    tweets = api.search_tweets(
                            q="ganjar",             # keyword yang dicari
                            lang="id",              # bahasa yang dicari (Indonesia)
                            result_type="recent",   # tweet yang diambil yang terkini
                            count=100               # max = 100 baris tweets
                        )

    list = []
    for tweet in tweets:
        search_data = {
                    'text': tweet.text, # cukup scrape text dari tweet agar tidak perlu cleaning berlebihan
                    }
        list.append(search_data)

    new_tweets_df = pd.DataFrame(list) # masukkan ke dataframe

    ## untuk load pertama
    new_tweets_df.to_csv('tweets.csv', index=False)
    ## untuk append data frame ke CSV file yang sudah ada
    # new_tweets_df.to_csv('tweets.csv', mode='a', index=False, header=False)