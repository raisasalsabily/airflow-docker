import pandas as pd 
from datetime import datetime
import json

import tweepy
import s3fs 

a_key = 'cyXcjxUc1ZlLD2v1DjaJrLiVI' 
a_secret = 'pD3mk041ROBiSSwt2C3kytR9eF1zYrX1vBD8whpDln5Zq4y1jX'
c_key = '1279253779540410373-8ODCqefNcIvSFXSrwKH7DPhtQTda3i' 
c_secret = 'nVGU8WyEdGCQ2gU1qQ57NXxLpYQts4Emt1UPM6GVlYaav'

# autentikasi ke twitter
auth = tweepy.OAuthHandler(a_key, a_secret)   
auth.set_access_token(c_key, c_secret) 

# Buat objek API 
api = tweepy.API(auth)
tweets = api.user_timeline(screen_name='@ugm_fess', 
                        count=15, # max=200
                        include_rts = False,
                        # penting untuk keep full_text, jika tidak maka hanya 140 kata pertama yang diekstrak 
                        tweet_mode = 'extended'
                        )

list = []
for tweet in tweets:
    text = tweet._json["full_text"]

    ugm_fess_data = {"user": tweet.user.screen_name,
                    'text' : text,
                    'favorite_count' : tweet.favorite_count,
                    'retweet_count' : tweet.retweet_count,
                    'created_at' : tweet.created_at}
        
    list.append(ugm_fess_data)

df = pd.DataFrame(list)
df.to_csv('ugm_fess_data.csv')