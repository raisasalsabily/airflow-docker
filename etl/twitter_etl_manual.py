import pip  
  
def install(package):  
    if hasattr(pip, 'main'):  
        pip.main(['install', package])  
    else:  
        pip._internal.main(['install', package])  
  
install('tweepy')     
install('s3fs')         
  
import pandas as pd   
from datetime import datetime  
import os.path  
  
import tweepy  
import s3fs   
  
# access key dari twitter  
a_key = 'qUnv0UK4ePe0qgMYZp0jV72YI'   
a_secret = 'ghN87rNJQIwR7c9cBRHROA4gwNDRN6aBJWL1u7JDMgojNwr0Hj'  
c_key = '1448947389847404550-3sj9PK07d1NWj8OStIuXWK4M8W1HCa'   
c_secret = 'nISY2fPiEzI5gNs1DyxyaGTbH2dqFrDQNGH9wJRHr33hF'  
  
def run_twitter_etl():  
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
  
    list = []  
    for tweet in tweets:  
        search_data = {  
                    # scrape username, text, dan waktu dibuat saja agar tidak perlu cleaning berlebihan  
                    'user': tweet.user.screen_name,  
                    'text': tweet.text  
                    }  
        list.append(search_data)  
  
    new_tweets_df = pd.DataFrame(list) # masukkan ke dataframe  
  
  
    # import ke csv - cek apakah file sudah ada/belum  
    file_exists = os.path.exists('tweets.txt')  
    if(not file_exists):  
        # buat file baru  
        new_tweets_df.to_csv('tweets.csv', mode='a', index=False)  
    else:  
        # append ke file lama  
        new_tweets_df.to_csv('tweets.csv', mode='a', index=False, header=False)  
  
  
# untuk testing  
run_twitter_etl()