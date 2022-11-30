# Libraries
import pip

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install('instaloader')

import instaloader
import pandas as pd
import csv
import os.path

def extract_tweet():
    L = instaloader.Instaloader()

    # Login dengan akun instagram
    L.login("lemiluvv","cONmb_dolW5X_MU")

    # Menentukan target akun instagram
    target_id='ganjar_pranowo'

    # ambil semua post dari akun target
    posts = instaloader.Profile.from_username(L.context, target_id).get_posts()

    # simpan data komentar dari post terbaru ke sebuah array
    list=[]
    i=0
    for post in posts:
        if i>=100:
            break
        for comment in post.get_comments():
            search_data = {
                # scrape username dan text saja agar tidak perlu cleaning berlebihan
                'user': comment.owner.username,
                'text': comment.text
            }
            list.append(search_data)
            i+=1
            if i>=100:
                break

    # Memasukan data dari array ke dataframe
    new_comments_df = pd.DataFrame(list)

    # cek apakah file sudah ada/belum
    file_exists = os.path.exists('comments.txt')
    if(not file_exists):
        # buat file baru
        new_comments_df.to_csv('comments.csv', mode='a', index=False)
    else:
        # append ke file lama
        new_comments_df.to_csv('comments.csv', mode='a', index=False, header=False)
    print(new_comments_df)