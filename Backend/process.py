# imports 

from time import sleep
from bs4 import BeautifulSoup, element
import urllib
import requests
import pandas as pd
import numpy as np
import firebase_admin
from firebase_admin import credentials, firestore


def get_data():
    pages = 19
    rec_count = 0
    rank = []
    gname = []
    platform = []
    year = []
    genre = []
    critic_score = []
    user_score = []
    publisher = []
    developer = []
    sales_na = []
    sales_pal = []
    sales_jp = []
    sales_ot = []
    sales_gl = []

    urlhead = 'http://www.vgchartz.com/gamedb/?page='
    urltail = '&console=&region=All&developer=&publisher=&genre=&boxart=Both&ownership=Both'
    urltail += '&results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0'
    urltail += '&showpublisher=1&showvgchartzscore=0&shownasales=1&showdeveloper=1&showcriticscore=1'
    urltail += '&showpalsales=0&showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1'
    urltail += '&showlastupdate=0&showothersales=1&showgenre=1&sort=GL'

    for page in range(1, pages):
        surl = urlhead + str(page) + urltail
        r = requests.get(surl)
        soup = BeautifulSoup(r.content,'lxml')
        print(f"Page: {page}")

        # vgchartz website is really weird so we have to search for
        # <a> tags with game urls


        #game_tags = list(filter(
         #   lambda x: x.attrs['href'].startswith('http://www.vgchartz.com/game/'),
            # discard the first 10 elements because those
            # links are in the navigation bar
          #  soup.find_all("a")
        #))[10:]

        game_tags=[]

        for a in soup.find_all('a', href=True):
            if a['href'].startswith("https://www.vgchartz.com/game/"):
                game_tags.append(a)


        for tag in game_tags:

            # add name to list
            gname.append(" ".join(tag.string.split()))
            print(f"{rec_count + 1} Fetch data for game {gname[-1]}")

            # get different attributes
            # traverse up the DOM tree
            data = tag.parent.parent.find_all("td")
            rank.append(np.int32(data[0].string))
            platform.append(data[3].find('img').attrs['alt'])
            publisher.append(data[4].string)
            developer.append(data[5].string)
            critic_score.append(
                float(data[6].string) 
                if not data[6].string.startswith("N/A") else np.nan)
            sales_na.append(
                float(data[9].string[:-1]) if
                not data[9].string.startswith("N/A") else np.nan)
            sales_pal.append(
                float(data[10].string[:-1]) if
                not data[10].string.startswith("N/A") else np.nan)
            sales_jp.append(
                float(data[11].string[:-1]) if
                not data[11].string.startswith("N/A") else np.nan)
            sales_ot.append(
                float(data[12].string[:-1]) if
                not data[12].string.startswith("N/A") else np.nan)
            sales_gl.append(
                float(data[8].string[:-1]) if
                not data[8].string.startswith("N/A") else np.nan)
            release_year = data[13].string.split()[-1]
            # different format for year
            if release_year.startswith('N/A'):
                year.append('N/A')
            else:
                if int(release_year) >= 80:
                    year_to_add = np.int32("19" + release_year)
                else:
                    year_to_add = np.int32("20" + release_year)
                year.append(year_to_add)


            rec_count += 1


    columns = {
        'Rank': rank,
        'Name': gname,
        'Platform': platform,
        'Year': year,
        'Critic_Score': critic_score,
    #     'User_Score': user_score,
        'Publisher': publisher,
        'Developer': developer,
        'NA_Sales': sales_na,
        'PAL_Sales': sales_pal,
        'JP_Sales': sales_jp,
        'Other_Sales': sales_ot,
        'Global_Sales': sales_gl
    }
    print(rec_count)
    df = pd.DataFrame(columns)
    print(df.columns)

    #removing records with empty values 
    df = df.dropna()

    df = df[[
        'Rank', 'Name', 'Platform', 'Year',
        'Publisher', 'Developer', 'Critic_Score', #'User_Score',
        'NA_Sales', 'PAL_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']]
    df.to_csv("vgsales.csv", sep=",", encoding='utf-8', index=False)

    return df


def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to cloud db
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64) or isinstance(val1, np.int32):
            val1 = int(val1)

        if isinstance(val1, np.float64) or isinstance(val1, np.float32):
            val1 = float(val1)

        new[key1] = val1

    return new

def post_data(data_dict, api_endpoint):
    for serial in range(len(data_dict)):
        r = requests.post(url = api_endpoint + "/add", json = data_dict[serial])
        pastebinurl = r.text
        print(pastebinurl)


def delete_collection(api_endpoint):
    r = requests.delete(url = api_endpoint + "/delete")

def update_db():
    df = get_data()

    data_dict = df.to_dict("records")
    for i in range(len(data_dict)):
        data_dict[i]['id'] = str(i)
        data_dict[i] = correct_encoding(data_dict[i])

    api_endpoint = "http://127.0.0.1:5000"

    delete_collection(api_endpoint= api_endpoint)
    post_data(data_dict= data_dict, api_endpoint= api_endpoint)


if __name__ == "__main__":

    while True:

        update_db()
        print("updated db")
        sleep(86400)