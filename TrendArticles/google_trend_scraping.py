import datetime
import json
import ndjson
import os
import re
import sqlite3
import time
import os
from bs4 import BeautifulSoup
import pandas as pd
from pytrends.request import TrendReq
import selenium.common.exceptions
import urllib
import requests
from selenium import webdriver
from selenium.webdriver.chrome import service as fs
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from StorageAccess.notion.notion_access import *
from StorageAccess.gspread.gspread_access import *
from StorageAccess.localfile.file_access import *


DIRECTORY_PATH = "./"

ARTICLE_JSON_FILE = DIRECTORY_PATH + "google_trend.json"
DB_FILE = DIRECTORY_PATH + "google_trend.db"
GOOGLE_TREND_SHEET_ID = os.environ["GSPREAD_SHEET_ID_GOOGLE_TREND"]

COUNTRY_SET = {
    'japan': "https://news.google.com/search?hl=ja&gl=JP&ceid=JP:ja",
    'united_states': "https://news.google.com/search?hl=en-US&gl=US&ceid=US%3Aen",
    'india': "https://news.google.com/search?hl=en-IN&gl=IN&ceid=IN:en",
    'brazil': "https://news.google.com/search?hl=pt-BR&gl=BR&ceid=BR:pt-419",
    'taiwan': "https://news.google.com/search?hl=zh-TW&gl=TW&ceid=TW:zh-Hant",
    'australia': "https://news.google.com/search?hl=en-AU&gl=AU&ceid=AU:en",
}


def get_country_trend():
    pytrends = TrendReq(hl='ja-jp', tz=540)
    dfs = []
    rank = list(range(1, 21))
    for con in COUNTRY_SET.keys():
        df = pytrends.trending_searches(pn=con)
        dfs.append(df)
    df_concat = pd.concat(dfs, axis=1)
    df_concat.columns = COUNTRY_SET.keys()
    df_concat.insert(loc=0, column="ranking", value=rank)
    df_concat = df_concat.set_index("ranking")

    return df_concat


def search_article_on_google_news(url, words, date):
    word_list = []
    for no, word in enumerate(words):
        res = requests.get(url, params={"q": word})
        soup = BeautifulSoup(res.content, "html.parser")
        h3_blocks = soup.select(".xrnccd")

        article_list = []
        for h3_idx, h3_entry in enumerate(h3_blocks):
            if h3_idx > 10:
                break
            h3_title = h3_entry.select_one("h3 a").text
            h3_link = h3_entry.select_one("h3 a")["href"]
            h3_link = urllib.parse.urljoin(url, h3_link)
            # h4_blocks = h3_entry.select(".SbNwzf")
            # inner_article_list = []
            # for h4_idx, h4_entry in enumerate(h4_blocks):
            #     h4_title = h4_entry.select_one("h4 a").text
            #     h4_link = h4_entry.select_one("h4 a")["href"]
            #     h4_link = urllib.parse.urljoin(url, h4_link)
            #     inner_article_list.append(h4_title)
            #     inner_article_list.append(h4_link)
            article_list.append(h3_title)
            article_list.append(h3_link)
            # if len(inner_article_list) != 0:
            #     article_list = article_list + inner_article_list
        word_list.append({"rank": no+1, "word": word, "articles": article_list})
    root_dic = {"date": date, "rank_list": word_list}


    return root_dic


def get_article_for_trend(l_df, l_date):
    ret_dic = {}
    for name in l_df.head(0).columns:
        if name == "ranking":
            continue
        url = COUNTRY_SET[name]
        words = l_df[name].to_list()
        #country_file_path = ARTICLE_JSON_FILE.replace(".json", f"_{name}.json")
        root_dic = search_article_on_google_news(url, words, l_date)

        ret_dic[name] = root_dic

    return ret_dic


if __name__ == "__main__":
    t_delta = datetime.timedelta(hours=9)
    JST = datetime.timezone(t_delta, 'JST')
    date = datetime.datetime.now(JST).strftime("%Y%m%d%H%M")

    df = get_country_trend()
    if df is not None:
        ret_dic = get_article_for_trend(df, date)
    if ret_dic is not None:
        for country, dic in ret_dic.items():
            upload_trend_to_notion(f"Googleトレンド  {country}", dic)
            tag_data = {}
            for idx, list in enumerate(dic["rank_list"]):
                tag_data[list["word"]] = idx + 1

            tag_data |= convert_date_to_tag_dict(date)
            iso_date = datetime.datetime.strptime(date, "%Y%m%d%H%M").isoformat()
            tag_df = pd.DataFrame.from_dict(data={iso_date+".000+09:00": tag_data}, orient="index")
            #add_dataframe_to_csv(tag_df, dir="/Users/daiki/work/statistics/", file=f"notion_tags_Googleトレンド  {country}.csv")
            add_dataframe_to_gspread(tag_df, sheet_id=GOOGLE_TREND_SHEET_ID, sheet_name=country, type_="all")

