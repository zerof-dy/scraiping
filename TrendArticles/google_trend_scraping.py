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

#TREND_EXCEL_FILE = DIRECTORY_PATH + "google_trend.xlsx"
#TEMP_TREND_EXCEL_FILE = DIRECTORY_PATH + "temp_country_trends.xlsx"
ARTICLE_JSON_FILE = DIRECTORY_PATH + "google_trend.json"
DB_FILE = DIRECTORY_PATH + "google_trend.db"
GOOGLE_TREND_SHEET_ID = os.environ["GSPREAD_SHEET_ID_GOOGLE_TREND"]

COUNTRY_SET = {
    'japan': "https://news.google.com/search?hl=ja&gl=JP&ceid=JP:ja",
    'united_states': "https://news.google.com/search?hl=en-US&gl=US&ceid=US%3Aen",
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


# def update_db_table(l_df, l_date):
#     s_date = f"t_{l_date}"
#     con = sqlite3.connect(DB_FILE)
#     cursor = con.cursor()
#
#     for name in l_df.head(0).columns:
#         if name == "ranking":
#             continue
#
#         create_table_query = f"CREATE TABLE IF NOT EXISTS {name} (ranking INTEGER PRIMARY KEY)"
#         cursor.execute(create_table_query)
#
#         read_table_query = f"SELECT * FROM {name}"
#         r_df = pd.read_sql_query(read_table_query, con)
#         r_df = r_df.set_index("ranking")
#
#         w_df = l_df[[name]]
#         w_df = w_df.rename(columns={name: s_date})
#
#         f_df = pd.concat([r_df, w_df], axis=1)
#         f_df = f_df.loc[:, ~f_df.columns.duplicated()]
#         f_df.to_sql(name, con, if_exists="replace")


# def update_xcel_file(l_df, l_date):
#
#     for name in l_df.head(0).columns:
#         if name == "ranking":
#             continue
#         w_df = l_df[[name]]
#         w_df = w_df.rename(columns={name: l_date})
#
#         if os.path.exists(TREND_EXCEL_FILE):
#             r_df = pd.read_excel(TREND_EXCEL_FILE, sheet_name=name, index_col=0)
#             w_df = pd.concat([r_df, w_df], axis=1)
#
#         try:
#             with pd.ExcelWriter(TEMP_TREND_EXCEL_FILE, engine="openpyxl", mode="a") as writer:
#                 w_df.to_excel(writer, sheet_name=name)
#         except FileNotFoundError as e:
#             with pd.ExcelWriter(TEMP_TREND_EXCEL_FILE, engine="openpyxl", mode="w") as writer:
#                 w_df.to_excel(writer, sheet_name=name)
#         except ValueError as e:
#             print("すでに同名のシートが存在するため、保存をスキップします")
#             return
#
#     if os.path.exists(TREND_EXCEL_FILE):
#         os.remove(TREND_EXCEL_FILE)
#     os.rename(TEMP_TREND_EXCEL_FILE, TREND_EXCEL_FILE)
#     print(f"{l_date} 分を追加しました")


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

            h4_blocks = h3_entry.select(".SbNwzf")
            inner_article_list = []
            for h4_idx, h4_entry in enumerate(h4_blocks):
                h4_title = h4_entry.select_one("h4 a").text
                h4_link = h4_entry.select_one("h4 a")["href"]
                h4_link = urllib.parse.urljoin(url, h4_link)
                inner_article_list.append(h4_title)
                inner_article_list.append(h4_link)

            article_list.append(h3_title)
            article_list.append(h3_link)
            if len(inner_article_list) != 0:
                article_list = article_list + inner_article_list
        word_list.append({"rank": no+1, "word": word, "articles": article_list})
    root_dic = {"date": date, "rank_list": word_list}

    # with open(file, "a") as f:
    #     writer = ndjson.writer(f, ensure_ascii=False)
    #     writer.writerow(root_dic)

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

#
# def loop():
#     date = datetime.datetime.today().strftime("%Y%m%d%H%M")
#     df = get_country_trend()
#
#     # if df is not None:
#     #     update_xcel_file(df, date)
#     if df is not None:
#         update_db_table(df, date)


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

