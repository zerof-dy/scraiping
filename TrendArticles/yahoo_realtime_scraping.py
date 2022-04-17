import datetime
import urllib
import re
import ndjson
import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import csv
import os
from StorageAccess.notion.notion_access import *
from StorageAccess.localfile.file_access import *
from StorageAccess.gspread.gspread_access import *

URL = "https://search.yahoo.co.jp/realtime"
DIRECTORY_PATH = "./"
YAHOO_REALTIME_SHEET_ID = os.environ["GSPREAD_SHEET_ID_YAHOO_REALTIME"]


def get_keyword_article(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "html.parser")
    link_block = soup.find("a", text="ニュース")

    news_res = requests.get(link_block["href"])
    news_soup = BeautifulSoup(news_res.content, "html.parser")
    news_blocks = news_soup.find_all("li", class_="newsFeed_item-ranking")

    article_list = []
    for news_idx, news_entry in enumerate(news_blocks):
        headline = news_entry.select_one(".newsFeed_item_title").text
        link = news_entry.select_one(".newsFeed_item_link")["href"]

        article_list.append(headline)
        article_list.append(link)
        if news_idx > 5:
            break

    return article_list


if __name__ == "__main__":
    # セッション開始
    session = HTMLSession()
    r = session.get(URL)
    # ブラウザエンジンでHTMLを生成させる
    r.html.render()
    # スクレイピング
    ranking_rows = r.html.find("div.main")
    ranking_list = []
    if ranking_rows:
        list = ranking_rows[0].find("ol > li")
        urls = ranking_rows[0].find("ol > li > a")
        ranking = ranking_rows[0].find("article")
    tweet_urls = []
    for url in urls:
        comb_url = urllib.parse.urljoin(URL, url.attrs["href"])
        tweet_urls.append(comb_url)

    t_delta = datetime.timedelta(hours=9)
    JST = datetime.timezone(t_delta, 'JST')
    date = datetime.datetime.now(JST).strftime("%Y%m%d%H%M")
    list = ranking[1].text.splitlines()
    title_list = list[2:len(list):2]
    save_list = []

    for i in range(len(list)//2):
        article_list = get_keyword_article(tweet_urls[i])
        save_list.append({"rank": list[i*2+1], "word": list[i*2+2], "articles": article_list})

    root_dic = {"date": date, "rank_list": save_list}
    upload_trend_to_notion("Yahooリアルタイム", root_dic)

    tag_data = {}
    for idx, list in enumerate(root_dic["rank_list"]):
        tag_data[list["word"]] = idx + 1

    tag_data |= convert_date_to_tag_dict(date)
    date = datetime.datetime.strptime(date, "%Y%m%d%H%M").isoformat()
    tag_df = pd.DataFrame.from_dict(data={date+".000+09:00": tag_data}, orient="index")
    add_dataframe_to_gspread(tag_df, sheet_id=YAHOO_REALTIME_SHEET_ID, sheet_name="Yahooリアルタイム", type_="all")



