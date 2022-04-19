import datetime
import urllib
import re
import ndjson
import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import csv
import os
from ApiAccess.notion.notion_access import *
from ApiAccess.localfile.file_access import *
from ApiAccess.gspread.gspread_access import *

URL = "https://search.yahoo.co.jp/realtime"
DIRECTORY_PATH = "./"

class YahooRealtimeScraping():
    YAHOO_REALTIME_SHEET_ID = os.environ["GSPREAD_SHEET_ID_YAHOO_REALTIME"]

    def __init__(self):
        self.gs_access = GspreadAccess(YahooRealtimeScraping.YAHOO_REALTIME_SHEET_ID)


    def get_datetime(self):
        t_delta = datetime.timedelta(hours=9)
        JST = datetime.timezone(t_delta, 'JST')
        time_stamp = datetime.datetime.now(JST)
        return time_stamp

    def get_article_for_words(self, url):
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


    def run_scraping_google_trend(self):
        # セッション開始
        session = HTMLSession()
        r = session.get(URL)
        # ブラウザエンジンでHTMLを生成させる
        r.html.render()
        date = self.get_datetime()
        # スクレイピング
        ranking_rows = r.html.find("div.main")
        if ranking_rows:
            # list = ranking_rows[0].find("ol > li")
            article_urls = ranking_rows[0].find("ol > li > a")
            ranking = ranking_rows[0].find("article")

        list = ranking[1].text.splitlines()

        tag_rank_list = {}
        save_list = []
        for idx, url in enumerate(article_urls):
            con_url = urllib.parse.urljoin(URL, url.attrs["href"])
            article_list = self.get_article_for_words(con_url)
            save_list.append({"rank": list[idx*2+1], "word": list[idx*2+2], "articles": article_list})
            tag_rank_list[list[idx*2+2]] = idx + 1

        root_dic = {"date": date.strftime("%Y%m%d%H%M"), "rank_list": save_list}
        upload_trend_to_notion("Yahooリアルタイム", root_dic)
        tag_rank_list |= convert_date_to_tag_dict(date)

        tag_df = pd.DataFrame.from_dict(data={date: tag_rank_list}, orient="index")
        self.gs_access.add_dataframe_to_gspread(tag_df, sheet_name="Yahooリアルタイム")


if __name__ == "__main__":
    yahoo_real_scraping = YahooRealtimeScraping()
    yahoo_real_scraping.run_scraping_google_trend()
