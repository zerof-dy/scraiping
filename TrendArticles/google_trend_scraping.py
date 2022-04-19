import urllib

import requests
from bs4 import BeautifulSoup
from pytrends.request import TrendReq
import pandas as pd
from ApiAccess.gspread.gspread_access import *
from ApiAccess.localfile.file_access import *
from ApiAccess.notion.notion_access import *


class GoogleTrendScraping():
    GOOGLE_TREND_SHEET_ID = os.environ["GSPREAD_SHEET_ID_GOOGLE_TREND"]

    def __init__(self):
        self.gs_access = GspreadAccess(GoogleTrendScraping.GOOGLE_TREND_SHEET_ID)

        conf_df = self.gs_access.read_df_from_gspread("conf")
        self.country_set = dict(zip(conf_df["国"], conf_df["検索URL"]))
        self.translate = conf_df["翻訳"][0]

    def get_datetime(self):
        t_delta = datetime.timedelta(hours=9)
        JST = datetime.timezone(t_delta, 'JST')
        time_stamp = datetime.datetime.now(JST)
        return time_stamp

    def get_country_trend(self, country):
        pytrends = TrendReq(hl='ja-jp', tz=540)
        rank = list(range(1, 21))
        trend_df = pytrends.trending_searches(pn=country)
        trend_df.columns = [country]
        scraping_time = self.get_datetime()
        trend_df.insert(loc=0, column="ranking", value=rank)
        #trend_df.set_index("ranking", inplace=True)

        return trend_df[country].to_list(), scraping_time

    def get_article_for_words(self, word_list, date, url):
        if "news.google" in url:
            result_dic = self.search_article_on_google_news(word_list, date, url)
            return result_dic
        else:
            return None

    def search_article_on_google_news(self, word_list, date, url):
        tag_rank_list = {}
        rank_list = []
        for no, word in enumerate(word_list):
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
                article_list.append(h3_title)
                article_list.append(h3_link)

            rank_list.append({"rank": (no + 1), "word": word, "articles": article_list})
            tag_rank_list[word] = no + 1
        result_dic = {"date": date, "rank_list": rank_list}
        return result_dic, tag_rank_list


    def run_scraping_google_trend(self):
        for country, url in self.country_set.items():
            df, date = self.get_country_trend(country)
            if df is not None:
                ret_dict, tag_dict = self.get_article_for_words(df, date.strftime("%Y%m%d%H%M"), url)
            if ret_dict is not None:
                upload_trend_to_notion(f"Googleトレンド  {country}", ret_dict, translate_engine=self.translate)
                tag_dict |= convert_date_to_tag_dict(date)
                # iso_date = datetime.datetime.strptime(date, "%Y%m%d%H%M").isoformat()
                tag_df = pd.DataFrame.from_dict(data={date: tag_dict}, orient="index")
                self.gs_access.add_dataframe_to_gspread(tag_df, sheet_name=country)


if __name__ == "__main__":
    google_trend_scraping = GoogleTrendScraping()
    google_trend_scraping.run_scraping_google_trend()
