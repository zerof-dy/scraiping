from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.chrome import service as fs
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import datetime
import re
from ApiAccess.localfile.file_access import *
from ApiAccess.notion.notion_access import *
from ApiAccess.gspread.gspread_access import GspreadAccess
import pandas as pd

SITE = {
    "name": "Qiita",
    "url": "https://qiita.com/",
    "search_url": "https://qiita.com/tags/",
}

CHROME_DRIVER = os.environ["CHROME_DRIVER"]
QIITA_SHEET_ID = os.environ["GSPREAD_SHEET_ID_QIITA"]

options = Options()
options.add_argument('--headless')
chrome_service = fs.Service(executable_path=CHROME_DRIVER)
driver = webdriver.Chrome(service=chrome_service, options=options)


def get_article_info(article_blocks, check):
    article_list = []
    for article in article_blocks:
        dict = {}
        writer = article.find_element(by=By.TAG_NAME, value="a p")
        dict["writer"] = writer.text
        date = article.find_element(by=By.TAG_NAME, value="time").get_attribute("datetime")
        dict["date"] = date
        title = article.find_element(by=By.TAG_NAME, value="h2")
        dict["title"] = title.text
        url = title.find_element(by=By.XPATH, value="./a[@href]").get_attribute("href")
        dict["url"] = url
        tags = article.find_elements(by=By.TAG_NAME, value="div a")
        tag_list = []
        for tag in tags:
            if "tag" in tag.get_attribute("href"):
                tag_list.append(tag.text)
        dict["tag"] = tag_list

        dict["site"] = SITE["name"]
        dict["site_url"] = SITE["url"]
        dict["pick_up"] = check
        dict["summary"] = "none"

        article_list.append(dict)

    return article_list


if __name__ == "__main__":
    gspread = GspreadAccess(QIITA_SHEET_ID)
    conf_df = gspread.read_df_from_gspread("conf")
    word_list = conf_df["ワード"]

    for word in word_list:
        driver.get(SITE["search_url"] + word)
        time.sleep(1)

        trend_block = driver.find_element(by=By.CLASS_NAME,
                                          value="p-tagShow_mainMiddle").find_elements(by=By.TAG_NAME, value="article")
        trend_articles = get_article_info(trend_block, True)

        latest_block = driver.find_element(by=By.CLASS_NAME,
                                           value="p-tagShow_mainBottom").find_elements(by=By.TAG_NAME, value="article")
        latest_articles = get_article_info(latest_block, False)
        articles = trend_articles + latest_articles

        df = pd.DataFrame(articles)
        df.drop('tag', axis=1, inplace=True)
        ret_df = gspread.add_dataframe_to_gspread(df, sheet_name=word)
        date_diff_list = ret_df['date'].tolist()
        upload_list = []
        for article in articles:
            if article["date"] in date_diff_list:
                upload_list.append(article)

        if len(upload_list) != 0:
            upload_tech_articles_to_notion(upload_list)

