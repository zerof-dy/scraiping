from selenium import webdriver
from selenium.common import exceptions
import time
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium.webdriver.chrome import service as fs
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from ApiAccess.gspread.gspread_access import *
from ApiAccess.localfile.file_access import *
from ApiAccess.notion.notion_access import *


class NikkeiNewsScraping():
    NIKKEI_NEWS_SHEET_ID = os.environ["GSPREAD_SHEET_ID_NEWS"]
    LOGIN_ID = os.environ["NIKKEI_LOGIN_ID"]
    LOGIN_PASS = os.environ["NIKKEI_LOGIN_PASS"]
    CHROME_DRIVER = os.environ["CHROME_DRIVER"]

    def __init__(self):
        self.gs_access = GspreadAccess(NikkeiNewsScraping.NIKKEI_NEWS_SHEET_ID)

        conf_df = self.gs_access.read_df_from_gspread("conf")
        self.nikkei_df = conf_df.query('site.str.contains("Nikkei")', engine='python')
        self.login_url = self.nikkei_df["login_url"][0]
        self.base_url = self.nikkei_df["base_url"][0]
        options = Options()
        options.add_argument("--headless")
        chrome_service = fs.Service(executable_path=NikkeiNewsScraping.CHROME_DRIVER)
        self.driver = webdriver.Chrome(service=chrome_service, options=options)

    def login(self):
        self.driver.get(self.login_url)
        login_email = self.driver.find_element(by=By.ID, value="LA7010Form01:LA7010Email")
        login_email.send_keys(NikkeiNewsScraping.LOGIN_ID)
        login_pass = self.driver.find_element(by=By.ID, value="LA7010Form01:LA7010Password")
        login_pass.send_keys(NikkeiNewsScraping.LOGIN_PASS)
        self.driver.find_element(by=By.CLASS_NAME, value="btnM1").click()
        time.sleep(2)

    def get_articles(self):
        all_category_list = []
        for index, info in self.nikkei_df.iterrows():
            headlines = self.get_headlines(info)
            all_category_list.append(headlines)
        return all_category_list

    def get_headlines(self, base_info):
        page_url = base_info["url"]
        category = base_info["category"]
        get_num = base_info["num"]
        block_class = base_info["blocks"]
        tag = base_info["tag"]
        topic_item_list = base_info.filter(like="topic_item_", axis=0)

        self.driver.get(page_url)
        time.sleep(3)
        html = self.driver.page_source

        topic_list = []
        soup = BeautifulSoup(html, "html.parser")
        blocks = soup.find_all(tag, class_=block_class)
        for block in blocks:
            topic = {}
            for idx in range(len(topic_item_list))[::3]:
                if topic_item_list[idx+2] != "None":
                    topic[topic_item_list[idx]] = block.select_one(f"{topic_item_list[idx+1]}.{topic_item_list[idx+2]}").text
                else:
                    topic[topic_item_list[idx]] = block.select_one(f"{topic_item_list[idx+1]}").text

            relative_url = block.a.get("href")
            topic["url"] = urljoin(self.base_url, relative_url)
            topic["site"] = "Nikkei"
            topic_list.append(topic)
        return {"category": category, "topics": topic_list}

    def get_article_bodies(self):
        pass

    def logout(self):
        logout_url = urljoin(self.base_url, "/logout")
        self.driver.get(logout_url)
        time.sleep(5)


if __name__ == "__main__":
    nikkei_news_scraping = NikkeiNewsScraping()
    nikkei_news_scraping.login()
    all_category_list = nikkei_news_scraping.get_articles()
    nikkei_news_scraping.logout()

    for list in all_category_list:
        df = pd.DataFrame(list["topics"])
        if df.empty is True:
            continue
        ret_df = nikkei_news_scraping.gs_access.add_dataframe_to_gspread(df, sheet_name="Nikkei_" + list["category"])

        diff_dict = ret_df.to_dict()
        upload_news_articles_to_notion(diff_dict)