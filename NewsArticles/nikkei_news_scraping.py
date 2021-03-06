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

class AlreadyLogin(Exception):
    pass

class NikkeiNewsScraping():
    NIKKEI_NEWS_SHEET_ID = os.environ["GSPREAD_SHEET_ID_NEWS"]
    LOGIN_ID = os.environ["NIKKEI_LOGIN_ID"]
    LOGIN_PASS = os.environ["NIKKEI_LOGIN_PASS"]
    CHROME_DRIVER = os.environ["CHROME_DRIVER"]
    SITE = "Nikkei"

    def __init__(self):
        self.gs_access = GspreadAccess(NikkeiNewsScraping.NIKKEI_NEWS_SHEET_ID)
        conf_df = self.gs_access.read_df_from_gspread("conf")
        self.nikkei_df = conf_df.query('site.str.contains("Nikkei")', engine='python')
        self.login_url = self.nikkei_df["login_url"][0]
        self.base_url = self.nikkei_df["base_url"][0]
        options = Options()
        options.add_argument("--headless")
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--remote-debugging-port=9222')
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

    def get_articles(self, info):
        topics = self.get_topics(info)
        for topic in topics:
            topic |= self.get_article_body(info, topic["url"])
        return topics

    def get_topics(self, base_info):
        page_url = base_info["URL"]
        get_num = base_info["num"]
        category = base_info["category"]
        topic_class = base_info["topic_class"]
        topic_tag = base_info["topic_tag"]
        topic_item_list = base_info.filter(like="topic_item_", axis=0)

        self.driver.get(page_url)
        time.sleep(3)
        html = self.driver.page_source

        topic_list = []
        soup = BeautifulSoup(html, "html.parser")
        if soup.find(text=re.compile("????????????ID????????????????????????")) is not None:
            raise AlreadyLogin("???????????????ID??????????????????")

        blocks = soup.find_all(topic_tag, class_=topic_class)
        for block in blocks:
            topic = {}
            for idx in range(len(topic_item_list))[::3]:
                if topic_item_list[idx+2] != "None":
                    topic[topic_item_list[idx]] = block.select_one(f"{topic_item_list[idx+1]}.{topic_item_list[idx+2]}").text
                else:
                    topic[topic_item_list[idx]] = block.select_one(f"{topic_item_list[idx+1]}").text

            relative_url = block.a.get("href")
            topic["url"] = urljoin(self.base_url, relative_url)
            topic["site"] = NikkeiNewsScraping.SITE
            topic["category"] = category
            topic_list.append(topic)
        return topic_list

    def get_article_body(self, base_info, url):
        body_item_list = base_info.filter(like="body_", axis=0)

        self.driver.get(url)
        time.sleep(3)
        html = self.driver.page_source

        article = {}
        soup = BeautifulSoup(html, "html.parser")

        if soup.time is not None:
            article["date"] = soup.time[body_item_list["body_datetime_tag"]]
        else:
            article["date"] = 0
        elements = soup.find_all([body_item_list["body_paragraph_tag"],
                                  body_item_list["body_figure_tag"]],
                                 class_=[re.compile(body_item_list["body_paragraph_class"]),
                                         re.compile(body_item_list["body_figure_class"])])
        body_list = []
        for element in elements:
            if element.name == body_item_list["body_paragraph_tag"]:
                body_list.append({"paragraph": element.text})
            elif element.name == body_item_list["body_figure_tag"]:
                body_list.append({"figure": element.get(body_item_list["body_figure_elem"])})
        article["body"] = body_list
        return article

    def logout(self):
        logout_url = urljoin(self.base_url, "/logout")
        self.driver.get(logout_url)
        time.sleep(5)


if __name__ == "__main__":
    nikkei_news_scraping = NikkeiNewsScraping()
    nikkei_news_scraping.login()

    try:
        for index, info in nikkei_news_scraping.nikkei_df.iterrows():
            topic_list = nikkei_news_scraping.get_articles(info)
            upload_list = []
            for idx, topic in enumerate(topic_list):
                copy_topic = topic.copy()
                del copy_topic["body"]
                df = pd.DataFrame(copy_topic, index=[idx, ])
                if df.empty is True:
                    continue
                ret_df = nikkei_news_scraping.gs_access.add_dataframe_to_gspread(df, sheet_name=f"{NikkeiNewsScraping.SITE}_" + topic["category"])
                if len(ret_df) != 0:
                    upload_list.append(topic)
            if len(upload_list) != 0:
                upload_news_articles_to_notion(upload_list)
    except AlreadyLogin as e:
        print(e)
    finally:
        nikkei_news_scraping.logout()