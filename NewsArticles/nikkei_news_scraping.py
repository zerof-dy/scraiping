import asyncio
import requests
from pyppeteer.launcher import launch
from pyppeteer.page import Page, Response
from pyppeteer.browser import Browser
from pyppeteer.element_handle import ElementHandle
from bs4 import BeautifulSoup
from ApiAccess.gspread.gspread_access import *
from ApiAccess.localfile.file_access import *
from ApiAccess.notion.notion_access import *


class NikkeiNewsScraping():
    NIKKEI_NEWS_SHEET_ID = os.environ["GSPREAD_SHEET_ID_NEWS"]

    def __init__(self):
        self.page = None
        self.browser = None
        self.gs_access = GspreadAccess(NikkeiNewsScraping.NIKKEI_NEWS_SHEET_ID)
        conf_df = self.gs_access.read_df_from_gspread("conf")
        # self.url = conf_df.loc["Nikkei", "URL"]
        self.nikkei_df = conf_df.query('サイト名.str.contains("Nikkei")', engine='python')
        self.login_url = self.nikkei_df["ログインURL"][0]


    def login(self):
        asyncio.get_event_loop().run_until_complete(self.extract_html())


    async def extract_html(self):
        # ブラウザを起動。headless=Falseにすると実際に表示される
        self.browser = await launch(args=["--no-sandbox"], headless=False)
        try:
            self.page = await self.browser.newPage()
            # ログイン画面に遷移
            response: Response = await self.page.goto(self.login_url)
            if response.status != 200:
                raise RuntimeError(f'site is not available. status: {response.status}')

            # Username・Passwordを入力
            await self.page.type("input[type=text]", 'shop.dy05@gmail.com')
            await self.page.type("input[type=password]", 'YaNaGi0940')
            # login_btn = await page.querySelector('button[type=submit]')
            await self.page.keyboard.press('Enter')

            # await asyncio.sleep(1)
        except:
            raise RuntimeError('failed to login')

    def get_rankig_arcles(self):
        for info in self.nikkei_df.itertuples():
            url = info.URL
            category = info.カテゴリ
            num = info.取得数



            print("dummy")









if __name__ == "__main__":
    nikkei_news_scraping = NikkeiNewsScraping()
    nikkei_news_scraping.login()
    nikkei_news_scraping.get_rankig_arcles()
