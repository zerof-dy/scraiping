import asyncio
from pyppeteer.launcher import launch
from pyppeteer.page import Page, Response
from pyppeteer.browser import Browser
from pyppeteer.element_handle import ElementHandle
from bs4 import BeautifulSoup
import pandas as pd
import dateutil.parser
import re
from ApiAccess.gspread.gspread_access import *
from ApiAccess.localfile.file_access import *
from ApiAccess.notion.notion_access import *


class WsjNewsScraping():
    WSJ_NEWS_SHEET_ID = os.environ["GSPREAD_SHEET_ID_NEWS"]
    LOGIN_ID = os.environ["WSJ_LOGIN_ID"]
    LOGIN_PASS = os.environ["WSJ_LOGIN_PASS"]

    def __init__(self):
        self.gs_access = GspreadAccess(WsjNewsScraping.WSJ_NEWS_SHEET_ID)
        conf_df = self.gs_access.read_df_from_gspread("conf")
        self.url = conf_df.loc["WSJ", "URL"]


    async def search_article_on_wsj(self, content):
        print(content)


    def login(self):
        html = asyncio.get_event_loop().run_until_complete(self.extract_html())
        return html

    async def extract_html(self):
        # ブラウザを起動。headless=Falseにすると実際に表示される
        browser: Browser = await launch(args=["--no-sandbox"])#headless=False)
        try:
            page: Page = await browser.newPage()

            # ログイン画面に遷移
            response: Response = await page.goto('https://sso.accounts.dowjones.com/login?state=hKFo2SByNWZjbzZyWUtRb2lqSFV5bW02S3BRV1B0eFZCblFSRKFupWxvZ2luo3RpZNkgUmdDbnpqZnBzcE4tbEFIWE4yRlE4Qk9aNlVqbnBIalijY2lk2SA1aHNzRUFkTXkwbUpUSUNuSk52QzlUWEV3M1ZhN2pmTw&client=5hssEAdMy0mJTICnJNvC9TXEw3Va7jfO&protocol=oauth2&scope=openid%20idp_id%20roles%20email%20given_name%20family_name%20djid%20djUsername%20djStatus%20trackid%20tags%20prts%20suuid%20createTimestamp&response_type=code&redirect_uri=https%3A%2F%2Faccounts.wsj.com%2Fauth%2Fsso%2Flogin&nonce=35563ba4-5a9b-42b9-a7ba-dca7e7056561&ui_locales=ja-jp-x-jwsj-0&ns=prod%2Faccounts-wsj#!/signin')
            if response.status != 200:
                raise RuntimeError(f'site is not available. status: {response.status}')

            # Username・Passwordを入力
            await page.type("input[id=username]", WsjNewsScraping.LOGIN_ID)
            login_btn: ElementHandle = await page.querySelector('button[type=button]')

            # Loginボタンクリック
            await asyncio.gather(login_btn.click(), page.waitForNavigation())
            await asyncio.sleep(3)
            await page.type("input[id=password]", WsjNewsScraping.LOGIN_PASS)
            await asyncio.sleep(1)
            await page.keyboard.press('Enter')
            # await submit_btn.click()
            await asyncio.sleep(10)

            response: Response = await page.goto(self.url, {"timeout": 60000})
            if response.status != 200:
                raise RuntimeError(f'site is not available. status: {response.status}')

            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            blocks = soup.find_all("article")
            article_list = []
            for block in blocks:
                article = {}
                texts = block.text.split()
                article["site"] = "WSJ"
                article["headline"] = texts[0]
                if len(texts) == 1:
                    summary = texts[0]
                else:
                    summary = texts[1]
                article["summary"] = re.sub(r'[0-9]*$', "", summary)
                article["url"] = block.select_one("h3 a")["href"]
                article_list.append(article)

            for article in article_list:
                response: Response = await page.goto(article["url"])
                if response.status != 200:
                    raise RuntimeError(f'site is not available. status: {response.status}')

                sub_content = await page.content()
                d_soup = BeautifulSoup(sub_content, "html.parser")
                try:
                    article["author"] = d_soup.find("meta", attrs={"name": "author"})["content"]
                except TypeError:
                    print("no author")
                    article["author"] = 0

                utc_time_str = d_soup.find("meta", attrs={"name": "article.published"})["content"]
                jst = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
                jst_timestamp = dateutil.parser.parse(utc_time_str).astimezone(jst)
                article["date"] = jst_timestamp.isoformat()
                paragraphs = d_soup.find_all("p")
                body = []
                for idx, paragraph in enumerate(paragraphs):
                    if idx < 2:
                        continue
                    elif idx > len(paragraphs) - 5:
                        break
                    body.append({"paragraph": paragraph.text})
                article["body"] = body

        finally:
            await browser.close()

        print(article_list)
        df = pd.DataFrame(article_list)
        df.drop('body', axis=1, inplace=True)
        ret_df = self.gs_access.add_dataframe_to_gspread(df, sheet_name="WSJ")
        date_diff_list = ret_df['date'].tolist()
        upload_list = []
        for article in article_list:
            if article["date"] in date_diff_list:
                upload_list.append(article)

        if len(upload_list) != 0:
            upload_news_articles_to_notion(upload_list)


if __name__ == "__main__":
    wsj_news_scraping = WsjNewsScraping()
    html = wsj_news_scraping.login()
    # wsj_news_scraping.search_article_on_wsj()

