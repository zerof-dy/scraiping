# ライブラリのインポート
import tweepy
import pytz
import re
import pandas as pd
import time
import os
from StorageAccess.localfile.file_access import *
from StorageAccess.notion.notion_access import *
from StorageAccess.gspread.gspread_access import *
from datetime import datetime, timezone, timedelta

# Twitterの認証
# 取得したキーを格納
#BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJ26bAEAAAAADPeoZDmMFu1sI4nvW1WwOpwH12E%3D5D58SJikD2SPcaWPbaX3oxDWDc5FHerZzHHcrro9Nfgj2qWyRb"
BEARER_TOKEN = os.environ['TWITTER_BEARER_TOKEN']
#TWITTER_API_KEY = "CsTT5jIfHwIc3JdPOLuunS7Vy"
API_KEY = os.environ['TWITTER_API_KEY']

#TWITTER_API_SECRET = "GEDR8oWGbZuiDp4w3tonhtRxQR3b0yRYYn4QVg2BCbbKcXKbcs"
API_SECRET = os.environ['TWITTER_API_SECRET']
#TWITTER_ACCESS_TOKEN = "2538954553-iid8FWW5pEgufnDHx1KkpGRlS2dWfWBQE16H9DP"
ACCESS_TOKEN = os.environ['TWITTER_ACCESS_TOKEN']
#TWITTER_ACCESS_TOKEN_SECRET = "zCfI9h155Ryp63SwlwK2PKCg88fEB0EQnGZDgAk2wJeop"
ACCESS_TOKEN_SECRET = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
GSPREAD_SHEET_ID_TWITTER = os.environ["GSPREAD_SHEET_ID_TWITTER"]
# 検索条件の設定
item_number = 60
user_names = [
    "shinjuku_sokai_",
    "GaaSyy_ch",
    "takigare3",
    "MacopeninSUTABA",
    "takemachelin",
    "itm_nlab",
]


# Tweepy設定
auth = tweepy.OAuthHandler(API_KEY, API_SECRET)  # Twitter API認証
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)  # アクセストークン設定

client = tweepy.Client(bearer_token=BEARER_TOKEN,
                       consumer_key=API_KEY,
                       consumer_secret=API_SECRET,
                       access_token=ACCESS_TOKEN,
                       access_token_secret=ACCESS_TOKEN_SECRET)


# 関数:　UTCをJSTに変換する
def change_time_JST(u_time):
    # イギリスのtimezoneを設定するために再定義する
    utc_time = datetime(u_time.year, u_time.month, u_time.day,
                        u_time.hour, u_time.minute, u_time.second,
                        tzinfo=timezone.utc)
    # タイムゾーンを日本時刻に変換
    jst_time = utc_time.astimezone(pytz.timezone("Asia/Tokyo"))
    # 文字列で返す
    str_time = jst_time.strftime("%Y-%m-%d_%H:%M:%S")
    return str_time


def get_tweet_thread(tweet_id):
    tweet_data = client.get_tweet(id=int(tweet_id),
                                  tweet_fields=['created_at',
                                                "referenced_tweets"],
                                  expansions=["author_id"],
                                  user_fields=["username"])
    # time.sleep(2)
    rep = {}
    ret_list = []
    ref_id = 0
    if tweet_data.data is not None:
        rep["tweet_id"] = tweet_id
        rep["user_id"] = tweet_data.includes["users"][0].id
        rep["username"] = tweet_data.includes["users"][0].username
        rep["text"] = tweet_data.data.text
        rep["created_at"] = change_time_JST(tweet_data.data.created_at)

        if tweet_data.data.referenced_tweets != None:
            ref_id = tweet_data.data.referenced_tweets[0]["id"]
        ret_list.append(rep)
        if tweet_id != ref_id:
            ret_list.extend(get_tweet_thread(ref_id))

    # 結果出力
    return ret_list


if __name__ == "__main__":

    e_date = datetime.now() - timedelta(days=3)
    for name in user_names:
        # 検索条件を元にツイートを抽出
        search = str(client.get_user(username=name))
        user_id = re.findall("(?<=User id=)\w+", search)[0]
        tweets = client.get_users_tweets(user_id,
                                         # start_time=e_date.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                                         max_results=item_number,
                                         tweet_fields=['created_at',
                                                       'public_metrics',
                                                       'conversation_id',
                                                       'referenced_tweets',
                                                       'entities'],
                                         expansions=['author_id',
                                                     'referenced_tweets.id'],
                                         user_fields=['username',
                                                      'public_metrics',
                                                      'description',
                                                      'created_at',
                                                      'url']
                                         )
        # time.sleep(2)
        date = datetime.today().strftime("%Y%m%d%H")
        for u in tweets.includes['users']:
            if u["id"] == int(user_id):
                user = u
        # 抽出したデータから必要な情報を取り出す
        # 取得したツイートを一つずつ取り出して必要な情報をtweet_dataに格納する
        tw_data = []
        for idx, tweet in enumerate(tweets.data):
            # ツイート時刻とユーザのアカウント作成時刻を日本時刻にする
            tweet_time = change_time_JST(tweet.created_at)
            create_account_time = change_time_JST(user.created_at)
            # tweet_dataの配列に取得したい情報を入れていく
            # リプライツイートの元ツイートも取りたいときは有効にする
            rep_tweet_list = []
            ref_url = "None"
            ref_text = "None"
            if tweet.entities is not None and "urls" in tweet.entities.keys():
                for url in tweet.entities["urls"]:
                    if f"https://twitter.com/{name}" not in url["expanded_url"]:
                        ref_url = url["url"]
                        ref_text = "== LINK =="
                        break
            pattern = re.compile(r"^@")
            if bool(pattern.search(tweet.text)):
                tweet_type = "Retweet"
            else:
                tweet_type = "Tweet"

            tweet_url = f"https://twitter.com/{name}/status/{str(tweet.id)}"
            tw_data.append([
                str(tweet.id),
                str(tweet_time),
                str(user_id),
                user.name,
                tweet_type,
                tweet.text,
                tweet_url,
                ref_url,
                ref_text,
                # tweet.public_metrics["like_count"],
                # tweet.public_metrics["retweet_count"],
                # tweet.public_metrics["quote_count"],
            ])
        # #取り出したデータをpandasのDataFrameに変換
        # #CSVファイルに出力するときの列の名前を定義
        labels = [
            'ツイートID',
            'ツイート時刻',
            'ユーザID',
            'ユーザ名',
            '区分',
            'ツイート本文',
            'ツイートURL',
            '参照URL',
            '参照テキスト',
            # 'いいね数',
            # 'リツイート数',
            # '引用数',
        ]
        # #tw_dataのリストをpandasのDataFrameに変換
        df = pd.DataFrame(tw_data, columns=labels)
        file_name = f"{user.name}_tw_data.csv"
        #ret_df = add_dataframe_to_csv(df, file_dir, file_name, dtype={'ツイートID': str, 'ユーザID': str})
        ret_df = add_dataframe_to_gspread(df, sheet_id=GSPREAD_SHEET_ID_TWITTER, sheet_name=user.name, type="all")

        if len(ret_df) > 0:
            # print("pass check ")
            up_list = ret_df.values.tolist()
            # # notionへアップロード
            upload_tweet_to_notion(up_list)
