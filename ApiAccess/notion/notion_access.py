from pprint import pprint

from notion_client import Client
import sys
from ApiAccess.localfile.file_access import *
from ApiAccess.translate.translate_access import *

# Notion API : https://developers.notion.com/
# Notion SDK : https://github.com/ramnes/notion-sdk-py
# [FYI] notion-py : https://github.com/jamalex/notion-py

DIRECTORY_PATH = "/Users/daiki/work/statistics/"
TAG_CSV_FILE = DIRECTORY_PATH + "notion_tags.csv"

# TREND_DATABASE_ID = "60fe6aa54eeb4bcfbdf09ad2be560d0f"
TREND_DATABASE_ID = os.environ['TREND_DATABASE_ID']
# TWEET_DATABASE_ID = "82588818307849b49a2a84d3242e8622"
TWEET_DATABASE_ID = os.environ['TWEET_DATABASE_ID']
# TECH_ARTICLE_DATABASE_ID = "fc2eb549d6c64b0695704a36f38b44a3"
TECH_ARTICLE_DATABASE_ID = os.environ['TECH_ARTICLE_DATABASE_ID']
SECRET_KEY = os.environ['NOTION_SECRET_KEY']

notion = Client(auth=SECRET_KEY)

#
#  Database Endpoint
#
def retrieve_database(database_id=TREND_DATABASE_ID):
    retrieve_database_response = notion.databases.retrieve(database_id=database_id)

    return retrieve_database_response


# query object reference
# https://developers.notion.com/reference/post-database-query
def query_database_record(tag, date):
    query_json = {
        'database_id': TREND_DATABASE_ID,  # データベースID
        'filter': {
            'and': [
                {
                    'property': 'Tag',
                    'multi_select': {
                        'contains': tag,
                    },
                    # },{
                    #    'property': 'Date',
                    #     'date': {
                    #         'equals': date,
                    #     }
                }]
        },
        # 'sorts': [{
        #     'property': 'date',
        #     'direction': 'descending',
        # }, ]
    }

    db = notion.databases.query(**query_json)
    return db


def get_users_info():
    list_users_response = notion.users.list()

    pprint(list_users_response)


def search_keyword(keyword):
    search_word_response = notion.search(query=keyword, )

    pprint(search_word_response)


def get_database_list():
    database_list_response = notion.databases.list()
    print(database_list_response.items())


#
#  Page Endpoint
#
def create_page(json, db_id):
    create_page_response = notion.pages.create(
        **{
            'parent': {'database_id': db_id},  # データベースID
            'properties': json['properties']  # ここにカラム名と値を記載
        }
    )

    return create_page_response


def retrieve_page(page_id):
    retrieve_page_response = notion.pages.retrieve(page_id)

    print(retrieve_page_response)


def update_page(page_id):
    update_page_response = notion.pages.update(page_id)

    return update_page_response


#
#  Block Endpoint
#
def get_block_list(block_id):
    get_block_list_response = notion.blocks.children.list(block_id)

    pprint(get_block_list_response)


# Block object reference
# https://developers.notion.com/reference/block
def append_block(page_id, blocks):
    append_block_response = notion.blocks.children.append(block_id=page_id,
                                                          children=blocks)
    return append_block_response


def make_article_page_json(dict):
    ret_json = {
        'properties': {'タグ': {'multi_select': [
            {'name': tag} for tag in dict["tag"]
        ],
            'type': 'multi_select'},
            'タイトル': {'title': [{'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                                'href': None,
                                'plain_text': dict["title"],
                                'text': {'content': dict["title"],
                                         'link': {'type': 'url',
                                                  'url': dict["url"]},
                                         },
                                'type': 'text'}],
                     'type': 'title'},
            'ライター': {'type': 'rich_text',
                     'rich_text': [{'annotations': {'bold': False,
                                                    'code': False,
                                                    'color': 'default',
                                                    'italic': False,
                                                    'strikethrough': False,
                                                    'underline': False},
                                    'href': None,
                                    'plain_text': dict["writer"],
                                    'text': {'content': dict["writer"],
                                             'link': None},
                                    'type': 'text'}], },

            'サマリー': {'type': 'rich_text',
                     'rich_text': [{'annotations': {'bold': False,
                                                    'code': False,
                                                    'color': 'default',
                                                    'italic': False,
                                                    'strikethrough': False,
                                                    'underline': False},
                                    'href': None,
                                    'plain_text': dict["summary"],
                                    'text': {'content': dict["summary"],
                                             'link': None},
                                    'type': 'text'}], },
            'サイト': {'rich_text': [{'annotations': {'bold': False,
                                                   'code': False,
                                                   'color': 'default',
                                                   'italic': False,
                                                   'strikethrough': False,
                                                   'underline': False},
                                   'href': None,
                                   'plain_text': dict["site"],
                                   'text': {'content': dict["site"],
                                            'link': {'type': 'url',
                                                     'url': dict["site_url"]},
                                            },
                                   'type': 'text'}],
                    'type': 'rich_text'},
            '掲載日時': {'date': {'end': None,
                              'start': dict["date"],
                              'time_zone': "Asia/Tokyo"},
                     'type': 'date'},
            '注目': {'type': 'checkbox',
                   'checkbox': dict["pick_up"]},
        }
    }
    return ret_json


def make_twitter_page_json(dict):
    ret_json = {
        'properties': {
            'ツイートタイプ': {
                'type': 'select',
                'select': {'name': dict["tweet_type"]}, },
            'ユーザーID': {
                # 'id': 'KeQQ',
                # 'name': 'ユーザーID',
                'type': 'rich_text',
                'rich_text': [{'annotations': {'bold': False,
                                               'code': False,
                                               'color': 'default',
                                               'italic': False,
                                               'strikethrough': False,
                                               'underline': False},
                               'href': None,
                               'plain_text': dict["user_id"],
                               'text': {'content': dict["user_id"],
                                        'link': None},
                               'type': 'text'}], },
            'ツイートID': {'type': 'rich_text',
                       'rich_text': [{'annotations': {'bold': False,
                                                      'code': False,
                                                      'color': 'default',
                                                      'italic': False,
                                                      'strikethrough': False,
                                                      'underline': False},
                                      'href': None,
                                      'plain_text': dict["tweet_id"],
                                      'text': {'content': dict["tweet_id"],
                                               'link': None},
                                      'type': 'text'}], },
            'ツイート': {
                # 'id': 'wzze',
                # 'name': 'ツイート',
                'type': 'rich_text',
                'rich_text': [{'annotations': {'bold': False,
                                               'code': False,
                                               'color': 'default',
                                               'italic': False,
                                               'strikethrough': False,
                                               'underline': False},
                               'href': None,
                               'plain_text': dict["tweet"],
                               'text': {'content': dict["tweet"],
                                        'link': {'type': 'url',
                                                 'url': dict["tweet_url"]}},
                               'type': 'text'}], },
            'ツイート日時': {
                # 'id': 'xfzN',
                # 'name': 'ツイート日時',
                'type': 'date',
                'date': {'end': None,
                         'start': dict["date"],
                         'time_zone': None}, },
            'ユーザー名': {
                # 'id': 'title',
                # 'name': 'ユーザー名',
                'type': 'title',
                'title': [{'annotations': {'bold': False,
                                           'code': False,
                                           'color': 'default',
                                           'italic': False,
                                           'strikethrough': False,
                                           'underline': False},
                           'href': None,
                           'plain_text': dict["user_name"],
                           'text': {'content': dict["user_name"],
                                    'link': None},
                           'type': 'text'}], },
            '参照URL': {
                'type': 'rich_text',
                'rich_text': [{'annotations': {'bold': False,
                                               'code': False,
                                               'color': 'default',
                                               'italic': False,
                                               'strikethrough': False,
                                               'underline': False},
                               'href': None,
                               'plain_text': dict["ref_text"],
                               'text': {'content': dict["ref_text"],
                                        'link': {'type': 'url',
                                                 'url': dict["ref_url"]}},
                               'type': 'text'}],
            },
        }
    }
    return ret_json


def make_trend_page_json(dict):
    ret_json = {
        'properties': {
            'Tag': {'multi_select': [{'name': tag} for tag in dict["tag"]],
                    'type': 'multi_select'},
            'Translate': {
                    'type': 'select',
                    'select': {'name': dict["translate"]}, },
            'Name': {'title': [{'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                                'href': None,
                                'plain_text': dict["name"],
                                'text': {'content': dict["name"],
                                         'link': None},
                                'type': 'text'}],
                     'type': 'title'},
            'Date': {'date': {'end': None,
                              'start': dict["date"],
                              'time_zone': None},
                     'type': 'date'}}
    }
    return ret_json


def get_text_object(text_set, text_color="default"):
    ret_list = []
    for text, url in text_set.items():
        text_object_template = {
            "type": "text",
            "text": {
                "content": text,
                "link": {"type": "url",
                         "url": url},
            },
            "annotations": {
                "bold": False,
                "italic": False,
                "strikethrough": False,
                "underline": False,
                "code": False,
                "color": text_color,
            },
            "plain_text": text,
        }
        if url is None:
            del text_object_template["text"]["link"]

        ret_list.append(text_object_template)

    return ret_list


def get_block_object(page_id, type, text_set, color="default", text_color="default"):
    block_object = {
        "object": "block",
        "id": page_id,
        "has_children": False,
        "type": type,
        type: {
            "rich_text": [],
            "color": color,
            "children": [],
        },
    }
    block_object[type]["rich_text"] = get_text_object(text_set, text_color=text_color)

    if type == "paragraph":
        del block_object["has_children"]

    return block_object


def upload_tech_articles_to_notion(article_list):
    for article in article_list:
        article_json = make_article_page_json(article)
        create_page(article_json, TECH_ARTICLE_DATABASE_ID)

def get_translate_engine_type(engine_str):
    if engine_str == "deepl":
        engine = EngineType.deepl
    elif engine_str == "google":
        engine = EngineType.gtrans
    else:
        engine = EngineType.none
    return engine


# ページのタイトルと、更新する内容を受け取り、ページの生成と内容の反映を行う
# create_page()で、生成対象のdb_idを指定しない場合、TREND_DATABASE_IDの配下に作成する
def upload_trend_to_notion(page_title, json_data, translate_engine="off"):

    tr = TranslateFactory.create(get_translate_engine_type(translate_engine))
    tags = []
    tag_data = {}
    for idx, list in enumerate(json_data["rank_list"]):
        tags.append(list["word"].replace(",", " "))
        tag_data[list["word"]] = idx + 1

    t_year = f"@{json_data['date'][:4]}年"
    t_month = f"@{json_data['date'][4:6]}月"
    t_day = f"{json_data['date'][6:8]}日"

    tags.append(t_year)
    tag_data |= {t_year: 99}
    tags.append(t_month)
    tag_data |= {t_month: 99}
    tags.append(t_month + t_day)
    tag_data |= {t_month + t_day: 99}

    date = datetime.datetime.strptime(json_data["date"], "%Y%m%d%H%M")
    iso_date = date.isoformat()

    page_data = {'tag': tags,
                 'name': page_title,
                 'date': iso_date + "+09:00",
                 'translate': translate_engine
                 }

    ret_m_json = make_trend_page_json(page_data)
    ret_c_json = create_page(ret_m_json, TREND_DATABASE_ID)
    page_id = ret_c_json['id']

    head_type = "heading_1"
    for list in json_data["rank_list"]:
        rank = list["rank"]
        word = list["word"]
        if "japan" not in page_title:
            word += tr().translate_text(word)
        head = f"{rank} : {word}"

        blocks = get_block_object(page_id, head_type, {head: None}, color="blue")

        blocks["has_children"] = True
        for idx in range(len(list["articles"]) // 2):
            title = list["articles"][idx * 2]
            if "japan" not in page_title:
                title += tr().translate_text(title)
            url = list["articles"][idx * 2 + 1]
            article_block = get_block_object(page_id, "paragraph", {title: url})
            blocks[head_type]["children"].append(article_block)

        append_block(page_id, [blocks, ])


def upload_tweet_to_notion(tweets):
    for tweet in tweets:
        page_data = {'tweet_id': str(tweet[0]),
                     'tweet': tweet[5],
                     'date': f'{str(tweet[1]).replace("_", "T")}.000+09:00',
                     'tweet_type': tweet[4],
                     'user_name': tweet[3],
                     'user_id': str(tweet[2]),
                     'tweet_url': tweet[6],
                     'ref_url': tweet[7],
                     'ref_text': tweet[8],
                     }
        ret_m_json = make_twitter_page_json(page_data)

        ret_c_json = create_page(ret_m_json, TWEET_DATABASE_ID)


# Debug用
# res_dbから引数のタグを検出
# 結果をupdate_tag_countへ投げて結果をデバッグ用ファイルに保存
def dump_tags_file(res_db, tag):
    if len(res_db["results"]) == 0:
        return

    for i in range(len(res_db["results"])):
        property = res_db["results"][i]["properties"]
        tag_dic = {}
        for j in range(len(property["Tag"]["multi_select"])):
            tag_name = property["Tag"]["multi_select"][j]["name"]
            if tag_name == tag:
                if '@' in tag_name:
                    tag_dic[tag_name] = 99
                else:
                    tag_dic[tag_name] = j + 1

        date = property["Date"]["date"]["start"]
        name = property["Name"]["title"][0]["text"]["content"]

        tag_df = pd.DataFrame.from_dict(data={date: tag_dic}, orient="index")
        # add_dataframe_to_csv(tag_df, dir="./", file=f"temp_notion_tags_{name}.csv")
        # add_dataframe_to_gspread(tag_df, sheet=f"temp_{name}")
        update_tag_count({date: tag_dic}, f"./temporary_tags_list_{name}.csv")


# スクレイピングした時間と検出されたワードのカウントの組み合わせをcsvファイルに保存
def update_tag_count(tag_data, file=TAG_CSV_FILE):
    tag_df = pd.DataFrame.from_dict(data=tag_data, orient="index")

    if os.path.exists(file):
        base_df = pd.read_csv(file, index_col=0)
        tag_df = pd.concat([base_df, tag_df])
        tag_df = tag_df.groupby(level=0).sum()

    with open(file, mode="w") as f:
        tag_df.to_csv(f, header=f.tell() == 0)

    # df_upload_to_spread_sheet(tag_df)


# Debug用
# DBからクエリした情報のtag保存処理を叩く
def get_tag_count():
    # FIXME
    res_db = retrieve_database(database_id=TREND_DATABASE_ID)
    tag_list = {}
    date = "2022-04-10T17:00:00-18:00"
    for idx, option in enumerate(res_db["properties"]["Tag"]["multi_select"]["options"]):
        tag = option["name"]
        res_db = query_database_record(tag, date)
        dump_tags_file(res_db, tag)

        # with open("temp.json", "a") as f:
        #     json.dump(res_db, f, indent=4, ensure_ascii=False)
        # pprint(res_db)

        # tag_list[tag] = len(res_db["results"])

    # pprint(tag_list)


if __name__ == "__main__":
    retrieve_page("fa6a7162cf714ec88e6beeaecd741085")
    # get_database_list()
    # res_db = retrieve_database("82588818307849b49a2a84d3242e8622")
    # print(res_db)
    # tag_list = {}
    # date = "2022-04-10"
    # for idx, option in enumerate(res_db["properties"]["Tag"]["multi_select"]["options"]):
    #     tag = option["name"]
    #     print(idx, tag)
    #     res_db = query_database_record(tag, date)
    #     print(len(res_db["results"]))
    #     tag_list[tag] = len(res_db["results"])
    #
    # pprint(tag_list)

    args = sys.argv
    if "tag_count" in args:
        get_tag_count()

        # retrieve_database_response = notion.databases.retrieve(database_id="514fa2eb00754c34ab54d01fb0eb2950")
        # pprint(retrieve_database_response)
    if "tag_update" in args:
        # tag_data = {"2022-04-19T19:00:00": {"TagA": 1, "TagB":1, "TagC":1}}

        update_tag_count({"2022-04-19T19:00:00": {"TagA": 1, "TagB": 1, "TagC": 1}})

    if "dump_json" in args:
        dump_tags_file("./temp.json")
