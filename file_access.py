import pandas as pd
import os

import gspread
from gspread_formatting import *
from gspread_formatting.dataframe import format_with_dataframe, BasicFormatter
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 30)

import pprint
# https://pypi.org/project/gspread-formatting/

#
# Common
#
def convert_date_to_tag_dict(date):
    t_year = f"@{date[:4]}年"
    t_month = f"@{date[4:6]}月"
    t_day = f"{date[6:8]}日"
    return {t_year: 99, t_month: 99, t_month + t_day: 99}

#
# CSV ファイル
#
DIRECTORY_PATH = "/Users/daiki/work/statistics/"
TAG_CSV_FILE = "notion_tags.csv"
def old_add_dataframe_to_csv(df, dir=DIRECTORY_PATH, file=TAG_CSV_FILE):
    file_path = dir + file
    if os.path.exists(file_path):
        base_df = pd.read_csv(file_path, index_col=0)

        df = pd.concat([base_df, df])
        df = df.groupby(level=0).sum()

    with open(file_path, mode="w") as f:
        df.to_csv(f, header=f.tell() == 0)

def add_dataframe_to_csv(df, dir=DIRECTORY_PATH, file=TAG_CSV_FILE, dtype={}, index=0):
    file_path = dir + file
    write_df = df
    diff_df = df
    if os.path.exists(file_path):
        read_df = pd.read_csv(file_path, index_col=index, dtype=dtype)
        write_df = pd.concat([df, read_df], axis=0)
        write_df = write_df.drop_duplicates().fillna(0)
        diff_df = df[~df.isin(read_df.to_dict(orient='list')).all(1)]

    # print("write : ", write_df)
    with open(file_path, mode="w") as f:
        write_df.to_csv(f)

    return diff_df

#
# def add_twitter_dataframe_to_csv(df, dir=DIRECTORY_PATH, file=TAG_CSV_FILE, dtype={}, index=0):
#     is_first = True
#     file_path = dir + file
#     up_df = df
#     # print("pass", up_df.values.tolist())
#     if os.path.exists(file_path):
#         base_df = pd.read_csv(file_path, index_col=index, dtype=dtype)
#         # print("read", base_df.values.tolist())
#         #pd.concat([df, base_df])
#         cut_df = pd.concat([df, base_df], axis=0)
#         cut_df.drop_duplicates(keep=False, inplace=True)
#         up_df = pd.concat([df, cut_df], axis=0)
#         up_df = up_df[up_df.duplicated()].fillna(0)
# #        up_df.drop_duplicates(inplace=True, keep=False)
#         # up_df = up_df.loc[:, ~up_df.columns.duplicated()]
#         #up_df.drop_duplicates(keep=False, inplace=True)
#         # base_df.set_index("ツイートID", drop=False, inplace=True)
#         # up_df = pd.concat([df, base_df.reindex(df.index)], axis=0, sort=False, join='outer')
#         # up_df = pd.concat([base_df, df]).fillna(0)
#         # up_df.drop_duplicates(inplace=True)
#         is_first = False
#         # print("concat", up_df.values.tolist())
#
#     with open(file_path, mode="a") as f:
#         up_df.to_csv(f, header=f.tell() == 0)
#
#     return up_df

#
# Google Spread Sheet
#
jsonf = "/Users/daiki/PycharmProjects/PythonLecture/scraping/Release/polar-city-346913-a99649e1f80b.json"
google_tag_spread_sheet_key = "1sB0r6YRe-wG6S_OuD9AzCfWEMq12KMPud-KHe56To4w"
scopes = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
def connect_to_gspread():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(jsonf, scopes)
    gc = gspread.authorize(credentials)
    workbook = gc.open_by_key(google_tag_spread_sheet_key)

    return workbook

def create_sheet_request_to_gspread(wb, name):

    for sheet in wb.worksheets():
        if sheet.title == name:
            ws = sheet
            ret = True
            break
    else:
        ws = wb.add_worksheet(title=name, rows=0, cols=0)
        ret = False
    return ws, ret


def add_dataframe_to_gspread(df, sheet):
    workbook = connect_to_gspread()
    worksheet, exist = create_sheet_request_to_gspread(workbook, sheet)

    if exist:
        read_df = get_as_dataframe(worksheet, skiprows=0, header=0, index_col=0)
        df = pd.concat([read_df, df])
        df = df.groupby(level=0).sum()

    formatter = BasicFormatter(
        header_background_color=Color(0, 0, 0),
        header_text_color=Color(1, 1, 1),
        # decimal_format='#,##0.00'
    )

    set_with_dataframe(worksheet, df.reset_index())
    # format_with_dataframe(worksheet,
    #                       df,
    #                       formatter,
    #                       include_index=True,
    #                       include_column_header=True)