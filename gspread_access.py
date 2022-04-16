import pandas as pd
import os

import gspread
from gspread_formatting import *
from gspread_formatting.dataframe import format_with_dataframe, BasicFormatter
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

#
# Google Spread Sheet
#
SCOPES = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
SHEET_GID = os.environ['SHEET_GID']
SHEET_PROJECT_ID = os.environ['SHEET_PROJECT_ID']
SHEET_PRIVATE_KEY_ID = os.environ['SHEET_PRIVATE_KEY_ID']
SHEET_PRIVATE_KEY = os.environ['SHEET_PRIVATE_KEY']
SHEET_CLIENT_EMAIL = os.environ['SHEET_CLIENT_EMAIL']
SHEET_CLIENT_ID = os.environ['SHEET_CLIENT_ID']
SHEET_CLIENT_X509_CERT_URL = os.environ['SHEET_CLIENT_X509_CERT_URL']

def prepare_access():
    # 書き込み用のcredential.jsonファイル作成
    credential_file_name = 'credential.json'
    credential_file = open(credential_file_name, 'w')

    # template.txtと環境変数から取得した各種データを統合してcredential.jsonに書き込む
    template_file_name = 'template.txt'
    template_file = open(template_file_name)
    template_texts = template_file.read()
    formatted_text = template_texts.format(SHEET_PROJECT_ID, SHEET_PRIVATE_KEY_ID, SHEET_PRIVATE_KEY,
                                           SHEET_CLIENT_EMAIL, SHEET_CLIENT_ID, SHEET_CLIENT_X509_CERT_URL)
    credential_file.write(formatted_text)

    # credential.jsonファイルの情報を使ってGoogle Spread Sheetに認証する
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file_name, SCOPES)
    client = gspread.authorize(credentials)
    workbook = client.open_by_key(SHEET_GID)

    template_file.close()
    credential_file.close()

    return workbook


# def connect_to_gspread():
#     credentials = ServiceAccountCredentials.from_json_keyfile_name(jsonf, scopes)
#     gc = gspread.authorize(credentials)
#     workbook = gc.open_by_key(SHEET_GID)
#
#     return workbook

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


def add_dataframe_to_gspread(df, sheet, type="all"):
    workbook = prepare_access()
    worksheet, exist = create_sheet_request_to_gspread(workbook, sheet)

    if exist:
        read_df = get_as_dataframe(worksheet, skiprows=0, header=0, index_col=0)
        # df = pd.concat([read_df, df])
        # df = df.groupby(level=0).sum()

        if type == "all":
            write_df = pd.concat([df, read_df], axis=0)
            write_df = write_df.drop_duplicates().fillna(0)
        elif type == "diff":
            write_df = df[~df.isin(read_df.to_dict(orient='list')).all(1)]
        else:
            print("write type error")
            return None
    else:
        write_df = df

    set_with_dataframe(worksheet, write_df.reset_index())

    return write_df
