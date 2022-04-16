import pandas as pd
import os
import json

import gspread
from gspread_formatting import *
from gspread_formatting.dataframe import format_with_dataframe, BasicFormatter
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

#
# Google Spread Sheet
#
SHEET_GID = "1Akfq8pR1avC_HgYxX67p9_vQaJv39wAaT76u_z6DO3w"
SCOPES = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

# SHEET_GID = os.environ['SHEET_GID']
SHEET_PROJECT_ID = os.environ['SHEET_PROJECT_ID']
SHEET_PRIVATE_KEY_ID = os.environ['SHEET_PRIVATE_KEY_ID']
# SHEET_PRIVATE_KEY = os.environ['SHEET_PRIVATE_KEY']
SHEET_CLIENT_EMAIL = os.environ['SHEET_CLIENT_EMAIL']
SHEET_CLIENT_ID = os.environ['SHEET_CLIENT_ID']
SHEET_CLIENT_X509_CERT_URL = os.environ['SHEET_CLIENT_X509_CERT_URL']


def prepare_access():
    # 書き込み用のcredential.jsonファイル作成
    credential_temp_file_name = 'ApiAccess/gspread/template_polar-city-346913-a99649e1f80b.json'
    credential_file_name = 'polar-city-346913-a99649e1f80b.json'

    with open(credential_temp_file_name, encoding='utf-8') as f:
        obj = json.load(f)

    obj["project_id"] = SHEET_PROJECT_ID
    obj["private_key_id"] = SHEET_PRIVATE_KEY_ID
    obj["client_email"] = SHEET_CLIENT_EMAIL
    obj["client_id"] = SHEET_CLIENT_ID
    obj["client_x509_cert_url"] = SHEET_CLIENT_X509_CERT_URL

    with open(credential_file_name, 'w', encoding='utf-8', newline='\n') as fp:
        json.dump(obj, fp)
    # credential.jsonファイルの情報を使ってGoogle Spread Sheetに認証する
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file_name, SCOPES)
    client = gspread.authorize(credentials)
    workbook = client.open_by_key(SHEET_GID)

    os.remove(credential_file_name)
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
