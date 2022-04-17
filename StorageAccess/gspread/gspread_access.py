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
SCOPES = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

# SHEET_GID = os.environ['SHEET_GID']
SHEET_PROJECT_ID = os.environ['SHEET_PROJECT_ID']
SHEET_PRIVATE_KEY_ID = os.environ['SHEET_PRIVATE_KEY_ID']
# SHEET_PRIVATE_KEY = os.environ['SHEET_PRIVATE_KEY']
SHEET_CLIENT_EMAIL = os.environ['SHEET_CLIENT_EMAIL']
SHEET_CLIENT_ID = os.environ['SHEET_CLIENT_ID']
SHEET_CLIENT_X509_CERT_URL = os.environ['SHEET_CLIENT_X509_CERT_URL']

# 書き込み用のcredential.jsonファイル作成
credential_temp_file_name = 'StorageAccess/gspread/template_polar-city-346913-a99649e1f80b.json'
credential_file_name = 'polar-city-346913-a99649e1f80b.json'


def prepare_access(sheet_id):
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
    workbook = client.open_by_key(sheet_id)

    os.remove(credential_file_name)
    return workbook


def create_sheet_request_to_gspread(wb, name):
    for sheet in wb.worksheets():
        if sheet.title == name:
            ws = sheet
            ret = True
            break
    else:
        ws = wb.add_worksheet(title=name, rows=1, cols=1)
        ret = False
    return ws, ret


def add_dataframe_to_gspread(df, sheet_id, sheet_name, type_="all"):
    workbook = prepare_access(sheet_id)
    worksheet, exist = create_sheet_request_to_gspread(workbook, sheet_name)

    if exist:
        read_df = get_as_dataframe(worksheet, skiprows=0, header=0, index_col=0)
        read_df = read_df.dropna(how='all').dropna(how='all', axis=1)
        con_df = pd.concat([df, read_df], axis=0)
        write_df = con_df.drop_duplicates().fillna(0)
        diff_df = df[~df.isin(read_df.to_dict(orient='list')).all(1)]
    else:
        write_df = df
        diff_df = df

    set_with_dataframe(worksheet, write_df.reset_index())

    if type_ == "diff":
        return diff_df
    else:
        return write_df

def read_df_from_gspread(sheet_id, sheet_name):
    workbook = prepare_access(sheet_id)
    for sheet in workbook.worksheets():
        if sheet.title == sheet_name:
            read_df = get_as_dataframe(sheet, skiprows=0, header=0, index_col=0)
            read_df = read_df.dropna(how='all').dropna(how='all', axis=1)
            break

    return read_df

