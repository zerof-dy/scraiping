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


# SHEET_PROJECT_ID = "polar-city-346913"
# SHEET_PRIVATE_KEY_ID = "a99649e1f80b17624cb65fc3a5e3cfef5fa0e6bd"
# SHEET_PRIVATE_KEY = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDUUe3IA5hC+D+rtFxqR1O69qsUlAhqYI5Hxancs0d+vVm+ysj7JNMrV+jS0CcWmKX6ew9WV6xxTjwcEqBeAXXVDY93p56H+dFenyjBZcavE0AiGDln4fCyLiDQwI65vUXjfMtst78dpMLTHx5ltaqmXbBBf4U+XGSvs/QkCjfCyOl8PJGp8+7ZBtQUCua8JnDNFqAGJNFhc2Ya2SqOllPXWDuGyIXj1gFL5hyvw6EZpTxqUKJsjHR1ewIR0wg4FGjEidwAMHhTrLD8OCcxWr4DaBV3zbrA7HNi8tBu2BUGjFuFlt4r54DoAv66QkVN9D14rh7Kv7LVKAegMqEVogFHAgMBAAECggEADXNf8icReyij9qTsoopV6nHHC9IxqEo+qUWt9FPikyERcZis29riNTboAoxPoEsCX4MdSSCGcFUpp15Mt401zE4EHpX0C/Q5HdqiZ9d38KPHu9aMsyE+sOMNjtFWxBCSwA3Utf9RWwzoOhzDaGPLuvy+IPV1rlTcchR0l+CSgMNBGN5miPCQpQp0YzXrDVT7CPZ5ntWXVTGTGDwFNtoQ5GVdhvo2gXPtqzRPfHpCZFlyx5cm3q+eKQGS91/jd/b160MNoFZ+ya/RBmrj29TOozQ+R5igDJLxIW9tL/s+mVDvVQMIm72IHCuOq6rP1TKE/ZrL0U1Mfcw1PIoVhThhpQKBgQDv41J6KBSd+lBJwhzGjvFEqLj6JxwttxTE1P2gupQ2VsxgXXTo4bSudtT8FiBjsD+6+4J9YyJOJSKSCW3UtJ+Po/2pEaIk2GS79/rUlEq57kVbNPZp7sZTsEf7X8WBLEV6WAeBJseBPjqGO1l29fl+n3vQ3Iceq3xCTC2gw2JYOwKBgQDilJkwsd47TZygjwQSD8tKa5zdRhWmQhlOkYRYQGNuAuDuLsysKpU3I5TPXdVWBHWfITiks9egFW+XrDkqWxgpGJhk1NYhkTcUhTnndMnmlou21A4Z+6gW5gFvjW1oRrP6Ghq8vWLDvQARCazaOQu4mlZmjBUtqx+DjZb5+N52ZQKBgGEc0s/XF/mYUxZ4+bM8PNoBv9itKpArNMI2Q3Lk9YlF/DjqvKBmdwHF45Ul0/jnafVlSj8+ea3PBL7ACHF6qbm5tr/R/qUR4T3jNu0GF3ZLylIRD8x0uF861vkhnVAcvwe3SVp0e4GL0p9yuVYaX+Vt5O75x82XuJcSRcp2LoLvAoGABF+b6V21jW19FXhNOtAFO4WCPJ8qsc/azzJt3Io5oRj7IH3Uw4n64VqVd7826/cgEhdBEaTLB7MCOWpAnDmv6LMp1eBp1bII3yOHL5mIgCPtxHSpZQT/hJmh83ZGY1niBXTViUkai+6s/qcyJ/Ar8r8/5bDUg9ImxJJKFL8iBCUCgYEAvOcAvPcx1w4qOvdcAEmfnBoVdRiTMXPkg8KPyalHhMfINb6Lg6nuAZOFVq+mbDL38/1duAzOy2NnRp4nWqORsGzyMC/HBZaa0XmU+S/THC/r4c+qgwhy8lZTbMsmM7Eo8tp4Tc6/cEZFBYCf0NeMhNgHjoq58K/nhgrZEIfLnBQ="
# SHEET_CLIENT_EMAIL = "google-python@polar-city-346913.iam.gserviceaccount.com"
# SHEET_CLIENT_ID = "117071281330489057787"
# SHEET_CLIENT_X509_CERT_URL = "https://www.googleapis.com/robot/v1/metadata/x509/google-python%40polar-city-346913.iam.gserviceaccount.com"

def prepare_access():
    # 書き込み用のcredential.jsonファイル作成
    credential_temp_file_name = '___polar-city-346913-a99649e1f80b.json'
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
