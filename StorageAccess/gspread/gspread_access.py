import pandas as pd
import os
import json

import gspread
from gspread_formatting import *
from gspread_formatting.dataframe import format_with_dataframe, BasicFormatter
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#
# Google Spread Sheet
#
class GspreadAccess:
    CREDENTIAL_TEMP_FILE = 'StorageAccess/gspread/template_polar-city-346913-a99649e1f80b.json'
    CREDENTIAL_FILE = 'polar-city-346913-a99649e1f80b.json'
    SCOPES = ['https://spreadsheets.google.com/feeds',
              'https://www.googleapis.com/auth/drive']

    def __init__(self, sheet_id, ):
        self.workbook = None
        self.worksheet = None
        self.credentials = None
        self.sheet_id = sheet_id

        with open(GspreadAccess.CREDENTIAL_TEMP_FILE, encoding='utf-8') as f:
            obj = json.load(f)

        obj["project_id"] = os.environ['SHEET_PROJECT_ID']
        obj["private_key_id"] = os.environ['SHEET_PRIVATE_KEY_ID']
        obj["client_email"] = os.environ['SHEET_CLIENT_EMAIL']
        obj["client_id"] = os.environ['SHEET_CLIENT_ID']
        obj["client_x509_cert_url"] = os.environ['SHEET_CLIENT_X509_CERT_URL']

        with open(self.CREDENTIAL_FILE, 'w', encoding='utf-8', newline='\n') as fp:
            json.dump(obj, fp)

        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(GspreadAccess.CREDENTIAL_FILE,
                                                                            GspreadAccess.SCOPES)
        client = gspread.authorize(self.credentials)
        self.workbook = client.open_by_key(self.sheet_id)
        self.worksheet_list = self.workbook.worksheets()

        os.remove(self.CREDENTIAL_FILE)

    def create_sheet_request_to_gspread(self, sheet_name):
        for sheet in self.worksheet_list:
            if sheet_name == sheet.title:
                self.worksheet = sheet
                ret = True
                break
        else:
            self.worksheet = self.workbook.add_worksheet(title=sheet_name, rows=1, cols=1)
            self.worksheet_list = self.workbook.worksheets()
            ret = False

        return ret

    def add_dataframe_to_gspread(self, df, sheet_name):
        is_exist = self.create_sheet_request_to_gspread(sheet_name)
        if is_exist:
            read_df = get_as_dataframe(self.worksheet, skiprows=0, header=0, index_col=0)
            read_df = read_df.dropna(how='all').dropna(how='all', axis=1)
            con_df = pd.concat([df, read_df], axis=0)
            write_df = con_df.drop_duplicates().fillna(0)
            diff_df = df[~df.isin(read_df.to_dict(orient='list')).all(1)]
        else:
            write_df = df
            diff_df = df

        set_with_dataframe(self.worksheet, write_df.reset_index())
        return diff_df

    def read_df_from_gspread(self, sheet_name):
        for worksheet in self.worksheet_list:
            if sheet_name == worksheet.title:
                    read_df = get_as_dataframe(worksheet, skiprows=0, header=0, index_col=0)
                    read_df = read_df.dropna(how='all').dropna(how='all', axis=1)
                    break
        else:
            raise FileNotFoundError

        return read_df
