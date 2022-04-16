import pandas as pd
import os

pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 30)

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

    with open(file_path, mode="w") as f:
        write_df.to_csv(f)

    return diff_df