import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from pytrends.request import TrendReq
from StorageAccess.gspread.gspread_access import *


GRAPH_SHEET_ID = os.environ["GSPREAD_SHEET_ID_GRAPH"]
gs_access = GspreadAccess(GRAPH_SHEET_ID)
conf_df = gs_access.read_df_from_gspread("conf")
view = conf_df["表現"]
word_list = conf_df["ワード"].values
time_frame = conf_df["期間"][0]
geo = conf_df["地域"][0]
resolution = conf_df["粒度"][0]

if view == "diff":
    pytrends = TrendReq(hl='ja-JP', tz=-540)
    pytrends.build_payload(kw_list=word_list, cat=0, timeframe=time_frame, geo=geo)
    df = pytrends.interest_by_region(resolution=resolution, inc_low_vol=True, inc_geo_code=False)

    FontPath = "../font/ipaexg.ttf"
    jpfont = FontProperties(fname=FontPath, size=14)
    fig = plt.figure(figsize=(10,20))
    plt.barh(df.index, df.iloc[:,0])
    plt.barh(df.index, df.iloc[:,1],left=df.iloc[:,0],)
    plt.yticks(range(len(df.index)), df.index, fontproperties=jpfont)
    plt.title("地域別Google検索ワード比率(1-week)", fontproperties=jpfont)
    plt.legend(labels=word_list, prop=jpfont)
    #plt.show()
    plt.savefig("test.jpg")