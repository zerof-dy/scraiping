import nlplot
import pandas as pd
import plotly
import MeCab
import urllib
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from plotly.subplots import  make_subplots
from ApiAccess.gspread.gspread_access import *

XML_PATH = "/Users/daiki/Documents/3rdParty/wikiextractor/wikiextractor/text.xml"

# googleトレンドの情報を読み取って、それを視覚化する

class NlplotText():
    GOOGLE_TREND_SHEET_ID = os.environ["GSPREAD_SHEET_ID_GOOGLE_TREND"]

    def __init__(self):
        self.npt = None

    def prepare_df(self, df):
        word_list = []
        for idx, word in enumerate(df.columns):
            count_list = []
            for rank in japan_df[word]:
                if rank != 0:
                    count_list += [word for i in range(21 - int(rank))]
            if len(count_list) != 0:
                word_list.append(count_list)
        word_df = pd.DataFrame({"word": word_list})
        self.npt = nlplot.NLPlot(word_df, target_col="word")

    def mecab_text(self, text):
        tagger = MeCab.Tagger('-d /usr/local/lib/mecab/dic/ipadic/')
        node = tagger.parseToNode(text)
        wordlist = []

        while node:
            if node.feature.split(',')[0] == '名詞':
                wordlist.append(node.surface)
            elif node.feature.split(',')[0] == '形容詞':
                wordlist.append(node.surface)
            elif node.feature.split(',')[0] == '動詞':
                wordlist.append(node.surface)
            node = node.next
        return wordlist

    def set_stopwords(self):
        # Defined by SlpothLib
        slothlib_path = 'http://svn.sourceforge.jp/svnroot/slothlib/CSharp/Version1/SlothLib/NLP/Filter/StopWord/word/Japanese.txt'
        slothlib_file = urllib.request.urlopen(slothlib_path)
        slothlib_stopwords = [line.decode("utf-8").strip() for line in slothlib_file]
        slothlib_stopwords = [ss for ss in slothlib_stopwords if not ss==u'']

        return slothlib_stopwords

    def prepare_file(self, file_path):
        stream = []
        with open(file_path, mode="r") as f:
            for line in f:
                if '［＃' in line or re.search('^\n', line) is not None:
                    continue
                stream.append(line.rstrip())

        df = pd.DataFrame({"stream": stream})
        df["words"] = df["stream"].apply(self.mecab_text)
        self.npt = nlplot.NLPlot(df, target_col="words")

    def prepare_text(self, text):
        df = pd.DataFrame({"stream": [text]})
        df["words"] = df["stream"].apply(self.mecab_text)
        self.npt = nlplot.NLPlot(df, target_col="words")

    def prepare_xml(self, word):
        tree = ET.parse(XML_PATH)
        root = tree.getroot()
        result = tree.find(f"./doc[@title='{word}']")
        if result is not None:
            text = result.text
            self.prepare_text(text)
            return True
        return False
        # print(result)

    def make_ngram_barchart(self):
        stopwords = self.npt.get_stopword(top_n=0, min_freq=0)

        fig_word_gram_bc = self.npt.bar_ngram(title="test-gram",
                                             xaxis_label="count",
                                             yaxis_label="word",
                                             ngram=1,
                                             top_n=400,
                                             width=1500,
                                             height=7000,
                                             stopwords=stopwords,
                                             save=True)

    def make_ngram_treemap(self):
        stopwords = self.npt.get_stopword(top_n=0, min_freq=0)
        fig_word_gram_tm = self.npt.treemap(
            stopwords=stopwords,
            title='Tree of Most Common Words',
            ngram=1,
            top_n=400,
            width=1500,
            height=2000,
            verbose=True,
            save=True
        )

    def make_wordcloud(self, file_word):
        stopwords = self.npt.get_stopword(top_n=0, min_freq=0)
        self.npt.wordcloud(max_words=100,
                           max_font_size=100,
                           colormap='tab20_r',
                           stopwords=self.set_stopwords(),
                           save=True)
        fname = f"wordcloud_{file_word}.png"
        os.rename("./wordcloud.png", fname)

    def make_co_occurrence_network(self):
        stopwords = self.npt.get_stopword(top_n=0, min_freq=0)
        self.npt.build_graph(stopwords=stopwords, min_edge_frequency=100)
        self.npt.co_network(
            title='Co-occurrence network',
            save=True)

    def make_sunburst_chart(self):
        stopwords = self.npt.get_stopword(top_n=0, min_freq=0)
        self.npt.sunburst(title="sunburst chart",
                          colorscale=True)


if __name__ == "__main__":
    # googleトレンドの情報を読み取って、それを視覚化する
    nlplot_test = NlplotText()

    gs_access = GspreadAccess(NlplotText.GOOGLE_TREND_SHEET_ID)
    japan_df = gs_access.read_df_from_gspread("japan")

    # nlplot_test.prepare_df(japan_df)
    # nlplot_test.prepare_text("./rashomon.txt")
    # nlplot_test.make_ngram_barchart()
    # nlplot_test.make_ngram_treemap()
    words = ["鳥取", "山梨", "富山", "岡山"]
    for word in words:
        if nlplot_test.prepare_xml(word) == True:
            nlplot_test.make_wordcloud(word)
    # nlplot_test.make_co_occurrence_network()
    # nlplot_test.make_sunburst_chart()