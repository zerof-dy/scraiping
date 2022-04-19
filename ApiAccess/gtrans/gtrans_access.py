from googletrans import Translator

class GtransAccess():

    def __init__(self):
        self.tr = Translator()

    def translate_text(self, src_text, src_lang="en", dst_lang="ja"):


tr = Translator()
trans_text = tr.translate("hello", dest="ja")

print(trans_text.text)