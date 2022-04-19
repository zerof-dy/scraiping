import deepl
from googletrans import Translator
from enum import Enum

auth_key = "c84de145-f8e6-0127-703e-1b8ca8ec429a:fx"

class EngineType:
    deepl = 'deepl'
    gtrans = 'google'
    none = 'none'


class TranslateFactory():
    class_ = {}

    @classmethod
    def create(cls, engine):
        cls_ = cls.class_[engine]
        return cls_

    @classmethod
    def register(cls, engine):
        def wrapper(cls_):
            if not issubclass(cls_, Translate):
                raise TypeError
            cls.class_[engine] = cls_
            return cls_
        return wrapper


class Translate():
    def __init__(self, target_lang):
        self.target_lang = target_lang
        self.is_valid = False
        pass

    def translate_text(self, src_text, target_lang="ja"):
        return NotImplementedError


@TranslateFactory.register(EngineType.gtrans)
class GoogleTranslate(Translate):
    def __init__(self, target_lang="ja"):
        super().__init__(target_lang)
        self.tr = Translator()
        self.is_valid = True

    def translate_text(self, src_text=""):
        trans_text = self.tr.translate(src_text, dest=self.target_lang).text
        return f"\n [G翻訳] {trans_text}"


@TranslateFactory.register(EngineType.deepl)
class DeeplTranslate(Translate):
    def __init__(self, target_lang="JA"):
        super().__init__(target_lang)
        self.tr = deepl.Translator(auth_key)
        self.is_valid = True

    def translate_text(self, src_text=""):
        trans_text = self.tr.translate_text(src_text, target_lang=self.target_lang)
        return f"\n [Deepl翻訳] {trans_text}"


@TranslateFactory.register(EngineType.none)
class DummyTranslate(Translate):
    def translate_text(self, src_text=""):
        return " "


if __name__ == "__main__":
    d_translate = TranslateFactory.create(EngineType.deepl)
    g_translate = TranslateFactory.create('google')
    n_translate = TranslateFactory.create(EngineType.none)
    trans_text = g_translate().translate_text("hello")
    print(trans_text)



