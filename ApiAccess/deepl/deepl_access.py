import deepl

auth_key = "c84de145-f8e6-0127-703e-1b8ca8ec429a:fx"


class DeeplAccess():

    def __init__(self):
        self.translator = deepl.Translator(auth_key)

    def translate_text(self, base_text, target_lang="JA"):
        result = self.translator.translate_text(base_text, target_lang=target_lang)
        translated_text = result.text

        return translated_text


if __name__ == "__main__":
    deepl = DeeplAccess()
    text = "Petrobras Jovem Aprendiz"
    trans_text = deepl.translate_text(text)
    print(trans_text)
    pass
