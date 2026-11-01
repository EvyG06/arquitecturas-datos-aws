from collections import Counter

import pytest


class FakeComprehend:
    """Doble de prueba de Comprehend. Palabra con mayuscula inicial = PERSON, palabra larga = frase clave."""

    def __init__(self):
        self.calls = Counter()

    def batch_detect_dominant_language(self, TextList):
        self.calls["idioma"] += len(TextList)
        return {"ErrorList": [], "ResultList": [
            {"Index": i, "Languages": [{"LanguageCode": "en" if t.startswith("EN ") else "es", "Score": 0.99}]}
            for i, t in enumerate(TextList)]}

    def batch_detect_entities(self, TextList, LanguageCode):
        self.calls["entidades"] += len(TextList)
        results = []
        for i, t in enumerate(TextList):
            ents = [{"Type": "PERSON", "Text": w, "Score": 0.9} for w in t.replace(".", " ").split() if w[:1].isupper()]
            ents.append({"Type": "DATE", "Text": "hoy", "Score": 0.99})
            results.append({"Index": i, "Entities": ents})
        return {"ErrorList": [], "ResultList": results}

    def batch_detect_key_phrases(self, TextList, LanguageCode):
        self.calls["frases"] += len(TextList)
        return {"ErrorList": [], "ResultList": [
            {"Index": i, "KeyPhrases": [{"Text": w, "Score": 0.8} for w in t.replace(".", " ").split() if len(w) > 5]}
            for i, t in enumerate(TextList)]}


@pytest.fixture
def fake_nlp():
    return FakeComprehend()
