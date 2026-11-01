from dl.enrich import MAX_TEXT_BYTES, analyze, entity_rows, key_phrase_rows, precheck


def test_precheck():
    assert precheck("   ") == "texto_vacio"
    assert precheck("x" * (MAX_TEXT_BYTES + 1)) == "texto_demasiado_largo"
    assert precheck("Hola") is None


def test_estados_explicitos(fake_nlp):
    res = analyze(fake_nlp, {"h1": "", "h2": "EN hello world", "h3": "Petro visito Bogota", "h4": "x" * 6000})
    assert {h: r["status"] for h, r in res.items()} == {
        "h1": "texto_vacio", "h2": "idioma_no_soportado", "h3": "analizado", "h4": "texto_demasiado_largo"}
    assert res["h2"]["language"] == "en" and res["h3"]["language"] == "es"
    assert fake_nlp.calls["idioma"] == 2 and fake_nlp.calls["entidades"] == 1


def test_entidad_distinta_por_noticia():
    result = {"entities": [
        {"Type": "PERSON", "Text": "Gustavo  Petro", "Score": 0.91},
        {"Type": "PERSON", "Text": "gustavo petro", "Score": 0.97},
        {"Type": "LOCATION", "Text": "Bogotá", "Score": 0.99},
        {"Type": "DATE", "Text": "ayer", "Score": 0.99}], "key_phrases": []}
    rows = entity_rows({"news_id": "1", "text_hash": "h"}, result)
    assert [(r["entity_type"], r["entity_norm"], r["mentions"]) for r in rows] == [
        ("lugar", "bogotá", 1), ("persona", "gustavo petro", 2)]
    assert rows[1]["score"] == 0.97


def test_frase_clave_distinta():
    result = {"entities": [], "key_phrases": [
        {"Text": "La reforma tributaria", "Score": 0.9}, {"Text": "la  reforma tributaria", "Score": 0.95}]}
    rows = key_phrase_rows({"news_id": "1", "text_hash": "h"}, result)
    assert len(rows) == 1 and rows[0]["mentions"] == 2 and rows[0]["score"] == 0.95
