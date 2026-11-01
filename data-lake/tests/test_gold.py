import pytest

from dl.gold import aggregate_day, reconcile

NEWS = [
    {"news_id": "1", "medio": "El Tiempo", "seccion": "Política"},
    {"news_id": "2", "medio": "El Tiempo", "seccion": "Política"},
    {"news_id": "3", "medio": "El Tiempo", "seccion": "Mundo"},
]


def ent(nid, etype, norm, mentions=1, score=0.9):
    return {"news_id": nid, "entity_type": etype, "entity_norm": norm, "entity_text": norm.title(),
            "mentions": mentions, "score": score}


def phrase(nid, norm, mentions=1):
    return {"news_id": nid, "phrase_norm": norm, "phrase_text": norm, "mentions": mentions, "score": 0.8}


def test_cuenta_noticias_distintas_no_menciones():
    entities = [ent("1", "persona", "petro", mentions=5), ent("2", "persona", "petro"), ent("3", "lugar", "lisboa")]
    gold = aggregate_day(NEWS, entities, [phrase("1", "reforma"), phrase("2", "reforma")])
    assert gold["news_daily"] == [{"medio": "El Tiempo", "seccion": "Mundo", "n_noticias": 1},
                                  {"medio": "El Tiempo", "seccion": "Política", "n_noticias": 2}]
    petro = [r for r in gold["entity_daily"] if r["entity_norm"] == "petro"][0]
    assert (petro["seccion"], petro["n_noticias"], petro["menciones"]) == ("Política", 2, 6)
    reforma = gold["key_phrase_daily"][0]
    assert (reforma["seccion"], reforma["n_noticias"]) == ("Política", 2)


def test_conciliacion_cuadra():
    entities = [ent("1", "persona", "petro"), ent("3", "lugar", "lisboa")]
    gold = aggregate_day(NEWS, entities, [])
    assert reconcile("d", NEWS, entities, [], gold) == {
        "noticias": 3, "pares_noticia_entidad": 2, "pares_noticia_frase": 0}


def test_conciliacion_detecta_descuadre():
    gold = aggregate_day(NEWS, [], [])
    gold["news_daily"][0]["n_noticias"] += 1
    with pytest.raises(ValueError, match="conciliacion"):
        reconcile("d", NEWS, [], [], gold)


def test_entidad_sin_noticia_falla():
    with pytest.raises(ValueError, match="sin noticia"):
        aggregate_day(NEWS, [ent("99", "persona", "fantasma")], [])
