from datetime import date, datetime, timezone

from dl.silver import dedup_batch, merge

T1 = datetime(2026, 10, 31, 23, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
D1 = date(2026, 10, 31)
D2 = date(2026, 11, 1)


def rec(news_id, text_hash="h1", source="eltiempo_politica", pos=1, ingested=T1, pub=D1):
    return {"news_id": news_id, "text_hash": text_hash, "source_id": source, "medio": "El Tiempo",
            "seccion": source, "item_position": pos, "ingested_at": ingested, "pub_date": pub}


def batch(*records):
    unique, _ = dedup_batch(list(records))
    return unique


def test_duplicado_entre_feeds_del_mismo_lote():
    unique, dups = dedup_batch([rec("1", source="eltiempo_politica"), rec("1", source="eltiempo_mundo"), rec("2")])
    assert dups == 1 and set(unique) == {"1", "2"}
    assert unique["1"]["source_id"] == "eltiempo_mundo"
    assert unique["1"]["sources_seen"] == ["eltiempo_mundo", "eltiempo_politica"]


def test_nueva_repetida_actualizada_obsoleta():
    state = {}
    stats, touched = merge(state, batch(rec("1"), rec("2")), "r1")
    assert stats["nuevas"] == 2 and touched == {D1}

    stats, touched = merge(state, batch(rec("1", ingested=T2), rec("2", text_hash="h2", ingested=T2)), "r2")
    assert stats["repetidas"] == 1 and stats["actualizadas"] == 1
    assert state["2"]["version"] == 2
    assert state["2"]["first_seen_run_id"] == "r1" and state["2"]["content_run_id"] == "r2"

    stats, touched = merge(state, batch(rec("2", text_hash="h1", ingested=T1)), "r1")
    assert stats["obsoletas"] == 1 and touched == set() and state["2"]["text_hash"] == "h2"


def test_repetir_un_lote_no_cambia_nada():
    state = {}
    merge(state, batch(rec("1")), "r1")
    before = dict(state["1"])
    stats, touched = merge(state, batch(rec("1")), "r1")
    assert stats["repetidas"] == 1 and touched == set() and state["1"] == before


def test_cambio_de_fecha_reescribe_ambas_particiones():
    state = {}
    merge(state, batch(rec("1")), "r1")
    stats, touched = merge(state, batch(rec("1", text_hash="h2", ingested=T2, pub=D2)), "r2")
    assert stats["actualizadas"] == 1 and touched == {D1, D2}
    assert state["1"]["pub_date"] == D2
