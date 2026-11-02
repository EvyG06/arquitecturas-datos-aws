"""Escenarios de Bronze y Silver contra un S3 simulado con moto."""
import json
from datetime import datetime, timedelta, timezone
from itertools import count

import pytest

boto3 = pytest.importorskip("boto3")
mock_aws = pytest.importorskip("moto").mock_aws

from dl import ingest, silver
from dl.config import Source

BUCKET = "bucket-prueba"
SOURCES = [Source("eltiempo_politica", "El Tiempo", "Política", "https://p.test"),
           Source("eltiempo_mundo", "El Tiempo", "Mundo", "https://m.test")]


def item(nid, title, pub="2026-10-31T10:00:00-05:00"):
    link = f"https://www.eltiempo.com/x/nota-{nid}"
    return (f"<item><title>{title}</title><link>{link}</link><guid>{link}</guid>"
            f"<pubDate>{pub}</pubDate><description>d</description></item>")


def feed(*items):
    body = "".join(items)
    return f'<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>t</title>{body}</channel></rss>'.encode()


@pytest.fixture
def env(monkeypatch):
    for k, v in {"DL_BUCKET": BUCKET, "AWS_DEFAULT_REGION": "us-east-2",
                 "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    ticks = count()
    base = datetime(2026, 10, 31, 23, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(ingest, "utc_now", lambda: base + timedelta(seconds=next(ticks)))
    feeds: dict[str, bytes] = {}
    monkeypatch.setattr(ingest, "fetch", lambda url: (feeds[url], "text/html", b"")
                        if isinstance(feeds[url], int) else (200, "text/xml", feeds[url]))
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        yield s3, feeds


def counts(report):
    return {k: report[k] for k in ("nuevas", "repetidas", "actualizadas", "obsoletas")}


def state(s3):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=silver.STATE_KEY)["Body"].read())


def test_escenarios_bronze_silver(env, fake_nlp):
    s3, feeds = env
    feeds["https://p.test"] = feed(item(1000001, "A"), item(1000002, "B"), item(1000003, "C", pub="ayer"))
    feeds["https://m.test"] = feed(item(1000002, "B"), item(1000004, "D", pub="2026-10-30T20:00:00-05:00"))
    assert ingest.ingest_all(SOURCES, "r1", s3=s3)["status"] == "complete"
    feeds["https://p.test"] = feed(item(1000001, "A editada"), item(1000005, "E"))
    ingest.ingest_all(SOURCES, "r2", s3=s3)

    r1 = silver.process_run("r1", s3=s3, nlp=fake_nlp)
    assert (r1["rechazados"], r1["duplicados_en_lote"]) == (1, 1)
    assert counts(r1) == {"nuevas": 3, "repetidas": 0, "actualizadas": 0, "obsoletas": 0}
    r2 = silver.process_run("r2", s3=s3, nlp=fake_nlp)
    assert counts(r2) == {"nuevas": 1, "repetidas": 2, "actualizadas": 1, "obsoletas": 0}
    assert r1["nlp"]["textos_enviados"] == 3
    assert r2["nlp"]["textos_enviados"] == 2 and r2["nlp"]["cache_hits"] == 1

    assert silver.process_run("r1", s3=s3, nlp=fake_nlp)["skipped"]
    forced = silver.process_run("r1", s3=s3, force=True, nlp=fake_nlp)
    assert forced["obsoletas"] == 1 and forced["particiones_afectadas"] == []
    history = state(s3)["processed_runs"]["r1"]
    assert history["procesamientos"] == 2 and history["primero"]["nuevas"] == 3

    calls_before_rebuild = dict(fake_nlp.calls)
    before = state(s3)
    silver.rebuild(s3=s3, nlp=fake_nlp)
    assert dict(fake_nlp.calls) == calls_before_rebuild
    after = state(s3)
    assert after["build_id"] != before["build_id"]
    rows = silver.load_current(s3, BUCKET, after)
    assert sorted(rows) == ["1000001", "1000002", "1000004", "1000005"]
    assert (rows["1000001"]["title"], rows["1000001"]["version"]) == ("A editada", 2)
    assert rows["1000002"]["sources_seen"] == ["eltiempo_mundo", "eltiempo_politica"]
    assert rows["1000001"]["nlp_status"] == "analizado" and rows["1000001"]["nlp_language"] == "es"
    assert set(after["datasets"]["news_entities"]) == set(after["datasets"]["news"])


def test_gold_es_incremental_e_idempotente(env, fake_nlp):
    from dl import gold
    s3, feeds = env
    feeds["https://p.test"] = feed(item(1000001, "Ana visita Lima"), item(1000002, "Luis en Quito",
                                                                           pub="2026-10-30T09:00:00-05:00"))
    feeds["https://m.test"] = feed(item(1000003, "Eva y Ana en Roma"))
    ingest.ingest_all(SOURCES, "r1", s3=s3)
    silver.process_run("r1", s3=s3, nlp=fake_nlp)

    first = gold.build(s3=s3)
    assert first["fechas_recalculadas"] == ["2026-10-30", "2026-10-31"]
    assert first["conciliacion"]["2026-10-31"]["noticias"] == 2
    assert gold.build(s3=s3)["fechas_recalculadas"] == []

    feeds["https://p.test"] = feed(item(1000004, "Nueva nota de Ana"))
    feeds["https://m.test"] = feed()
    ingest.ingest_all(SOURCES, "r2", s3=s3)
    silver.process_run("r2", s3=s3, nlp=fake_nlp)
    assert gold.build(s3=s3)["fechas_recalculadas"] == ["2026-10-31"]



def keys(s3, prefix):
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        out += [o["Key"] for o in page.get("Contents", [])]
    return out


def test_copia_controlada_fuente_caida_y_fallo_controlado(env, fake_nlp, monkeypatch):
    from dl import controlled
    from dl.config import ControlledFault
    s3, feeds = env
    feeds["https://p.test"] = feed(item(1000001, "Ana en Lima"), item(1000002, "Luis en Quito"))
    feeds["https://m.test"] = feed(item(1000003, "Eva en Roma"))
    ingest.ingest_all(SOURCES, "r1", s3=s3)
    silver.process_run("r1", s3=s3, nlp=fake_nlp)

    out = controlled.run_controlled(s3, BUCKET, SOURCES[0], "r1", "ctl")
    assert out["ingesta"]["status"] == "complete"
    meta_key = [k for k in keys(s3, "bronze/") if "run_id=ctl/" in k and k.endswith("_meta.json")][0]
    assert json.loads(s3.get_object(Bucket=BUCKET, Key=meta_key)["Body"].read())["origen"] == "copia_controlada"
    rep = silver.process_run("ctl", s3=s3, nlp=fake_nlp)
    assert (rep["items_recibidos"], rep["rechazados"], rep["duplicados_en_lote"]) == (6, 3, 1)
    assert counts(rep) == {"nuevas": 0, "repetidas": 1, "actualizadas": 1, "obsoletas": 0}
    assert rep["nlp"]["textos_enviados"] == 1
    assert keys(s3, "quarantine/silver/run_id=ctl/") == ["quarantine/silver/run_id=ctl/rechazos.jsonl"]

    feeds["https://p.test"] = feed(item(1000004, "Nueva de Ana"))
    feeds["https://m.test"] = 503
    assert ingest.ingest_all(SOURCES, "r2", s3=s3)["status"] == "partial"
    feeds["https://m.test"] = feed(item(1000005, "Otra de Eva"))
    assert ingest.ingest_all(SOURCES, "r2", attempt=2, s3=s3)["status"] == "complete"
    assert len([k for k in keys(s3, "bronze/") if "run_id=r2/" in k and k.endswith("feed.xml")]) == 2

    before = state(s3)
    monkeypatch.setenv("DL_FAULT", "silver_antes_de_publicar")
    with pytest.raises(ControlledFault):
        silver.process_run("r2", s3=s3, nlp=fake_nlp)
    assert state(s3) == before
    assert keys(s3, "_control/silver/run_id=r2/") == []
    written = set(keys(s3, "silver/"))
    monkeypatch.delenv("DL_FAULT")
    rep = silver.process_run("r2", s3=s3, nlp=fake_nlp)
    assert rep["nuevas"] == 2
    assert set(keys(s3, "silver/")) == written
    assert state(s3)["processed_runs"]["r2"]["procesamientos"] == 1
    assert len(keys(s3, "_control/silver/run_id=r2/")) == 1


def test_etapas_separadas_con_fallo_y_reintento(env, fake_nlp, monkeypatch):
    from dl.config import ControlledFault
    s3, feeds = env
    feeds["https://p.test"] = feed(item(1000001, "Ana en Lima"))
    feeds["https://m.test"] = feed(item(1000002, "Eva en Roma"))
    ingest.ingest_all(SOURCES, "r1", s3=s3)
    silver.process_run("r1", s3=s3, nlp=fake_nlp)
    feeds["https://p.test"] = feed(item(1000001, "Ana en Lima"), item(1000003, "Luis en Quito"))
    ingest.ingest_all(SOURCES, "r2", s3=s3)

    plan = silver.stage_prepare("r2", s3=s3)
    assert plan["stage"] == "preparado" and (plan["nuevas"], plan["repetidas"]) == (1, 2)
    assert state(s3)["processed_runs"].keys() == {"r1"}
    plan = silver.stage_enrich("r2", s3=s3, nlp=fake_nlp)
    assert plan["nlp"]["textos_enviados"] == 1

    monkeypatch.setenv("DL_FAULT", "silver_antes_de_publicar")
    with pytest.raises(ControlledFault):
        silver.stage_publish("r2", s3=s3)
    assert "r2" not in state(s3)["processed_runs"]
    monkeypatch.delenv("DL_FAULT")
    report = silver.stage_publish("r2", s3=s3)
    assert report["nuevas"] == 1 and report["noticias_vigentes_total"] == 3
    assert silver.stage_publish("r2", s3=s3)["stage"] == "publicado"
    assert state(s3)["processed_runs"]["r2"]["procesamientos"] == 1
    assert silver.stage_prepare("r2", s3=s3)["stage"] == "omitido"


def test_publicar_rechaza_plan_sobre_estado_cambiado(env, fake_nlp):
    from dl.s3util import ConcurrentUpdate
    s3, feeds = env
    feeds["https://p.test"] = feed(item(1000001, "Ana en Lima"))
    feeds["https://m.test"] = feed(item(1000002, "Eva en Roma"))
    ingest.ingest_all(SOURCES, "r1", s3=s3)
    silver.process_run("r1", s3=s3, nlp=fake_nlp)
    feeds["https://p.test"] = feed(item(1000003, "Luis en Quito"))
    ingest.ingest_all(SOURCES, "r2", s3=s3)
    feeds["https://p.test"] = feed(item(1000004, "Sara en Cali"))
    ingest.ingest_all(SOURCES, "r3", s3=s3)

    silver.stage_prepare("r2", s3=s3)
    silver.stage_enrich("r2", s3=s3, nlp=fake_nlp)
    silver.process_run("r3", s3=s3, nlp=fake_nlp)
    with pytest.raises(ConcurrentUpdate):
        silver.stage_publish("r2", s3=s3)
