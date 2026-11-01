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
    monkeypatch.setattr(ingest, "fetch", lambda url: (200, "text/xml", feeds[url]))
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        yield s3, feeds


def counts(report):
    return {k: report[k] for k in ("nuevas", "repetidas", "actualizadas", "obsoletas")}


def state(s3):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=silver.STATE_KEY)["Body"].read())


def test_escenarios_bronze_silver(env):
    s3, feeds = env
    feeds["https://p.test"] = feed(item(1000001, "A"), item(1000002, "B"), item(1000003, "C", pub="ayer"))
    feeds["https://m.test"] = feed(item(1000002, "B"), item(1000004, "D", pub="2026-10-30T20:00:00-05:00"))
    assert ingest.ingest_all(SOURCES, "r1", s3=s3)["status"] == "complete"
    feeds["https://p.test"] = feed(item(1000001, "A editada"), item(1000005, "E"))
    ingest.ingest_all(SOURCES, "r2", s3=s3)

    r1 = silver.process_run("r1", s3=s3)
    assert (r1["rechazados"], r1["duplicados_en_lote"]) == (1, 1)
    assert counts(r1) == {"nuevas": 3, "repetidas": 0, "actualizadas": 0, "obsoletas": 0}
    r2 = silver.process_run("r2", s3=s3)
    assert counts(r2) == {"nuevas": 1, "repetidas": 2, "actualizadas": 1, "obsoletas": 0}

    assert silver.process_run("r1", s3=s3)["skipped"]
    forced = silver.process_run("r1", s3=s3, force=True)
    assert forced["obsoletas"] == 1 and forced["particiones_afectadas"] == []
    history = state(s3)["processed_runs"]["r1"]
    assert history["procesamientos"] == 2 and history["primero"]["nuevas"] == 3

    before = state(s3)
    silver.rebuild(s3=s3)
    after = state(s3)
    assert after["build_id"] != before["build_id"]
    rows = silver.load_current(s3, BUCKET, after)
    assert sorted(rows) == ["1000001", "1000002", "1000004", "1000005"]
    assert (rows["1000001"]["title"], rows["1000001"]["version"]) == ("A editada", 2)
    assert rows["1000002"]["sources_seen"] == ["eltiempo_mundo", "eltiempo_politica"]
