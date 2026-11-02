import json

import pytest

boto3 = pytest.importorskip("boto3")
mock_aws = pytest.importorskip("moto").mock_aws

from dl.preflight import check_lote


@pytest.fixture
def s3(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket="bucket-prueba", CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        yield client


def put_run(s3, lote, sources):
    s3.put_object(Bucket="bucket-prueba", Key=f"_control/ingestion/run_id={lote}/_run.json",
                  Body=json.dumps({"status": "partial", "sources": sources}))


def test_lote_existente(s3):
    put_run(s3, "r1", {"eltiempo_mundo": {"status": "ok"}, "eltiempo_politica": {"status": "http_error"}})
    assert check_lote(s3, "bucket-prueba", "r1") == "r1 estado=partial fuentes=eltiempo_mundo"


def test_lote_inexistente(s3):
    with pytest.raises(ValueError, match="no existe"):
        check_lote(s3, "bucket-prueba", "vacio")


def test_lote_sin_fuentes_validas(s3):
    put_run(s3, "r2", {"eltiempo_mundo": {"status": "network_error"}})
    with pytest.raises(ValueError, match="ninguna fuente"):
        check_lote(s3, "bucket-prueba", "r2")
