import json

import pytest

boto3 = pytest.importorskip("boto3")
mock_aws = pytest.importorskip("moto").mock_aws

from ecommerce_dw.extraction.extract import STATE_KEY
from ecommerce_dw.pipeline import preflight


def test_reporta_todos_los_errores_y_sale_con_1(capsys):
    def falla():
        raise ConnectionError("timeout")
    rc = preflight.run_checks({"a": lambda: "bien", "b": falla, "c": lambda: "tambien"})
    out = capsys.readouterr().out
    assert rc == 1 and "OK     a" in out and "ERROR  b" in out and "timeout" in out and "OK     c" in out


def test_reproceso_no_consulta_las_fuentes():
    normal = preflight.build_checks(None, "b", None)
    reproceso = preflight.build_checks(None, "b", "ext_01")
    assert {"mysql", "documentdb"} <= set(normal) and "lote" not in normal
    assert "mysql" not in reproceso and "documentdb" not in reproceso and "lote" in reproceso


def test_reproceso_exige_lote_confirmado(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="dw-prueba", CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        s3.put_object(Bucket="dw-prueba", Key=STATE_KEY,
                      Body=json.dumps({"seq": 1, "historial": [{"seq": 1, "batch_id": "ext_01", "filas": 9}]}))
        assert "seq=1" in preflight.check_reprocess(s3, "dw-prueba", "ext_01")
        with pytest.raises(ValueError, match="no esta confirmado"):
            preflight.check_reprocess(s3, "dw-prueba", "vacio")
        with pytest.raises(ValueError, match="invalido"):
            preflight.check_reprocess(s3, "dw-prueba", "x; rm -rf /")
