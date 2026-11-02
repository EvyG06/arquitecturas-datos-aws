import pytest

boto3 = pytest.importorskip("boto3")
mock_aws = pytest.importorskip("moto").mock_aws

from ecommerce_dw.extraction import s3util
from ecommerce_dw.extraction.entities import ENTITIES
from ecommerce_dw.extraction.extract import STATE_KEY, batch_prefix
from ecommerce_dw.loading import ddl
from ecommerce_dw.loading.load import Loader
from ecommerce_dw.loading.redshift import DataApi, RedshiftError

BUCKET = "dw-test"
ROLE = "arn:aws:iam::1:role/arq-datos-redshift-s3"


class FakeDataApi:
    """Imita la Data API: guarda las sentencias y responde las consultas de lotes cargados."""

    def __init__(self, loaded=()):
        self.batches, self.loaded = [], set(loaded)

    def batch(self, sqls, name):
        self.batches.append((name, sqls))
        for sql in sqls:
            if sql.startswith("select 1 / 0"):
                raise RedshiftError("divide by zero")
        for sql in sqls:
            if sql.startswith(f"insert into {ddl.CONTROL_TABLE}"):
                self.loaded.add(sql.split("('")[1].split("'")[0])
        return {}

    def query(self, sql, name="consulta"):
        return [[b] for b in sorted(self.loaded)] if "distinct batch_id" in sql else []


def record(batch_id, seq, rows):
    return {"batch_id": batch_id, "seq": seq, "modo": "inicial" if seq == 1 else "incremental",
            "entidades": {e.id: {"filas": rows.get(e.id, 0)} for e in ENTITIES},
            "total_filas": sum(rows.values())}


@pytest.fixture
def s3(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("DW_FAULT", raising=False)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        lotes = [record("ext_01", 1, {"mysql.orders": 106, "documentdb.reviews": 158}),
                 record("ext_02", 2, {"mysql.orders": 3}),
                 record("ext_03", 3, {"mysql.orders": 6})]
        for r in lotes:
            s3util.put_json_once(client, BUCKET, f"{batch_prefix(r['batch_id'])}/batch.json", r)
        state = {"seq": 3, "historial": [{"seq": r["seq"], "batch_id": r["batch_id"]} for r in reversed(lotes)]}
        s3util.put_json_once(client, BUCKET, STATE_KEY, state)
        yield client


def test_ddl_mismo_orden_que_el_parquet_y_tipos():
    orders = ENTITIES[2]
    names = [n for n, _ in ddl.columns(orders)]
    assert names == [f.name for f in orders.schema]  # COPY de Parquet empareja por posicion
    types = dict(ddl.columns(orders))
    assert types["total"] == "decimal(14,2)" and types["order_date"] == "timestamp"
    assert dict(ddl.columns(ENTITIES[4]))["document_json"] == "varchar(65535)"
    assert ddl.create_raw_table(orders).rstrip().endswith("_loaded_at timestamp not null\n)")
    assert "_loaded_at" not in ddl.create_temp_table(orders)
    assert ddl.table(ENTITIES[5]) == "raw.mongo_navigation_events"  # mismo nombre que el source de dbt


def test_sentencias_de_un_lote(s3):
    loader = Loader(FakeDataApi(), s3, BUCKET, ROLE)
    sqls = loader.statements(record("ext_01", 1, {"mysql.orders": 106}))
    copy = [s for s in sqls if s.startswith("copy carga_mysql_orders")]
    assert copy == [f"copy carga_mysql_orders from 's3://{BUCKET}/_control/copy/batch_id=ext_01/mysql_orders.manifest' "
                    f"iam_role '{ROLE}' format as parquet manifest"]
    assert "select 1 / (case when (select count(*) from carga_mysql_orders) = 106 then 1 else 0 end) as conteo_ok" in sqls
    assert "delete from raw.mysql_orders where _batch_id = 'ext_01'" in sqls
    # las entidades sin filas no hacen COPY, solo limpian lo que hubiera de ese lote
    assert not any(s.startswith("copy carga_mysql_customers") for s in sqls)
    assert "delete from raw.mysql_customers where _batch_id = 'ext_01'" in sqls
    assert sqls[-1].startswith("insert into raw._cargas values ('ext_01', 1, 'inicial', 'mysql.customers', 0")
    full = loader.statements(record("x", 1, {e.id: 1 for e in ENTITIES}))
    assert len(full) == 32 <= 40  # limite de BatchExecuteStatement


def test_batch_id_inseguro_no_llega_al_sql(s3):
    with pytest.raises(ValueError, match="invalido"):
        Loader(FakeDataApi(), s3, BUCKET, ROLE).statements(record("x'; drop table raw.mysql_orders; --", 1, {}))


def test_carga_pendientes_en_orden_y_repeticion(s3):
    api = FakeDataApi(loaded={"ext_01"})
    loader = Loader(api, s3, BUCKET, ROLE)
    assert loader.run()["lotes"] == ["ext_02", "ext_03"]
    assert [name for name, _ in api.batches] == ["dw-esquema-raw", "dw-carga-ext_02", "dw-carga-ext_03"]
    assert loader.run()["resultado"] == "sin_pendientes"
    assert loader.run("ext_02")["resultado"] == "ya_cargado"
    assert loader.run("ext_02", reload=True)["lotes"] == ["ext_02"]
    with pytest.raises(ValueError, match="no esta confirmado"):
        loader.run("ext_99")


def test_fallo_a_mitad_no_registra_el_lote(s3, monkeypatch):
    api = FakeDataApi()
    loader = Loader(api, s3, BUCKET, ROLE)
    monkeypatch.setenv("DW_FAULT", "carga_a_mitad")
    with pytest.raises(RedshiftError):
        loader.run()
    assert api.loaded == set()  # la transaccion se revirtio entera
    monkeypatch.delenv("DW_FAULT")
    assert loader.run()["lotes"] == ["ext_01", "ext_02", "ext_03"]


class FakeClient:
    def __init__(self, statuses, error=None):
        self.statuses, self.error, self.calls = list(statuses), error, []

    def batch_execute_statement(self, **kw):
        self.calls.append(kw)
        return {"Id": "s1"}

    execute_statement = batch_execute_statement

    def describe_statement(self, Id):
        status = self.statuses.pop(0)
        d = {"Id": Id, "Status": status, "HasResultSet": True}
        if status == "FAILED":
            d["Error"] = "batch fallo"
            d["SubStatements"] = [{"Status": "FINISHED"}, {"Status": "FAILED", "QueryString": "copy x from 's3://b'",
                                                          "Error": "Spectrum Scan Error"}]
        return d

    def get_statement_result(self, Id, NextToken=None):
        if NextToken is None:
            return {"Records": [[{"stringValue": "a"}, {"longValue": 1}]], "NextToken": "t"}
        return {"Records": [[{"isNull": True}, {"booleanValue": True}]]}


def test_data_api_espera_reporta_error_y_pagina():
    client = FakeClient(["SUBMITTED", "STARTED", "FINISHED"])
    api = DataApi(client, "wg", "db", sleep=lambda s: None)
    api.batch(["select 1"], "prueba")
    assert client.calls[0]["WorkgroupName"] == "wg" and "SecretArn" not in client.calls[0]
    with pytest.raises(RedshiftError, match="(?s)copy x from.*Spectrum Scan Error"):
        DataApi(FakeClient(["FAILED"]), "wg", "db", sleep=lambda s: None).batch(["a", "b"], "x")
    assert DataApi(FakeClient(["FINISHED"]), "wg", "db").query("select") == [["a", 1], [None, True]]
    with pytest.raises(ValueError, match="40"):
        api.batch(["select 1"] * 41, "x")
