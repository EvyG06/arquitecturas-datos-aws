import json

import pytest

boto3 = pytest.importorskip("boto3")
mock_aws = pytest.importorskip("moto").mock_aws

from ecommerce_dw import aws_conn
from ecommerce_dw.sources import generate_data
from ecommerce_dw.sources.create_schema import statements


@pytest.fixture
def aws(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
                 "DW_AWS": "1"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    aws_conn.params.cache_clear()
    with mock_aws():
        sm = boto3.client("secretsmanager")
        mysql_arn = sm.create_secret(Name="rds!db-x", SecretString=json.dumps({"username": "dw_admin", "password": "p@ss:/?#"}))["ARN"]
        docdb_arn = sm.create_secret(Name="rds!cluster-x", SecretString=json.dumps({"username": "dw_admin", "password": "otra/cl@ve"}))["ARN"]
        ssm = boto3.client("ssm")
        for name, value in {"mysql_host": "mysql.interno", "mysql_secret_arn": mysql_arn,
                            "docdb_host": "docdb.interno", "docdb_secret_arn": docdb_arn}.items():
            ssm.put_parameter(Name=f"/arq-datos/dw/{name}", Value=value, Type="String")
        yield
    aws_conn.params.cache_clear()


def test_mysql_con_tls_y_credenciales_del_secreto(aws, monkeypatch):
    captured = {}
    monkeypatch.setattr("pymysql.connect", lambda **kw: captured.update(kw) or "conexion")
    assert generate_data.connect_mysql() == "conexion"
    assert captured["host"] == "mysql.interno" and captured["user"] == "dw_admin"
    assert captured["password"] == "p@ss:/?#"
    assert captured["ssl"] == {"ca": aws_conn.CA_BUNDLE} and captured["autocommit"] is False


def test_documentdb_sin_escrituras_reintentables(aws, monkeypatch):
    captured = {}
    monkeypatch.setattr("pymongo.MongoClient", lambda **kw: captured.update(kw) or "cliente")
    assert aws_conn.mongo_client(tz_aware=True) == "cliente"
    assert captured["password"] == "otra/cl@ve" and captured["tls"] is True
    assert captured["retryWrites"] is False and captured["replicaSet"] == "rs0"
    assert captured["tlsCAFile"] == aws_conn.CA_BUNDLE and captured["tz_aware"] is True


def test_faltan_parametros(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}.items():
        monkeypatch.setenv(k, v)
    aws_conn.params.cache_clear()
    with mock_aws():
        with pytest.raises(RuntimeError, match="Faltan parametros"):
            aws_conn.params()
    aws_conn.params.cache_clear()


def test_validadores_compatibles_con_documentdb():
    def walk(node):
        if isinstance(node, dict):
            assert not isinstance(node.get("bsonType"), list), node
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(generate_data.REVIEW_VALIDATOR)
    walk(generate_data.EVENT_VALIDATOR)


def test_separa_el_esquema_sin_fragmentos_vacios():
    sql = "-- comentario\nCREATE TABLE a (x INT);\n\n-- solo comentario\n;\nCREATE TABLE b (y INT);\n"
    assert [s.splitlines()[-1] for s in statements(sql)] == ["CREATE TABLE a (x INT)", "CREATE TABLE b (y INT)"]


def test_esquema_real_tiene_cuatro_tablas():
    from ecommerce_dw.sources.create_schema import SCHEMA
    assert len(statements(SCHEMA.read_text(encoding="utf-8"))) == 4


def test_lector_usa_su_propio_secreto(aws, monkeypatch):
    sm = boto3.client("secretsmanager")
    arn = sm.create_secret(Name="lector", SecretString=json.dumps({"username": "etl_lector", "password": "abc123"}))["ARN"]
    boto3.client("ssm").put_parameter(Name="/arq-datos/dw/mysql_reader_secret_arn", Value=arn, Type="String")
    aws_conn.params.cache_clear()
    captured = {}
    monkeypatch.setattr("pymysql.connect", lambda **kw: captured.update(kw) or "conexion")
    aws_conn.mysql_connect(lector=True)
    assert (captured["user"], captured["password"]) == ("etl_lector", "abc123")


def test_lector_sin_parametro_falla_claro(aws):
    with pytest.raises(RuntimeError, match="docdb_reader_secret_arn"):
        aws_conn.mongo_client(lector=True)


def test_marca_cdc_solo_en_aws(monkeypatch):
    docs = [{"_id": "a"}, {"_id": "b"}]
    monkeypatch.delenv("DW_AWS", raising=False)
    assert all("_cdc_ts" not in d for d in aws_conn.cdc_stamp(docs))
    monkeypatch.setenv("DW_AWS", "1")
    stamped = aws_conn.cdc_stamp(docs)
    assert stamped[0]["_cdc_ts"] == stamped[1]["_cdc_ts"] and stamped[0]["_cdc_ts"].tzinfo is not None


def test_columna_cdc_idempotente():
    from ecommerce_dw.sources.cdc import add_cdc_columns

    class Cursor:
        def __init__(self, existing):
            self.existing, self.ddl, self._last = existing, [], None

        def execute(self, sql, args=None):
            if sql.startswith("SELECT"):
                self._last = (1,) if args[0] in self.existing else (0,)
            else:
                self.ddl.append(sql)

        def fetchone(self):
            return self._last

    cursor = Cursor(existing={"customers"})
    result = add_cdc_columns(cursor)
    assert result["customers"] == "ya existia" and result["orders"] == "agregada"
    assert len(cursor.ddl) == 3 and all("ON UPDATE CURRENT_TIMESTAMP(6)" in d for d in cursor.ddl)
