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
