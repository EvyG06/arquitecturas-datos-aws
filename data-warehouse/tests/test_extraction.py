import io
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

boto3 = pytest.importorskip("boto3")
mock_aws = pytest.importorskip("moto").mock_aws
pq = pytest.importorskip("pyarrow.parquet")

from ecommerce_dw.extraction import s3util
from ecommerce_dw.extraction.entities import ENTITIES
from ecommerce_dw.extraction.extract import (STATE_KEY, ControlledFault, Extractor, StaleBatch, data_key,
                                              manifest_key, safe_batch_id)
from ecommerce_dw.extraction.readers import DocumentDBReader, MySQLReader

UTC = timezone.utc
T0 = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
BUCKET = "dw-test"


def customer(i, ts):
    return {"customer_id": i, "name": f"C{i}", "email": f"c{i}@x.co", "phone": "300", "city": "Medellin",
            "department": "Antioquia", "registration_date": date(2025, 1, 1),
            "updated_at": datetime(2025, 1, 1, 8, 0), "_cdc_ts": ts}


def product(i, ts):
    return {"product_id": i, "name": f"P{i}", "category": "Hogar", "price": Decimal("10.50"),
            "description": "d", "is_active": True, "updated_at": datetime(2025, 1, 1), "_cdc_ts": ts}


def order(i, ts):
    return {"order_id": i, "customer_id": 1, "order_date": datetime(2025, 2, 1, 9, 30), "status": "paid",
            "sales_channel": "web", "payment_method": "card", "total": Decimal("21.00"), "_cdc_ts": ts}


def item(i, ts):
    return {"order_item_id": i, "order_id": 1, "product_id": 1, "quantity": 2, "unit_price": Decimal("10.50"),
            "subtotal": Decimal("21.00"), "_cdc_ts": ts}


def review(i, ts):
    return {"review_id": i, "customer_id": 1, "product_id": 1, "rating": 5, "reviewed_at": T0,
            "document_json": "{}", "_cdc_ts": ts}


def event(i, ts):
    return {"event_id": i, "event_type": "busqueda", "occurred_at": T0, "customer_id": None, "product_id": None,
            "sales_channel": "web", "document_json": "{}", "_cdc_ts": ts}


class FakeReader:
    def __init__(self, source, rows, now):
        self.source, self.rows, self.now, self.reads = source, rows, now, []

    def clock(self):
        return self.now

    @contextmanager
    def snapshot(self):
        def fetch(entity, low, high):
            self.reads.append(entity.name)
            for row in self.rows.get(entity.name, []):
                ts = row["_cdc_ts"]
                if ts is None:
                    if low is None:
                        yield dict(row)
                elif ts <= high and (low is None or ts > low):
                    yield dict(row)
        yield fetch


@pytest.fixture
def world(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": "us-east-2", "AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("DW_FAULT", raising=False)
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        t = T0 - timedelta(hours=1)
        mysql = FakeReader("mysql", {"customers": [customer(1, t), customer(2, t)], "products": [product(1, t)],
                                     "orders": [order(1, t)], "order_items": [item(1, t)]}, T0)
        docdb = FakeReader("documentdb", {"reviews": [review(1, t), review(2, None)],
                                          "navigation_events": [event(1, t)]}, T0)
        ex = Extractor(s3, BUCKET, {"mysql": mysql, "documentdb": docdb}, overlap=timedelta(minutes=5),
                       now=lambda: T0)
        yield ex, mysql, docdb


def obj(ex, key):
    return ex.s3.head_object(Bucket=BUCKET, Key=key)


def table(ex, entity, batch):
    return pq.read_table(io.BytesIO(ex.s3.get_object(Bucket=BUCKET, Key=data_key(entity, batch))["Body"].read()))


def test_inicial_escribe_parquet_manifiestos_y_marca(world):
    ex, _, _ = world
    result = ex.run("lote1")
    assert result["resultado"] == "extraido" and result["modo"] == "inicial" and result["seq"] == 1
    counts = {e: s["filas"] for e, s in result["entidades"].items()}
    assert counts == {"mysql.customers": 2, "mysql.products": 1, "mysql.orders": 1, "mysql.order_items": 1,
                      "documentdb.reviews": 2, "documentdb.navigation_events": 1}
    for entity in ENTITIES:
        t = table(ex, entity, "lote1")
        assert t.schema.equals(entity.schema)  # mismo orden y tipos que esperara COPY
        assert set(t["_batch_id"].to_pylist()) == {"lote1"} and set(t["_source"].to_pylist()) == {entity.id}
        manifest = s3util.read_json(ex.s3, BUCKET, manifest_key(entity, "lote1"))
        size = obj(ex, data_key(entity, "lote1"))["ContentLength"]
        assert manifest["entries"] == [{"url": f"s3://{BUCKET}/{data_key(entity, 'lote1')}", "mandatory": True,
                                        "meta": {"content_length": size}}]
    state = s3util.read_json(ex.s3, BUCKET, STATE_KEY)
    assert state["seq"] == 1 and state["marcas"]["mysql.orders"] == T0.isoformat()
    assert [h["batch_id"] for h in state["historial"]] == ["lote1"]


def test_tipos_del_parquet(world):
    ex, _, _ = world
    ex.run("lote1")
    products = table(ex, ENTITIES[1], "lote1").to_pylist()[0]
    assert products["price"] == Decimal("10.50") and products["is_active"] is True
    assert products["_cdc_ts"].tzinfo is not None and products["updated_at"].tzinfo is None
    reviews = table(ex, ENTITIES[4], "lote1").to_pylist()
    assert [r["_cdc_ts"] is None for r in reviews] == [False, True]  # documento anterior a la marca CDC


def test_incremental_trae_solo_cambios_y_cuenta_solape(world):
    ex, mysql, docdb = world
    ex.run("lote1")
    mysql.rows["orders"].append(order(2, T0 + timedelta(minutes=10)))
    mysql.rows["customers"][0]["_cdc_ts"] = T0 - timedelta(minutes=2)  # escrito justo antes del corte
    mysql.now = docdb.now = T0 + timedelta(minutes=30)
    result = ex.run("lote2")
    assert result["modo"] == "incremental" and result["seq"] == 2
    e = result["entidades"]
    assert e["mysql.orders"]["filas"] == 1 and e["mysql.orders"]["repetidas_por_solape"] == 0
    # La fila con _cdc_ts dentro del solape de 5 minutos se vuelve a traer y queda contada como repetida.
    assert e["mysql.customers"]["filas"] == 1 and e["mysql.customers"]["repetidas_por_solape"] == 1
    assert e["documentdb.reviews"]["filas"] == 0  # el documento sin _cdc_ts solo entra en la inicial
    assert e["mysql.orders"]["desde"] == (T0 - timedelta(minutes=5)).isoformat()
    assert s3util.read_json(ex.s3, BUCKET, STATE_KEY)["marcas"]["mysql.orders"] == mysql.now.isoformat()


def test_repetir_lote_confirmado_no_consulta_ni_reescribe(world):
    ex, mysql, _ = world
    ex.run("lote1")
    etags = {e.id: obj(ex, data_key(e, "lote1"))["ETag"] for e in ENTITIES}
    state_etag = obj(ex, STATE_KEY)["ETag"]
    mysql.reads.clear()
    assert ex.run("lote1")["resultado"] == "ya_extraido"
    assert mysql.reads == []
    assert {e.id: obj(ex, data_key(e, "lote1"))["ETag"] for e in ENTITIES} == etags
    assert obj(ex, STATE_KEY)["ETag"] == state_etag


def test_fallo_a_mitad_y_reintento_sin_duplicar(world, monkeypatch):
    ex, mysql, docdb = world
    monkeypatch.setenv("DW_FAULT", "extraccion_a_mitad")
    with pytest.raises(ControlledFault):
        ex.run("lote1")
    mysql_etag = obj(ex, data_key(ENTITIES[0], "lote1"))["ETag"]
    assert s3util.head(ex.s3, BUCKET, data_key(ENTITIES[4], "lote1")) is None
    assert s3util.read_json(ex.s3, BUCKET, STATE_KEY) is None  # la marca no avanzo

    monkeypatch.delenv("DW_FAULT")
    mysql.rows["customers"].append(customer(3, T0 + timedelta(minutes=1)))  # llega despues del corte del plan
    mysql.now = docdb.now = T0 + timedelta(hours=1)  # el reloj avanza, pero la ventana ya estaba fijada
    mysql.reads.clear()
    result = ex.run("lote1")
    assert mysql.reads == []  # mysql ya estaba completo: no se vuelve a leer
    assert obj(ex, data_key(ENTITIES[0], "lote1"))["ETag"] == mysql_etag
    assert result["entidades"]["mysql.customers"]["filas"] == 2 and result["entidades"]["documentdb.reviews"]["filas"] == 2
    state = s3util.read_json(ex.s3, BUCKET, STATE_KEY)
    assert state["seq"] == 1 and state["marcas"]["documentdb.reviews"] == T0.isoformat()
    # El cliente que llego tarde lo trae el siguiente lote.
    assert ex.run("lote2")["entidades"]["mysql.customers"]["filas"] == 1


def test_fallo_antes_de_confirmar(world, monkeypatch):
    ex, mysql, _ = world
    monkeypatch.setenv("DW_FAULT", "extraccion_antes_de_confirmar")
    with pytest.raises(ControlledFault):
        ex.run("lote1")
    assert s3util.read_json(ex.s3, BUCKET, "_control/extraction/batch_id=lote1/batch.json")["seq"] == 1
    assert s3util.read_json(ex.s3, BUCKET, STATE_KEY) is None
    monkeypatch.delenv("DW_FAULT")
    mysql.reads.clear()
    assert ex.run("lote1")["resultado"] == "extraido" and mysql.reads == []
    assert s3util.read_json(ex.s3, BUCKET, STATE_KEY)["seq"] == 1


def test_lote_obsoleto_no_se_confirma(world, monkeypatch):
    ex, mysql, docdb = world
    ex.run("lote1")
    monkeypatch.setenv("DW_FAULT", "extraccion_a_mitad")
    with pytest.raises(ControlledFault):
        ex.run("lote_a")
    monkeypatch.delenv("DW_FAULT")
    mysql.now = docdb.now = T0 + timedelta(hours=1)
    ex.run("lote_b")
    with pytest.raises(StaleBatch, match="seq=1.*seq=2"):
        ex.run("lote_a")
    assert [h["batch_id"] for h in s3util.read_json(ex.s3, BUCKET, STATE_KEY)["historial"]] == ["lote1", "lote_b"]


def test_incremental_sin_inicial_y_batch_id_invalido(world):
    ex, _, _ = world
    with pytest.raises(ValueError, match="inicial"):
        ex.run("lote1", "incremental")
    with pytest.raises(ValueError, match="invalido"):
        ex.run("x/../y")
    assert safe_batch_id("manual__2026-11-02T19:38:59.1+00:00") == "manual__2026-11-02T19_38_59.1_00_00"


def test_mysql_lee_en_snapshot_consistente_y_utc():
    executed = []

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, args=None): executed.append((sql, args))
        def __iter__(self):
            return iter([{"product_id": 1, "is_active": 1, "_cdc_ts": datetime(2026, 1, 1)}])

    class Conn:
        def cursor(self): return Cursor()
        def rollback(self): executed.append(("ROLLBACK", None))
        def close(self): executed.append(("CLOSE", None))

    reader = MySQLReader(connect=Conn)
    with reader.snapshot() as fetch:
        rows = list(fetch(ENTITIES[1], T0 - timedelta(hours=1), T0))
    assert executed[0][0] == "SET time_zone = '+00:00'"
    assert executed[1][0] == "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"
    sql, args = executed[2]
    assert "WHERE _cdc_ts <= %s AND _cdc_ts > %s ORDER BY product_id" in sql
    assert args == [datetime(2026, 11, 2, 12, 0), datetime(2026, 11, 2, 11, 0)]
    assert rows[0]["is_active"] is True and rows[0]["_cdc_ts"].tzinfo == UTC
    assert [e[0] for e in executed[-2:]] == ["ROLLBACK", "CLOSE"]


def test_documentdb_ventana_y_aplanado():
    assert DocumentDBReader.window_filter(T0, T0) == {"_cdc_ts": {"$gt": T0, "$lte": T0}}
    assert "$or" in DocumentDBReader.window_filter(None, T0)
    doc = {"_id": "review-1", "review_id": 1, "customer": {"customer_id": 7}, "product": None, "rating": 4,
           "reviewed_at": T0, "_cdc_ts": T0}

    class Cursor(list):
        def sort(self, *a): return self
        def batch_size(self, n): return self

    class Collection:
        def find(self, flt):
            self.flt = flt
            return Cursor([doc])

    class Db:
        def get_collection(self, name, read_preference):
            assert read_preference.name == "Primary"
            return Collection()

    rows = list(DocumentDBReader._rows(Db(), ENTITIES[4], None, T0))
    assert rows[0]["customer_id"] == 7 and rows[0]["product_id"] is None and rows[0]["_cdc_ts"] == T0
    assert '"review_id": 1' in rows[0]["document_json"]
