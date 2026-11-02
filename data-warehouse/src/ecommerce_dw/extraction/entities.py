"""Contrato de cada entidad extraida: fuente, llave, columnas y tipos del Parquet.

El esquema es explicito y fijo porque COPY de Redshift empareja las columnas de un Parquet por posicion,
no por nombre, y exige que la tabla tenga el mismo numero de columnas que el archivo. Inferir tipos fila
a fila tambien fallaria con columnas que en un lote vienen todas nulas.
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

UTC_TS = pa.timestamp("us", tz="UTC")   # instante: se guarda normalizado a UTC
LOCAL_TS = pa.timestamp("us")           # fecha y hora de negocio sin zona, igual que en la version local

# Columnas tecnicas al final de cada archivo, en este orden.
META = (
    ("_cdc_ts", UTC_TS),          # marca de cambio en la fuente, define la ventana del lote
    ("_batch_id", pa.string()),   # lote que extrajo la fila
    ("_extracted_at", UTC_TS),    # cuando se leyo la fila de la fuente
    ("_source", pa.string()),     # fuente.entidad, para trazabilidad en raw y quarantine
)


@dataclass(frozen=True)
class Entity:
    source: str
    name: str
    key: str
    fields: tuple[tuple[str, pa.DataType], ...]

    @property
    def id(self) -> str:
        return f"{self.source}.{self.name}"

    @property
    def columns(self) -> list[str]:
        return [name for name, _ in self.fields]

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([pa.field(name, kind) for name, kind in self.fields + META])


def _dec(precision: int) -> pa.DataType:
    return pa.decimal128(precision, 2)


ENTITIES = (
    Entity("mysql", "customers", "customer_id", (
        ("customer_id", pa.int64()), ("name", pa.string()), ("email", pa.string()), ("phone", pa.string()),
        ("city", pa.string()), ("department", pa.string()), ("registration_date", pa.date32()),
        ("updated_at", LOCAL_TS))),
    Entity("mysql", "products", "product_id", (
        ("product_id", pa.int64()), ("name", pa.string()), ("category", pa.string()), ("price", _dec(12)),
        ("description", pa.string()), ("is_active", pa.bool_()), ("updated_at", LOCAL_TS))),
    Entity("mysql", "orders", "order_id", (
        ("order_id", pa.int64()), ("customer_id", pa.int64()), ("order_date", LOCAL_TS), ("status", pa.string()),
        ("sales_channel", pa.string()), ("payment_method", pa.string()), ("total", _dec(14)))),
    Entity("mysql", "order_items", "order_item_id", (
        ("order_item_id", pa.int64()), ("order_id", pa.int64()), ("product_id", pa.int64()),
        ("quantity", pa.int32()), ("unit_price", _dec(12)), ("subtotal", _dec(14)))),
    Entity("documentdb", "reviews", "review_id", (
        ("review_id", pa.int64()), ("customer_id", pa.int64()), ("product_id", pa.int64()), ("rating", pa.int32()),
        ("reviewed_at", UTC_TS), ("document_json", pa.string()))),
    Entity("documentdb", "navigation_events", "event_id", (
        ("event_id", pa.int64()), ("event_type", pa.string()), ("occurred_at", UTC_TS), ("customer_id", pa.int64()),
        ("product_id", pa.int64()), ("sales_channel", pa.string()), ("document_json", pa.string()))),
)
SOURCES = tuple(dict.fromkeys(e.source for e in ENTITIES))


def by_source(source: str) -> list[Entity]:
    return [e for e in ENTITIES if e.source == source]
