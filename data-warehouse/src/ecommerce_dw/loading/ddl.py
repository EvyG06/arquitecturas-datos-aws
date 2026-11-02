"""Tablas raw en Redshift derivadas del mismo contrato que escribe los Parquet (extraction/entities.py).

Asi el orden y el numero de columnas de la tabla temporal coinciden siempre con el archivo, que es lo que
COPY exige para Parquet. Las tablas conservan los nombres de la version local (raw.mysql_*, raw.mongo_*)
para no cambiar los sources de dbt. La columna _source dice la fuente real (documentdb.*).
"""
from __future__ import annotations

import pyarrow as pa

from ecommerce_dw.extraction.entities import ENTITIES, META, Entity

SCHEMA = "raw"
CONTROL_TABLE = f"{SCHEMA}._cargas"
_PREFIX = {"mysql": "mysql", "documentdb": "mongo"}


def table(entity: Entity) -> str:
    return f"{SCHEMA}.{_PREFIX[entity.source]}_{entity.name}"


def temp_table(entity: Entity) -> str:
    return f"carga_{_PREFIX[entity.source]}_{entity.name}"


def redshift_type(column: str, kind: pa.DataType) -> str:
    if pa.types.is_int64(kind):
        return "bigint"
    if pa.types.is_int32(kind):
        return "integer"
    if pa.types.is_boolean(kind):
        return "boolean"
    if pa.types.is_date32(kind):
        return "date"
    if pa.types.is_decimal(kind):
        return f"decimal({kind.precision},{kind.scale})"
    if pa.types.is_timestamp(kind):
        # Los instantes llegan normalizados a UTC: se guardan como TIMESTAMP en UTC y staging los tipa.
        return "timestamp"
    if pa.types.is_string(kind):
        # VARCHAR en Redshift se mide en bytes, no en caracteres: un texto en espanol con tildes ocupa mas.
        return "varchar(65535)" if column == "document_json" else "varchar(4096)"
    raise TypeError(f"Tipo sin equivalente en Redshift para {column}: {kind}")


def columns(entity: Entity) -> list[tuple[str, str]]:
    return [(name, redshift_type(name, kind)) for name, kind in entity.fields + META]


def create_raw_table(entity: Entity) -> str:
    cols = ",\n    ".join(f"{name} {kind}" for name, kind in columns(entity))
    return f"create table if not exists {table(entity)} (\n    {cols},\n    _loaded_at timestamp not null\n)"


def create_temp_table(entity: Entity) -> str:
    cols = ", ".join(f"{name} {kind}" for name, kind in columns(entity))
    return f"create temp table {temp_table(entity)} ({cols})"


CREATE_CONTROL = (f"create table if not exists {CONTROL_TABLE} (batch_id varchar(128) not null, "
                  "seq integer not null, modo varchar(16) not null, entidad varchar(64) not null, "
                  "filas bigint not null, cargado_en timestamp not null)")


def bootstrap() -> list[str]:
    return [f"create schema if not exists {SCHEMA}", CREATE_CONTROL] + [create_raw_table(e) for e in ENTITIES]
