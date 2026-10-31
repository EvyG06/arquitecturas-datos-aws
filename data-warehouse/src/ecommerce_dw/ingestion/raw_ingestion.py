"""Airflow's split extract/load helpers for the shared DuckDB warehouse."""

import csv
import fcntl
import os
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from pymongo import MongoClient

from .raw_sources import (
    BATCH_SIZE,
    MONGO_TABLES,
    MYSQL_TABLES,
    connect_mysql,
    flatten_mongo,
)

MYSQL_CSV_COLUMNS = {
    "customers": {"customer_id": "BIGINT", "name": "VARCHAR", "email": "VARCHAR", "phone": "VARCHAR", "city": "VARCHAR", "department": "VARCHAR", "registration_date": "DATE", "updated_at": "TIMESTAMP"},
    "products": {"product_id": "BIGINT", "name": "VARCHAR", "category": "VARCHAR", "price": "DECIMAL(12,2)", "description": "VARCHAR", "is_active": "BOOLEAN", "updated_at": "TIMESTAMP"},
    "orders": {"order_id": "BIGINT", "customer_id": "BIGINT", "order_date": "TIMESTAMP", "status": "VARCHAR", "sales_channel": "VARCHAR", "payment_method": "VARCHAR", "total": "DECIMAL(14,2)"},
    "order_items": {"order_item_id": "BIGINT", "order_id": "BIGINT", "product_id": "BIGINT", "quantity": "INTEGER", "unit_price": "DECIMAL(12,2)", "subtotal": "DECIMAL(14,2)"},
}
MONGO_CSV_COLUMNS = {
    "reviews": {"review_id": "BIGINT", "customer_id": "BIGINT", "product_id": "BIGINT", "rating": "INTEGER", "reviewed_at": "TIMESTAMPTZ", "document_json": "VARCHAR"},
    "navigation_events": {"event_id": "BIGINT", "event_type": "VARCHAR", "occurred_at": "TIMESTAMPTZ", "customer_id": "BIGINT", "product_id": "BIGINT", "sales_channel": "VARCHAR", "document_json": "VARCHAR"},
}


def _csv_value(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _write_csv(path, headers, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        count = 0
        for row in rows:
            writer.writerow([_csv_value(value) for value in row])
            count += 1
    temporary.replace(path)
    return count


def extract_mysql(directory):
    """Extrae tablas MySQL a archivos CSV por ejecución sin tocar DuckDB."""
    source = connect_mysql()
    outputs = {}
    try:
        for table, columns in MYSQL_CSV_COLUMNS.items():
            cursor = source.cursor()
            try:
                cursor.execute(f"SELECT * FROM {table} ORDER BY {next(iter(columns))}")
                path = Path(directory) / f"mysql_{table}.csv"

                def rows():
                    while batch := cursor.fetchmany(BATCH_SIZE):
                        yield from batch

                outputs[table] = {"path": str(path), "rows": _write_csv(path, columns, rows())}
            finally:
                cursor.close()
    finally:
        source.close()
    return outputs


def extract_mongo(directory):
    """Extrae colecciones Mongo a archivos CSV por ejecución sin tocar DuckDB."""
    client = MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
    outputs = {}
    try:
        database = client[os.getenv("MONGO_DATABASE", "ecommerce")]
        for collection, columns in MONGO_CSV_COLUMNS.items():
            cursor = database[collection].find({}).sort("_id", 1).batch_size(BATCH_SIZE)

            def rows():
                for document in cursor:
                    _, values = flatten_mongo(collection, document, None)
                    yield values[:-2]

            path = Path(directory) / f"mongo_{collection}.csv"
            outputs[collection] = {"path": str(path), "rows": _write_csv(path, columns, rows())}
    finally:
        client.close()
    return outputs


def _load_csvs(extracts, table_definitions, csv_columns, prefix, duckdb_path):
    duckdb_path = Path(duckdb_path)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    loaded_at = datetime.now(timezone.utc)
    # ponytail: one lock for the shared DuckDB file; use per-file locks if the warehouse splits later.
    with duckdb_path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with closing(duckdb.connect(str(duckdb_path))) as warehouse:
                warehouse.begin()
                try:
                    warehouse.execute("CREATE SCHEMA IF NOT EXISTS raw")
                    for ddl in table_definitions.values():
                        warehouse.execute(ddl)
                    for table, column_types in csv_columns.items():
                        raw_table = f"raw.{prefix}_{table}"
                        warehouse.execute(f"DELETE FROM {raw_table}")
                        names = list(column_types)
                        source_name = f"{prefix}.{table}" if prefix == "mysql" else f"mongodb.{table}"
                        columns_sql = "{" + ", ".join(f"'{name}': '{kind}'" for name, kind in column_types.items()) + "}"
                        select = ", ".join(f'"{name}"' for name in names)
                        if "document_json" in names:
                            select = select.replace('"document_json"', '"document_json"::JSON')
                        warehouse.execute(
                            f"INSERT INTO {raw_table} SELECT {select}, ?::TIMESTAMPTZ, ? "
                            f"FROM read_csv(?, columns={columns_sql}, header=true, nullstr='')",
                            [loaded_at, source_name, extracts[table]["path"]],
                        )
                    warehouse.commit()
                except Exception:
                    warehouse.rollback()
                    raise
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def load_mysql(extracts, duckdb_path):
    _load_csvs(extracts, MYSQL_TABLES, MYSQL_CSV_COLUMNS, "mysql", duckdb_path)


def load_mongo(extracts, duckdb_path):
    _load_csvs(extracts, MONGO_TABLES, MONGO_CSV_COLUMNS, "mongo", duckdb_path)


def count_raw(duckdb_path):
    with closing(duckdb.connect(str(duckdb_path), read_only=True)) as warehouse:
        return {
            f"{prefix}_{table}": warehouse.execute(f"SELECT COUNT(*) FROM raw.{prefix}_{table}").fetchone()[0]
            for prefix, tables in (("mysql", MYSQL_TABLES), ("mongo", MONGO_TABLES))
            for table in tables
        }
