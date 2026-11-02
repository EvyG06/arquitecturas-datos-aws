#!/usr/bin/env python3
"""Prueba la logica del proyecto dbt sin Redshift: arma raw con el mismo esquema de AWS en DuckDB y corre dbt.

Usa el generador real (seed 42), el mismo contrato de columnas de la extraccion (entities.py) y el mismo DDL
de la carga (loading/ddl.py). Simula dos lotes:
  b1: carga inicial completa.
  b2: repite 3 pedidos identicos (como el solape CDC) y trae al cliente 1 mudado a Medellín, Antioquia, con un updated_at nuevo.
Despues de cada lote corre dbt build. Los hechos no deben duplicarse y el cliente 1 debe tener dos versiones SCD2.
Uso: python probar_dbt_duckdb.py <directorio_del_proyecto_dbt> <dbt>
"""
import io
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

SRC = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(SRC))
from ecommerce_dw.extraction.entities import ENTITIES  # noqa: E402
from ecommerce_dw.extraction.extract import to_parquet  # noqa: E402
from ecommerce_dw.ingestion.raw_sources import flatten_mongo  # noqa: E402
from ecommerce_dw.loading import ddl  # noqa: E402
from ecommerce_dw.sources import generate_data as g  # noqa: E402

UTC = timezone.utc
T1 = datetime(2026, 11, 2, 21, 36, tzinfo=UTC)


def source_rows():
    sys.argv = ["generate_data"]
    args = g.parse_args()
    rng, fake, customers, products, orders, details, start = g.build_rows(args)
    bad = g.build_bad_records(args, start)
    reviews, events = g.build_mongo_documents(args, rng, fake, customers, products, start)
    mongo = {"reviews": reviews + bad["mongo_reviews"], "navigation_events": events + bad["mongo_events"]}
    mysql = {"customers": customers + bad["mysql_customers"], "products": products + bad["mysql_products"],
             "orders": orders + bad["mysql_orders"], "order_items": details + bad["mysql_order_items"]}
    rows = {}
    for entity in ENTITIES:
        if entity.source == "mysql":
            rows[entity.id] = [{c: r.get(c) for c in entity.columns} for r in mysql[entity.name]]
        else:
            rows[entity.id] = [dict(zip(entity.columns, flatten_mongo(entity.name, d, None)[1][:len(entity.columns)]))
                               for d in mongo[entity.name]]
        for r in rows[entity.id]:
            if "is_active" in r and r["is_active"] is not None:
                r["is_active"] = bool(r["is_active"])
            r["_cdc_ts"] = T1
    return rows


def load(con, batch_id, rows_by_entity, extracted_at):
    for entity in ENTITIES:
        rows = rows_by_entity.get(entity.id, [])
        if not rows:
            continue
        table = pq.read_table(io.BytesIO(to_parquet(entity, rows, batch_id, extracted_at)))
        con.register("lote", table)
        cols = ", ".join(n for n, _ in ddl.columns(entity))
        con.execute(f"insert into {ddl.table(entity)} ({cols}, _loaded_at) select {cols}, now()::timestamp from lote")
        con.unregister("lote")


def dbt(project, dbt_bin, db):
    env = {**os.environ, "DW_DUCKDB_PATH": db}
    cmd = [dbt_bin, "build", "--profiles-dir", project, "--project-dir", project, "--target", "duckdb_prueba"]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    summary = [l for l in p.stdout.splitlines() if "Done. PASS" in l or "ERROR" in l or "FAIL " in l or "WARN " in l]
    print("\n".join(summary[-15:]))
    if p.returncode:
        print(p.stdout[-6000:])
        raise SystemExit(f"dbt build fallo con codigo {p.returncode}")


def counts(db, label):
    con = duckdb.connect(db, read_only=True)
    q = {t: f"select count(*) from marts.{t}" for t in ("fct_sales", "fct_reviews", "fct_navigation_events")}
    q["dim_customer versiones de 1"] = "select count(*) from marts.dim_customer where customer_id = 1"
    q["ciudad vigente de 1"] = "select city from marts.dim_customer where customer_id = 1 and is_current"
    q["raw.mysql_orders filas"] = "select count(*) from raw.mysql_orders"
    q["stg_mysql__orders filas"] = "select count(*) from intermediate.stg_mysql__orders"
    q["cuarentena con lote"] = ("select count(*) from quarantine.quarantine_mysql__orders "
                                "where _batch_id is not null and rejection_reason is not null")
    print(f"\n[{label}]")
    out = {}
    for k, sql in q.items():
        out[k] = con.execute(sql).fetchone()[0]
        print(f"  {k:30} {out[k]}")
    con.close()
    return out


def main():
    project, dbt_bin = sys.argv[1], sys.argv[2]
    db = str(Path(project) / "prueba.duckdb")
    Path(db).unlink(missing_ok=True)
    rows = source_rows()
    con = duckdb.connect(db)
    con.execute("set TimeZone = 'UTC'")
    for sql in ddl.bootstrap():
        con.execute(sql)
    load(con, "b1", rows, T1 + timedelta(minutes=5))
    con.close()
    dbt(project, dbt_bin, db)
    first = counts(db, "despues del lote b1")

    con = duckdb.connect(db)
    con.execute("set TimeZone = 'UTC'")
    repeated = {"mysql.orders": rows["mysql.orders"][:3]}  # mismas filas, mismo _cdc_ts: como el solape
    changed = dict(rows["mysql.customers"][0])
    changed.update(city="Medellín", department="Antioquia", updated_at=changed["updated_at"] + timedelta(days=30),
                   _cdc_ts=T1 + timedelta(hours=1))
    load(con, "b2", {**repeated, "mysql.customers": [changed]}, T1 + timedelta(hours=1, minutes=5))
    con.close()
    dbt(project, dbt_bin, db)
    second = counts(db, "despues del lote b2")

    assert first["fct_sales"] == second["fct_sales"], "el lote repetido duplico ventas"
    assert second["raw.mysql_orders filas"] == first["raw.mysql_orders filas"] + 3
    assert second["stg_mysql__orders filas"] == first["stg_mysql__orders filas"]
    assert second["dim_customer versiones de 1"] == first["dim_customer versiones de 1"] + 1
    assert second["ciudad vigente de 1"] == "Medellín"
    print("\nOK: sin duplicados, ultima version en staging y nueva version SCD2 del cliente 1")


if __name__ == "__main__":
    main()
