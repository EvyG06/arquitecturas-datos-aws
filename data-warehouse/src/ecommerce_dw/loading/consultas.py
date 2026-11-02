#!/usr/bin/env python3
"""Consultas de validacion del DW en Redshift, con el rol del pipeline. Cada una imprime una tabla y sale
con codigo 1 si alguna de sus verificaciones falla.

  huella        conteos y totales de hechos y dimensiones, sin columnas que cambian por corrida (_loaded_at)
  capas         filas por entidad en raw (todas las versiones) y en staging (ultima version)
  cuarentena    rechazos por regla y lote de origen
  scd2          historia del cliente 1 y del producto 1, y que haya una sola version vigente por llave
  conciliacion  ventas desde MySQL hasta fct_sales: fuente = staging = validas + cuarentena, y hechos = validas
"""
from __future__ import annotations

import argparse
import sys
from decimal import Decimal

from ecommerce_dw.extraction.entities import ENTITIES

from . import ddl

STG = {"mysql.customers": "stg_mysql__customers", "mysql.products": "stg_mysql__products",
       "mysql.orders": "stg_mysql__orders", "mysql.order_items": "stg_mysql__order_items",
       "documentdb.reviews": "stg_mongodb__reviews", "documentdb.navigation_events": "stg_mongodb__navigation_events"}
QUARANTINE = {e: f"quarantine.{s.replace('stg_', 'quarantine_')}" for e, s in STG.items()}

HUELLA = """
select 'fct_sales' as tabla, count(*) as filas, count(distinct sale_sk) as llaves,
       cast(sum(subtotal) as decimal(18,2)) as total from marts.fct_sales
union all select 'fct_reviews', count(*), count(distinct review_sk), cast(sum(rating) as decimal(18,2)) from marts.fct_reviews
union all select 'fct_navigation_events', count(*), count(distinct navigation_event_sk), cast(null as decimal(18,2))
          from marts.fct_navigation_events
union all select 'dim_customer', count(*), count(distinct customer_id),
          cast(sum(case when is_current then 1 else 0 end) as decimal(18,2)) from marts.dim_customer
union all select 'dim_product', count(*), count(distinct product_id),
          cast(sum(case when is_current then 1 else 0 end) as decimal(18,2)) from marts.dim_product
order by 1
"""

SCD2 = """
select 'cliente' as entidad, customer_id as id, city as valor, department as detalle,
       cast(valid_from as date) as desde, cast(valid_to as date) as hasta, is_current as vigente
from marts.dim_customer where customer_id = 1
union all
select 'producto', product_id, category, cast(null as varchar), cast(valid_from as date), cast(valid_to as date), is_current
from marts.dim_product where product_id = 1
order by 1, 5
"""

VIGENTES_DUPLICADOS = """
select (select count(*) from (select customer_id from marts.dim_customer where is_current group by 1 having count(*) > 1) c)
     + (select count(*) from (select product_id from marts.dim_product where is_current group by 1 having count(*) > 1) p)
"""

CONCILIACION = """
select
  (select count(*) from intermediate.stg_mysql__order_items),
  (select sum(subtotal) from intermediate.stg_mysql__order_items),
  (select count(*) from quarantine.quarantine_mysql__order_items),
  (select coalesce(sum(subtotal), 0) from quarantine.quarantine_mysql__order_items),
  (select count(*) from intermediate.int_mysql__order_items__valid),
  (select sum(subtotal) from intermediate.int_mysql__order_items__valid),
  (select count(*) from intermediate.int_mysql__order_items__valid i
     join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id),
  (select sum(i.subtotal) from intermediate.int_mysql__order_items__valid i
     join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id),
  (select count(*) from marts.fct_sales),
  (select sum(subtotal) from marts.fct_sales)
"""


def _table(headers, rows) -> None:
    rows = [["" if v is None else str(v) for v in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    for r in rows:
        print("  ".join(v.ljust(w) for v, w in zip(r, widths)))


def huella(api) -> bool:
    _table(["tabla", "filas", "llaves", "total o vigentes"], api.query(HUELLA, "dw-huella"))
    return True


def capas(api) -> bool:
    union = " union all ".join(
        f"select '{e.id}', (select count(*) from {ddl.table(e)}), (select count(*) from intermediate.{STG[e.id]})"
        for e in ENTITIES)
    rows = api.query(union, "dw-capas")
    _table(["entidad", "raw (versiones)", "staging (ultima)"], rows)
    return all(int(raw) >= int(stg) for _, raw, stg in rows)


def cuarentena(api) -> bool:
    union = " union all ".join(
        f"select '{e}' as entidad, rejection_reason, _batch_id, count(*) as filas from {q} group by 1, 2, 3"
        for e, q in QUARANTINE.items())
    rows = api.query(f"select * from ({union}) as r order by 1, 2, 3", "dw-cuarentena")
    _table(["entidad", "motivo", "lote de origen", "filas"], rows)
    return all(r[1] and r[2] for r in rows)  # todo rechazo tiene motivo y lote


def scd2(api) -> bool:
    _table(["entidad", "id", "valor", "detalle", "desde", "hasta", "vigente"], api.query(SCD2, "dw-scd2"))
    duplicated = int(api.query(VIGENTES_DUPLICADOS, "dw-scd2-vigentes")[0][0])
    print(f"\nllaves con mas de una version vigente: {duplicated}")
    return duplicated == 0


def conciliacion(api) -> bool:
    from ecommerce_dw import aws_conn

    connection = aws_conn.mysql_connect(lector=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*), SUM(subtotal) FROM order_items")
            src_n, src_total = cursor.fetchone()
    finally:
        connection.close()
    (stg_n, stg_t, q_n, q_t, v_n, v_t, vo_n, vo_t, f_n, f_t) = api.query(CONCILIACION, "dw-conciliacion")[0]
    d = lambda x: Decimal(str(x or 0))  # noqa: E731
    rows = [
        ["MySQL (fuente, etl_lector)", src_n, d(src_total)],
        ["staging: ultima version", stg_n, d(stg_t)],
        ["cuarentena de items", q_n, d(q_t)],
        ["items validos", v_n, d(v_t)],
        ["items validos de pedidos validos", vo_n, d(vo_t)],
        ["marts.fct_sales", f_n, d(f_t)],
    ]
    _table(["capa", "lineas", "subtotal"], rows)
    checks = {
        "fuente = staging": (int(src_n), d(src_total)) == (int(stg_n), d(stg_t)),
        "staging = validos + cuarentena": int(stg_n) == int(v_n) + int(q_n) and d(stg_t) == d(v_t) + d(q_t),
        "fct_sales = validos de pedidos validos": (int(vo_n), d(vo_t)) == (int(f_n), d(f_t)),
    }
    print()
    for name, ok in checks.items():
        print(f"{'OK   ' if ok else 'ERROR'} {name}")
    return all(checks.values())


CONSULTAS = {"huella": huella, "capas": capas, "cuarentena": cuarentena, "scd2": scd2, "conciliacion": conciliacion}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("consultas", nargs="+", choices=sorted(CONSULTAS))
    args = parser.parse_args(argv)

    import boto3

    from ecommerce_dw import aws_conn

    from .redshift import DataApi

    p = aws_conn.params()
    api = DataApi(boto3.client("redshift-data"), p["redshift_workgroup"], p["redshift_database"])
    ok = True
    for name in args.consultas:
        print(f"\n## {name}")
        ok = CONSULTAS[name](api) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
