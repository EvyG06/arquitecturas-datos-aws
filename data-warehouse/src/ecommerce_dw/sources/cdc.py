#!/usr/bin/env python3
"""Agrega la columna tecnica _cdc_ts a las tablas MySQL de origen. Idempotente. Solo para AWS.

MySQL la actualiza solo en cada INSERT y UPDATE (DEFAULT y ON UPDATE CURRENT_TIMESTAMP(6)). Es la marca de
agua de la extraccion incremental y no depende de updated_at, que es una fecha de negocio: simulate_changes
la escribe con valores del pasado y orders u order_items ni siquiera la tienen.
No se agrega a schema.sql porque la extraccion local hace SELECT * y desalinearia sus columnas.
"""
TABLES = ("customers", "products", "orders", "order_items")
DDL = ("ALTER TABLE {t} ADD COLUMN _cdc_ts TIMESTAMP(6) NOT NULL "
       "DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), ADD INDEX ix_{t}_cdc_ts (_cdc_ts)")


def add_cdc_columns(cursor) -> dict[str, str]:
    result = {}
    for table in TABLES:
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns "
                       "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = '_cdc_ts'", (table,))
        if cursor.fetchone()[0]:
            result[table] = "ya existia"
        else:
            cursor.execute(DDL.format(t=table))
            result[table] = "agregada"
    return result


def main():
    from ecommerce_dw import aws_conn

    connection = aws_conn.mysql_connect(autocommit=True)
    try:
        with connection.cursor() as cursor:
            for table, estado in add_cdc_columns(cursor).items():
                print(f"{table:12} _cdc_ts {estado}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
