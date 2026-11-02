#!/usr/bin/env python3
"""Evidencia de las fuentes en AWS: version, TLS, conteos, marca CDC y permisos de los usuarios lectores."""

import pymysql
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from ecommerce_dw import aws_conn

MYSQL_TABLES = ("customers", "products", "orders", "order_items")
MONGO_COLLECTIONS = ("reviews", "navigation_events")


def mysql_report():
    connection = aws_conn.mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            cursor.execute("SHOW SESSION STATUS WHERE Variable_name IN ('Ssl_version', 'Ssl_cipher')")
            tls = dict(cursor.fetchall())
            cursor.execute("SHOW VARIABLES LIKE 'require_secure_transport'")
            required = cursor.fetchone()[1]
            rows = []
            for table in MYSQL_TABLES:
                try:
                    cursor.execute(f"SELECT COUNT(*), MAX(_cdc_ts) FROM {table}")
                    rows.append((table, *cursor.fetchone()))
                except pymysql.MySQLError:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    rows.append((table, cursor.fetchone()[0], "sin columna _cdc_ts"))
    finally:
        connection.close()
    print(f"RDS MySQL {version} | TLS {tls.get('Ssl_version')} {tls.get('Ssl_cipher')} | require_secure_transport={required}")
    for table, count, cdc in rows:
        print(f"  {table:18} {count:>5}  max _cdc_ts {cdc}")


def mysql_without_tls():
    params = aws_conn.params()
    creds = aws_conn.secret(params["mysql_secret_arn"])
    try:
        pymysql.connect(host=params["mysql_host"], user=creds["username"], password=creds["password"],
                        database="ecommerce", connect_timeout=5).close()
        return "ACEPTADA (no deberia)"
    except pymysql.MySQLError as err:
        return f"rechazada: {err.args[-1] if err.args else err}"


def mysql_reader():
    connection = aws_conn.mysql_connect(lector=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT CURRENT_USER(), COUNT(*) FROM customers")
            user, count = cursor.fetchone()
            try:
                cursor.execute("INSERT INTO customers (customer_id, name, email, phone, city, department, "
                               "registration_date) VALUES (0, 'x', 'x@x.co', '0', 'x', 'x', '2025-01-01')")
                write = "PERMITIDA (no deberia)"
            except pymysql.MySQLError as err:
                write = f"denegada: {err.args[-1]}"
    finally:
        connection.close()
    return f"{user} lee {count} clientes | escritura {write}"


def mongo_report():
    client = aws_conn.mongo_client(tz_aware=True)
    try:
        info = client.server_info()
        db = client["ecommerce"]
        validators = {c["name"]: bool(c.get("options", {}).get("validator")) for c in db.list_collections()}
        rows = [(name, db[name].count_documents({}), db[name].count_documents({"_cdc_ts": {"$exists": True}}))
                for name in MONGO_COLLECTIONS]
    finally:
        client.close()
    print(f"DocumentDB (compatibilidad MongoDB {info.get('version')}) | TLS obligatorio con CA de AWS")
    for name, count, with_cdc in rows:
        print(f"  {name:18} {count:>5}  validador={'si' if validators.get(name) else 'no'}  con _cdc_ts={with_cdc}")


def mongo_without_tls():
    params = aws_conn.params()
    client = MongoClient(host=params["docdb_host"], port=27017, tls=False, serverSelectionTimeoutMS=5000,
                         connectTimeoutMS=5000, retryWrites=False, directConnection=True)
    try:
        client.admin.command("ping")
        return "ACEPTADA (no deberia)"
    except PyMongoError as err:
        return f"rechazada: {type(err).__name__}"
    finally:
        client.close()


def mongo_reader():
    client = aws_conn.mongo_client(lector=True)
    try:
        db = client["ecommerce"]
        count = db["reviews"].count_documents({})
        try:
            db["prueba_lector"].insert_one({"x": 1})
            db["prueba_lector"].drop()
            write = "PERMITIDA (no deberia)"
        except PyMongoError as err:
            write = f"denegada: {type(err).__name__}"
    finally:
        client.close()
    return f"etl_lector lee {count} resenas | escritura {write}"


def main():
    mysql_report()
    print(f"  conexion sin TLS: {mysql_without_tls()}")
    try:
        print(f"  usuario lector: {mysql_reader()}")
    except Exception as err:  # el lector aun no existe antes de create_readers
        print(f"  usuario lector: no disponible ({type(err).__name__})")
    print()
    mongo_report()
    print(f"  conexion sin TLS: {mongo_without_tls()}")
    try:
        print(f"  usuario lector: {mongo_reader()}")
    except Exception as err:
        print(f"  usuario lector: no disponible ({type(err).__name__})")


if __name__ == "__main__":
    main()
