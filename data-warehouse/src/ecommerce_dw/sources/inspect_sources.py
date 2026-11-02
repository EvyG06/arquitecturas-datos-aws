#!/usr/bin/env python3
"""Evidencia de las fuentes en AWS: version, cifrado en transito, conteos y rechazo de conexiones sin TLS."""

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
            counts = {}
            for table in MYSQL_TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
    finally:
        connection.close()
    print(f"RDS MySQL {version} | TLS {tls.get('Ssl_version')} {tls.get('Ssl_cipher')} | require_secure_transport={required}")
    for table, count in counts.items():
        print(f"  {table:18} {count}")


def mysql_without_tls():
    params = aws_conn.params()
    creds = aws_conn.secret(params["mysql_secret_arn"])
    try:
        pymysql.connect(host=params["mysql_host"], user=creds["username"], password=creds["password"],
                        database="ecommerce", connect_timeout=5).close()
        return "ACEPTADA (no deberia)"
    except pymysql.MySQLError as err:
        return f"rechazada: {err.args[-1] if err.args else err}"


def mongo_report():
    client = aws_conn.mongo_client(tz_aware=True)
    try:
        info = client.server_info()
        db = client["ecommerce"]
        validators = {c["name"]: bool(c.get("options", {}).get("validator")) for c in db.list_collections()}
        counts = {name: db[name].count_documents({}) for name in MONGO_COLLECTIONS}
    finally:
        client.close()
    print(f"DocumentDB (compatibilidad MongoDB {info.get('version')}) | TLS obligatorio con CA de AWS")
    for name, count in counts.items():
        print(f"  {name:18} {count}  validador={'si' if validators.get(name) else 'no'}")


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


def main():
    mysql_report()
    print(f"  conexion MySQL sin TLS: {mysql_without_tls()}")
    print()
    mongo_report()
    print(f"  conexion DocumentDB sin TLS: {mongo_without_tls()}")


if __name__ == "__main__":
    main()
