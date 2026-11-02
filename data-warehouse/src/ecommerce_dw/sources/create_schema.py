#!/usr/bin/env python3
"""Crea las tablas MySQL y las colecciones con validador. Equivale a scripts/create_source_schema.sh en AWS."""

import re
from pathlib import Path

from ecommerce_dw.sources.generate_data import connect_mysql, ensure_mongo_collections
from ecommerce_dw import aws_conn

SCHEMA = Path(__file__).parent / "mysql" / "schema.sql"


def statements(sql: str) -> list[str]:
    """Separa el script por ';' y descarta fragmentos que solo tengan comentarios o espacios."""
    out = []
    for chunk in sql.split(";"):
        body = re.sub(r"--[^\n]*", "", chunk).strip()
        if body:
            out.append(chunk.strip())
    return out


def main():
    connection = connect_mysql()
    try:
        with connection.cursor() as cursor:
            for statement in statements(SCHEMA.read_text(encoding="utf-8")):
                cursor.execute(statement)
            cursor.execute("SHOW TABLES")
            tables = sorted(row[0] for row in cursor.fetchall())
        connection.commit()
    finally:
        connection.close()

    client = aws_conn.mongo_client()
    try:
        db = client["ecommerce"]
        ensure_mongo_collections(db)
        collections = {c["name"]: "con validador" if c.get("options", {}).get("validator") else "sin validador"
                       for c in db.list_collections()}
    finally:
        client.close()
    print(f"MySQL ecommerce: {', '.join(tables)}")
    print(f"DocumentDB ecommerce: {collections}")


if __name__ == "__main__":
    main()
