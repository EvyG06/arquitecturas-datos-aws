#!/usr/bin/env python3
"""Crea o actualiza el usuario de solo lectura de cada fuente con la contrasena de su secreto. Idempotente.

Lo corre el sistema de origen (rol arq-datos-fuentes), que es quien tiene las credenciales maestras.
"""
from ecommerce_dw import aws_conn

DOCDB_ROLES = [{"role": "read", "db": "ecommerce"}]


def mysql_reader(creds: dict) -> list[str]:
    connection = aws_conn.mysql_connect(autocommit=True)
    try:
        with connection.cursor() as cursor:
            user = (creds["username"],)
            cursor.execute("CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s REQUIRE SSL", (*user, creds["password"]))
            cursor.execute("ALTER USER %s@'%%' IDENTIFIED BY %s REQUIRE SSL", (*user, creds["password"]))
            cursor.execute("GRANT SELECT ON ecommerce.* TO %s@'%%'", user)
            cursor.execute("SHOW GRANTS FOR %s@'%%'", user)
            return [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()


def docdb_reader(creds: dict) -> list:
    client = aws_conn.mongo_client()
    try:
        admin = client.admin
        exists = admin.command("usersInfo", creds["username"]).get("users")
        action = "updateUser" if exists else "createUser"
        admin.command(action, creds["username"], pwd=creds["password"], roles=DOCDB_ROLES)
        return admin.command("usersInfo", creds["username"])["users"][0]["roles"]
    finally:
        client.close()


def main():
    p = aws_conn.params()
    my = aws_conn.secret(p["mysql_reader_secret_arn"])
    dc = aws_conn.secret(p["docdb_reader_secret_arn"])
    print(f"MySQL {my['username']}:")
    for grant in mysql_reader(my):
        print(f"  {grant}")
    print(f"DocumentDB {dc['username']}: {docdb_reader(dc)}")


if __name__ == "__main__":
    main()
