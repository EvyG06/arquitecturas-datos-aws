#!/usr/bin/env python3
"""Prepara la base del DW con el admin, una sola vez. Corre en el Mac con el perfil admin.

Usa la Data API con el secreto del admin que administra Redshift, asi la contrasena no pasa por la terminal.
  - Crea el usuario IAMR:arq-datos-pipeline con PASSWORD DISABLE: solo entra con credenciales temporales de IAM.
  - Le da CREATE sobre la base: crea y es dueno de sus esquemas (raw, intermediate, quarantine, marts).
  - Quita a PUBLIC el permiso de usar roles IAM en COPY y se lo da solo al pipeline, solo para COPY.
"""
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "data-warehouse" / "src"))
from ecommerce_dw.loading.redshift import DataApi  # noqa: E402

USER = "IAMR:arq-datos-pipeline"


def param(ssm, name: str) -> str:
    return ssm.get_parameter(Name=f"/arq-datos/dw/{name}")["Parameter"]["Value"]


def main() -> None:
    ssm = boto3.client("ssm")
    db, role = param(ssm, "redshift_database"), param(ssm, "redshift_copy_role_arn")
    api = DataApi(boto3.client("redshift-data"), param(ssm, "redshift_workgroup"), db,
                  secret_arn=param(ssm, "redshift_admin_secret_arn"))
    exists = api.query(f"select count(*) from pg_user where usename = '{USER}'")[0][0]
    statements = [] if exists else [f'create user "{USER}" password disable']
    statements += [
        f'grant create on database {db} to "{USER}"',
        "revoke assumerole on all from public for all",
        f"grant assumerole on '{role}' to \"{USER}\" for copy",
    ]
    for sql in statements:  # una por una: algunas sentencias de usuarios no corren dentro de una transaccion
        api.execute(sql, "dw-bootstrap")
        print(f"ok  {sql}")
    for usename, can_create in api.query(
            f"select usename, has_database_privilege(usename, '{db}', 'create') from pg_user "
            f"where usename = '{USER}'"):
        print(f"\n{usename}: create en {db} = {can_create}")


if __name__ == "__main__":
    main()
