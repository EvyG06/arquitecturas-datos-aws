#!/usr/bin/env python3
"""Usuario de Redshift para QuickSight: bi_lector. Corre en el Mac con el perfil admin. Idempotente.

  - La contrasena se genera aqui y va a Secrets Manager. A Redshift solo llega su hash MD5 ('md5' + md5(clave +
    usuario)), asi la clave no aparece en el texto de la sentencia, ni en el historial de la Data API ni en el
    de Redshift.
  - Solo recibe USAGE sobre el esquema marts. El SELECT sobre marts.ventas_dashboard lo otorga dbt (grants)
    cada vez que reconstruye esa tabla: QuickSight no puede leer ninguna otra tabla.
"""
import hashlib
import json
import secrets
import string
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "data-warehouse" / "src"))
from ecommerce_dw.loading.redshift import DataApi  # noqa: E402

USER = "bi_lector"
SECRET = "arq-datos/dw/redshift-bi-lector"
TAGS = [{"Key": "Project", "Value": "arquitecturas-datos"}, {"Key": "Component", "Value": "dw"}]


def password() -> str:
    """Redshift exige mayuscula, minuscula y digito. Solo alfanumericos: nada que escapar en SQL ni en JDBC."""
    alphabet = string.ascii_letters + string.digits
    while True:
        p = "".join(secrets.choice(alphabet) for _ in range(32))
        if any(c.isupper() for c in p) and any(c.islower() for c in p) and any(c.isdigit() for c in p):
            return p


def param(ssm, name: str) -> str:
    return ssm.get_parameter(Name=f"/arq-datos/dw/{name}")["Parameter"]["Value"]


def main() -> None:
    ssm, sm = boto3.client("ssm"), boto3.client("secretsmanager")
    try:
        arn = sm.describe_secret(SecretId=SECRET)["ARN"]
        estado = "ya existia"
    except sm.exceptions.ResourceNotFoundException:
        arn = sm.create_secret(Name=SECRET, Tags=TAGS, Description="Usuario de solo lectura de QuickSight en Redshift",
                               SecretString=json.dumps({"username": USER, "password": password()}))["ARN"]
        estado = "creado"
    ssm.put_parameter(Name="/arq-datos/dw/redshift_bi_secret_arn", Value=arn, Type="String", Overwrite=True)
    print(f"secreto {estado}: {arn}")

    clave = json.loads(sm.get_secret_value(SecretId=arn)["SecretString"])["password"]
    md5 = "md5" + hashlib.md5((clave + USER).encode()).hexdigest()
    api = DataApi(boto3.client("redshift-data"), param(ssm, "redshift_workgroup"), param(ssm, "redshift_database"),
                  secret_arn=param(ssm, "redshift_admin_secret_arn"))
    existe = api.query(f"select count(*) from pg_user where usename = '{USER}'")[0][0]
    accion = "alter" if existe else "create"
    for sql, mostrar in (
        (f"{accion} user {USER} password '{md5}' connection limit 10", f"{accion} user {USER} password 'md5...'"),
        (f"grant usage on schema marts to {USER}", None),
    ):
        api.execute(sql, "dw-bi-lector")
        print(f"ok  {mostrar or sql}")


if __name__ == "__main__":
    main()
