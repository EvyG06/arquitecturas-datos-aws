#!/usr/bin/env python3
"""Crea los secretos de los usuarios de solo lectura de las fuentes. Corre en el Mac con el perfil admin.

La contrasena se genera aqui y va directo a Secrets Manager por la API. No pasa por la terminal, por el
historial ni por los argumentos de un proceso, donde otro usuario de la maquina podria verla con ps.
Si el secreto ya existe no lo toca, asi repetir el script no rompe usuarios ya creados en las bases.
"""
import json
import secrets
import string

import boto3

TAGS = [{"Key": "Project", "Value": "arquitecturas-datos"}, {"Key": "Component", "Value": "dw"}]
LECTORES = {
    "mysql": ("arq-datos/dw/mysql-lector", "mysql_reader_secret_arn"),
    "docdb": ("arq-datos/dw/docdb-lector", "docdb_reader_secret_arn"),
}


def password(length: int = 32) -> str:
    """Solo letras y digitos, unos 190 bits de entropia, sin simbolos que haya que escapar en SQL o URI."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def main() -> None:
    sm, ssm = boto3.client("secretsmanager"), boto3.client("ssm")
    for fuente, (name, param) in LECTORES.items():
        try:
            arn, estado = sm.describe_secret(SecretId=name)["ARN"], "ya existia"
        except sm.exceptions.ResourceNotFoundException:
            arn = sm.create_secret(
                Name=name, Tags=TAGS,
                Description=f"Usuario de solo lectura de {fuente} para la extraccion del DW",
                SecretString=json.dumps({"username": "etl_lector", "password": password()}),
            )["ARN"]
            estado = "creado"
        ssm.put_parameter(Name=f"/arq-datos/dw/{param}", Value=arn, Type="String", Overwrite=True)
        ssm.add_tags_to_resource(ResourceType="Parameter", ResourceId=f"/arq-datos/dw/{param}", Tags=TAGS)
        print(f"{fuente:6} {estado:11} {arn}")


if __name__ == "__main__":
    main()
