"""Conexiones a las fuentes en AWS.

Los endpoints viven en SSM Parameter Store (/arq-datos/dw/) y las credenciales en los secretos que RDS y
DocumentDB administran en Secrets Manager. No hay contrasenas en variables de entorno ni en archivos.
Se activa con DW_AWS=1. Sin esa variable, los scripts siguen usando las variables de la version local.

Hay dos identidades por fuente. Las credenciales maestras las usa solo el sistema de origen simulado
(rol arq-datos-fuentes). La extraccion usa usuarios de solo lectura (lector=True, rol arq-datos-pipeline).
"""
from __future__ import annotations

import json
import os
import warnings
from datetime import datetime, timezone
from functools import lru_cache

PARAM_PATH = "/arq-datos/dw/"
CA_BUNDLE = os.getenv("RDS_CA_BUNDLE", "/opt/certs/rds-global-bundle.pem")


def enabled() -> bool:
    return os.getenv("DW_AWS") == "1"


@lru_cache(maxsize=1)
def params() -> dict[str, str]:
    import boto3

    values = {}
    for page in boto3.client("ssm").get_paginator("get_parameters_by_path").paginate(Path=PARAM_PATH):
        for p in page["Parameters"]:
            values[p["Name"].rsplit("/", 1)[1]] = p["Value"]
    missing = {"mysql_host", "mysql_secret_arn", "docdb_host", "docdb_secret_arn"} - set(values)
    if missing:
        raise RuntimeError(f"Faltan parametros en {PARAM_PATH}: {sorted(missing)}")
    return values


def secret(arn: str) -> dict:
    """Sin cache a proposito: los secretos administrados rotan cada 7 dias."""
    import boto3

    return json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=arn)["SecretString"])


def _secret_arn(p: dict, fuente: str, lector: bool) -> str:
    key = f"{fuente}_reader_secret_arn" if lector else f"{fuente}_secret_arn"
    if key not in p:
        raise RuntimeError(f"Falta el parametro {PARAM_PATH}{key}")
    return p[key]


def mysql_connect(lector: bool = False, **kwargs):
    """RDS for MySQL con TLS verificado contra el bundle de AWS (ca implica CERT_REQUIRED y check_hostname)."""
    import pymysql

    p = params()
    creds = secret(_secret_arn(p, "mysql", lector))
    return pymysql.connect(host=p["mysql_host"], port=3306, user=creds["username"], password=creds["password"],
                           database=os.getenv("MYSQL_DATABASE", "ecommerce"), charset="utf8mb4",
                           ssl={"ca": CA_BUNDLE}, **kwargs)


def mongo_client(lector: bool = False, **kwargs):
    """DocumentDB. Las credenciales van como argumentos, asi no hay que escaparlas dentro de una URI.

    retryWrites=False porque DocumentDB no soporta escrituras reintentables, y replicaSet=rs0 es el nombre
    fijo del conjunto de replicas de todo cluster DocumentDB.
    """
    from pymongo import MongoClient

    # pymongo avisa en cada conexion que el servidor es DocumentDB. Ya lo sabemos y esta documentado.
    warnings.filterwarnings("ignore", message="You appear to be connected to a DocumentDB cluster")
    p = params()
    creds = secret(_secret_arn(p, "docdb", lector))
    return MongoClient(host=p["docdb_host"], port=27017, username=creds["username"], password=creds["password"],
                       tls=True, tlsCAFile=CA_BUNDLE, replicaSet="rs0", readPreference="secondaryPreferred",
                       retryWrites=False, **kwargs)


def cdc_stamp(documents: list[dict]) -> list[dict]:
    """Marca tecnica para la extraccion incremental de DocumentDB, equivalente a la columna _cdc_ts de MySQL.

    DocumentDB no la pone solo, asi que la escribe la aplicacion en cada insercion o reemplazo. Solo en AWS:
    la version local queda igual.
    """
    if enabled():
        now = datetime.now(timezone.utc)
        for document in documents:
            document["_cdc_ts"] = now
    return documents
