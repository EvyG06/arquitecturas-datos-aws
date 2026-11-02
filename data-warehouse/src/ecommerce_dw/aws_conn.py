"""Conexiones a las fuentes en AWS.

Los endpoints viven en SSM Parameter Store (/arq-datos/dw/) y las credenciales en los secretos que RDS y
DocumentDB administran en Secrets Manager. No hay contrasenas en variables de entorno ni en archivos.
Se activa con DW_AWS=1. Sin esa variable, los scripts siguen usando las variables de la version local.
"""
from __future__ import annotations

import json
import os
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


def mysql_connect(**kwargs):
    """RDS for MySQL con TLS verificado contra el bundle de AWS (ca implica CERT_REQUIRED y check_hostname)."""
    import pymysql

    p = params()
    creds = secret(p["mysql_secret_arn"])
    return pymysql.connect(host=p["mysql_host"], port=3306, user=creds["username"], password=creds["password"],
                           database=os.getenv("MYSQL_DATABASE", "ecommerce"), charset="utf8mb4",
                           ssl={"ca": CA_BUNDLE}, **kwargs)


def mongo_client(**kwargs):
    """DocumentDB. Las credenciales van como argumentos, asi no hay que escaparlas dentro de una URI.

    retryWrites=False porque DocumentDB no soporta escrituras reintentables, y replicaSet=rs0 es el nombre
    fijo del conjunto de replicas de todo cluster DocumentDB.
    """
    from pymongo import MongoClient

    p = params()
    creds = secret(p["docdb_secret_arn"])
    return MongoClient(host=p["docdb_host"], port=27017, username=creds["username"], password=creds["password"],
                       tls=True, tlsCAFile=CA_BUNDLE, replicaSet="rs0", readPreference="secondaryPreferred",
                       retryWrites=False, **kwargs)
