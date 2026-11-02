#!/usr/bin/env python3
"""Etapa 1 del DAG del DW: valida conexiones y parametros antes de tocar datos.

Corre todas las verificaciones aunque una falle, para mostrar de una vez todo lo que esta mal, y sale con
codigo 1 si alguna fallo. En el DAG no tiene reintentos: un parametro equivocado no se arregla esperando.
En modo reproceso no consulta las fuentes: un lote ya extraido se reprocesa desde S3 aunque esten apagadas.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Callable

REQUIRED_PARAMS = ("mysql_host", "docdb_host", "mysql_reader_secret_arn", "docdb_reader_secret_arn",
                   "redshift_workgroup", "redshift_database", "redshift_copy_role_arn")


def check_params() -> str:
    from ecommerce_dw import aws_conn

    missing = [k for k in REQUIRED_PARAMS if k not in aws_conn.params()]
    if missing:
        raise RuntimeError(f"faltan parametros en {aws_conn.PARAM_PATH}: {', '.join(missing)}")
    return f"{len(REQUIRED_PARAMS)} parametros en {aws_conn.PARAM_PATH}"


def check_bucket(s3, bucket: str) -> str:
    from ecommerce_dw.extraction import s3util
    from ecommerce_dw.extraction.extract import STATE_KEY

    s3.head_bucket(Bucket=bucket)
    state = s3util.read_json(s3, bucket, STATE_KEY)
    return f"s3://{bucket} accesible, marca de agua seq={state['seq'] if state else 0}"


def check_mysql() -> str:
    from ecommerce_dw import aws_conn

    connection = aws_conn.mysql_connect(lector=True, connect_timeout=10)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT CURRENT_USER(), VERSION()")
            user, version = cursor.fetchone()[:2]
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_version'")
            tls = cursor.fetchone()[1]
    finally:
        connection.close()
    return f"{user} en MySQL {version} con {tls}"


def check_documentdb() -> str:
    from ecommerce_dw import aws_conn

    client = aws_conn.mongo_client(lector=True, serverSelectionTimeoutMS=10000)
    try:
        client.admin.command("ping")
        reviews = client["ecommerce"]["reviews"].estimated_document_count()
    finally:
        client.close()
    return f"etl_lector conectado con TLS, {reviews} resenas"


def check_redshift() -> str:
    import boto3

    from ecommerce_dw import aws_conn
    from ecommerce_dw.loading.redshift import DataApi

    p = aws_conn.params()
    api = DataApi(boto3.client("redshift-data"), p["redshift_workgroup"], p["redshift_database"], timeout=120)
    user = api.query("select current_user", "dw-preflight")[0][0]
    return f"Data API como {user} en {p['redshift_workgroup']}/{p['redshift_database']}"


def check_reprocess(s3, bucket: str, batch_id: str) -> str:
    from ecommerce_dw.extraction import s3util
    from ecommerce_dw.extraction.extract import BATCH_ID, STATE_KEY

    if not BATCH_ID.match(batch_id):
        raise ValueError(f"reprocesar_batch_id invalido: {batch_id!r}")
    state = s3util.read_json(s3, bucket, STATE_KEY) or {"historial": []}
    entry = next((h for h in state["historial"] if h["batch_id"] == batch_id), None)
    if entry is None:
        raise ValueError(f"el lote {batch_id} no esta confirmado en state.json: no hay nada que reprocesar")
    return f"lote {batch_id} confirmado con seq={entry['seq']} y {entry['filas']} filas"


def run_checks(checks: dict[str, Callable[[], str]]) -> int:
    failures = 0
    for name, check in checks.items():
        try:
            print(f"OK     {name:12} {check()}", flush=True)
        except Exception as err:  # se reporta y se sigue: la salida muestra todos los problemas juntos
            failures += 1
            print(f"ERROR  {name:12} {type(err).__name__}: {err}", flush=True)
    print(f"\n{'Configuracion valida' if not failures else f'{failures} verificaciones fallaron'}")
    return 1 if failures else 0


def build_checks(s3, bucket: str, reprocess: str | None) -> dict[str, Callable[[], str]]:
    checks = {"parametros": check_params, "bucket": lambda: check_bucket(s3, bucket)}
    if reprocess:
        checks["lote"] = lambda: check_reprocess(s3, bucket, reprocess)
    else:
        checks["mysql"] = check_mysql
        checks["documentdb"] = check_documentdb
    checks["redshift"] = check_redshift
    return checks


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--reprocesar", metavar="BATCH_ID", help="Valida un reproceso: no consulta las fuentes")
    args = parser.parse_args(argv)

    import boto3

    bucket = os.environ.get("DW_BUCKET")
    if not bucket:
        print("ERROR  DW_BUCKET no esta definido")
        return 1
    return run_checks(build_checks(boto3.client("s3"), bucket, args.reprocesar))


if __name__ == "__main__":
    sys.exit(main())
