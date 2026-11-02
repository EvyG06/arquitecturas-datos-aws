"""Etapa 1 del DAG: valida la configuracion y el acceso a cada servicio antes de tocar datos."""
from __future__ import annotations

import json
import logging
import sys

import boto3

from dl.athena import WORKGROUP
from dl.catalog import DATABASE
from dl.config import bucket, load_sources

log = logging.getLogger("dl.preflight")


def checks():
    yield "configuracion", lambda: f"{len(load_sources())} fuentes validas"
    yield "identidad", lambda: boto3.client("sts").get_caller_identity()["Arn"]
    yield "s3", lambda: (boto3.client("s3").head_bucket(Bucket=bucket()), f"s3://{bucket()}")[1]
    yield "glue", lambda: boto3.client("glue").get_database(Name=DATABASE)["Database"]["Name"]
    yield "athena", lambda: boto3.client("athena").get_work_group(WorkGroup=WORKGROUP)["WorkGroup"]["State"]
    yield "comprehend", lambda: boto3.client("comprehend").detect_dominant_language(
        Text="Prueba de acceso al servicio de lenguaje natural")["Languages"][0]["LanguageCode"]


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    results, failed = {}, []
    for name, check in checks():
        try:
            results[name] = {"ok": True, "detalle": check()}
            log.info("task=preflight servicio=%s ok %s", name, results[name]["detalle"])
        except Exception as err:  # se reporta cualquier fallo de acceso o de configuracion
            results[name] = {"ok": False, "detalle": f"{type(err).__name__}: {err}"[:300]}
            failed.append(name)
            log.error("task=preflight servicio=%s fallo %s", name, results[name]["detalle"])
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
