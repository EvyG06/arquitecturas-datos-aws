#!/usr/bin/env python3
"""Refresca el dataset SPICE de QuickSight al final de cada lote, despues de las pruebas y la conciliacion.

Asi el dashboard muestra el lote recien procesado sin intervencion manual. Si QuickSight aun no esta
configurado (no existe el parametro quicksight_dataset_id) sale con 99 y el DAG marca la tarea como omitida.
"""
from __future__ import annotations

import argparse
import re
import sys
import time

EXIT_OMITIDO = 99


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--lote", required=True)
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args(argv)

    import boto3

    from ecommerce_dw import aws_conn

    dataset = aws_conn.params().get("quicksight_dataset_id")
    if not dataset:
        print(f"batch_id={args.lote} QuickSight no esta configurado: se omite el refresco")
        return EXIT_OMITIDO
    account = boto3.client("sts").get_caller_identity()["Account"]
    qs = boto3.client("quicksight")
    ingestion = re.sub(r"[^A-Za-z0-9_-]", "-", f"{args.lote}-{int(time.time())}")[-128:]
    qs.create_ingestion(AwsAccountId=account, DataSetId=dataset, IngestionId=ingestion, IngestionType="FULL_REFRESH")
    print(f"batch_id={args.lote} refresco SPICE iniciado dataset={dataset} ingestion={ingestion}", flush=True)
    start = time.monotonic()
    while True:
        info = qs.describe_ingestion(AwsAccountId=account, DataSetId=dataset, IngestionId=ingestion)["Ingestion"]
        status = info["IngestionStatus"]
        if status == "COMPLETED":
            rows = info.get("RowInfo", {})
            print(f"batch_id={args.lote} refresco SPICE completo filas={rows.get('RowsIngested')} "
                  f"descartadas={rows.get('RowsDropped', 0)} segundos={info.get('IngestionTimeInSeconds')}")
            return 0
        if status in ("FAILED", "CANCELLED"):
            print(f"batch_id={args.lote} refresco SPICE {status}: {info.get('ErrorInfo')}")
            return 1
        if time.monotonic() - start > args.timeout:
            print(f"batch_id={args.lote} el refresco sigue en {status} despues de {args.timeout} s")
            return 1
        time.sleep(10)


if __name__ == "__main__":
    sys.exit(main())
