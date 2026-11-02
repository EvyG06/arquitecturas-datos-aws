"""Etapa 8 del DAG: validaciones finales en Athena sobre lo que quedo publicado."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from dl.athena import render, run
from dl.config import DL_ROOT, bucket, safe_run_id
from dl.s3util import put_json

log = logging.getLogger("dl.validate")

SQL_DIR = DL_ROOT / "sql"
CONCILIACION = SQL_DIR / "05_conciliacion_gold_vs_silver.sql"
CALIDAD = SQL_DIR / "06_validaciones_calidad.sql"


def evaluate(conciliation: dict, quality: dict) -> dict:
    mismatches = [dict(zip(conciliation["columns"], r)) for r in conciliation["rows"] if r[-1] != "OK"]
    failures = {r[0]: int(r[1]) for r in quality["rows"] if int(r[1] or 0) > 0}
    return {"descuadres": mismatches, "fallas": failures, "ok": not mismatches and not failures}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validaciones finales con Athena")
    p.add_argument("--run-id", default="manual")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    run_id = safe_run_id(args.run_id)

    conciliation = run(CONCILIACION.read_text(encoding="utf-8"))
    quality = run(CALIDAD.read_text(encoding="utf-8"))
    print(render(conciliation))
    print(render(quality))
    result = evaluate(conciliation, quality)
    now = datetime.now(timezone.utc)
    result.update(run_id=run_id, validado_utc=now.isoformat(timespec="seconds"),
                  consultas={"conciliacion": conciliation["id"], "calidad": quality["id"]})
    import boto3
    put_json(boto3.client("s3"), bucket(), f"_control/validaciones/run_id={run_id}/{now:%Y%m%dT%H%M%SZ}.json", result)
    if result["ok"]:
        log.info("task=validaciones run_id=%s ok filas_conciliadas=%s", run_id, len(conciliation["rows"]))
        return 0
    log.error("task=validaciones run_id=%s descuadres=%s fallas=%s", run_id, result["descuadres"], result["fallas"])
    return 1


if __name__ == "__main__":
    sys.exit(main())
