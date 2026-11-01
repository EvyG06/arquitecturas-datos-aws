"""Ejecuta consultas SQL en Athena dentro del workgroup del proyecto."""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import boto3

from dl.catalog import DATABASE

WORKGROUP = "arq-dl"


def run(sql: str, athena=None, timeout_s: int = 300) -> dict:
    athena = athena or boto3.client("athena")
    qid = athena.start_query_execution(QueryString=sql.strip().rstrip(";"),
                                       QueryExecutionContext={"Database": DATABASE},
                                       WorkGroup=WORKGROUP)["QueryExecutionId"]
    deadline = time.monotonic() + timeout_s
    while True:
        execution = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            athena.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Consulta {qid} supero {timeout_s} s")
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Consulta {qid} {state}: {execution['Status'].get('StateChangeReason')}")
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    stats = execution["Statistics"]
    return {"id": qid, "columns": rows[0] if rows else [], "rows": rows[1:],
            "bytes_scanned": stats.get("DataScannedInBytes", 0), "ms": stats.get("EngineExecutionTimeInMillis")}


def render(result: dict) -> str:
    table = [result["columns"]] + [["" if v is None else v for v in r] for r in result["rows"]]
    widths = [max(len(str(row[i])) for row in table) for i in range(len(result["columns"]))]
    lines = ["  ".join(str(v).ljust(w) for v, w in zip(row, widths)) for row in table]
    lines.insert(1, "  ".join("-" * w for w in widths))
    lines.append(f"[{len(result['rows'])} filas | {result['bytes_scanned']:,} bytes escaneados | "
                 f"{result['ms']} ms | {result['id']}]")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ejecuta consultas SQL en Athena")
    p.add_argument("files", nargs="*", type=Path, help="archivos .sql")
    p.add_argument("-e", "--execute", action="append", default=[], metavar="SQL", help="consulta en linea")
    args = p.parse_args(argv)
    if not args.files and not args.execute:
        p.error("indica al menos un archivo .sql o una consulta con -e")
    for f in args.files:
        print(f"\n== {f.name}")
        print(render(run(f.read_text(encoding="utf-8"))))
    for sql in args.execute:
        print(f"\n== {' '.join(sql.split())[:110]}")
        print(render(run(sql)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
