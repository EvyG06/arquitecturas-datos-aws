#!/usr/bin/env python3
"""Resumen de la ultima ejecucion de dbt a partir de target/run_results.json.

Muestra conteos por tipo de nodo y estado, y el mensaje completo de cada nodo que fallo. Hace falta porque la
salida de un comando por Session Manager se corta a 24.000 caracteres y el resumen de dbt sale al final.
Codigo de salida 1 si algun modelo, snapshot, seed o prueba termino en error o fail.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter

RUN_RESULTS = os.getenv("DBT_RUN_RESULTS", "/opt/dbt-work/project/target/run_results.json")
BAD = ("error", "fail", "runtime error")


def summarize(data: dict) -> int:
    results = data["results"]
    print(f"dbt {data.get('args', {}).get('which', '?')} | generado {data['metadata']['generated_at']} | "
          f"{data.get('elapsed_time', 0):.1f} s | {len(results)} nodos")
    counts = Counter((r["unique_id"].split(".")[0], r["status"]) for r in results)
    print(f"\n{'tipo':10} {'estado':10} {'nodos':>5}")
    for (kind, status), n in sorted(counts.items()):
        print(f"{kind:10} {status:10} {n:>5}")
    bad = [r for r in results if r["status"] in BAD]
    for r in bad:
        print(f"\n[{r['status']}] {r['unique_id']}")
        print((r.get("message") or "sin mensaje").strip()[:3000])
        if r.get("failures"):
            print(f"filas que incumplen: {r['failures']}")
    print(f"\n{'OK: sin errores ni fallos' if not bad else f'{len(bad)} nodos con error o fallo'}")
    return 1 if bad else 0


def main() -> int:
    with open(RUN_RESULTS, encoding="utf-8") as f:
        return summarize(json.load(f))


if __name__ == "__main__":
    sys.exit(main())
