"""Vistas legibles del estado del data lake para las evidencias. Solo lectura."""
from __future__ import annotations

import argparse
import io
import json
import sys
from collections import Counter, defaultdict

import boto3
import pyarrow.parquet as pq

from dl.catalog import DATABASE, LAYERS, tables
from dl.config import bucket
from dl.enrich import cache_key
from dl.gold import GOLD_STATE_KEY
from dl.s3util import read_json
from dl.silver import STATE_KEY, bronze_runs


def table(rows: list[dict], cols: list[str] | None = None, width: int = 60) -> str:
    if not rows:
        return "(sin filas)"
    cols = cols or list(rows[0])
    data = [cols] + [[("" if r.get(c) is None else str(r.get(c)))[:width] for c in cols] for r in rows]
    widths = [max(len(row[i]) for row in data) for i in range(len(cols))]
    lines = ["  ".join(v.ljust(w) for v, w in zip(row, widths)) for row in data]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def _rows(s3, b: str, key: str | None) -> list[dict]:
    if not key:
        return []
    return pq.read_table(io.BytesIO(s3.get_object(Bucket=b, Key=key)["Body"].read())).to_pylist()


def _dataset(s3, b: str, state: dict, ds: str) -> list[dict]:
    out = []
    for day, key in sorted(state["datasets"].get(ds, {}).items()):
        out += [{**r, "pub_date": day} for r in _rows(s3, b, key)]
    return out


def cmd_bronze(s3, b, args) -> None:
    rows = []
    for run_id in bronze_runs(s3, b):
        run = read_json(s3, b, f"_control/ingestion/run_id={run_id}/_run.json") or {}
        for sid, info in sorted(run.get("sources", {}).items()):
            rows.append({"run_id": run_id, "inicio_utc": run.get("run_ts_utc"), "lote": run.get("status"),
                         "fuente": sid, "estado": info["status"], "http": info["http_status"],
                         "items": info["item_count"]})
    print(table(rows))
    if args.meta:
        run = read_json(s3, b, f"_control/ingestion/run_id={args.meta}/_run.json")
        uri = next(i["s3_uri"] for i in run["sources"].values() if i["s3_uri"])
        meta_key = uri.split(f"s3://{b}/", 1)[1].rsplit("/", 1)[0] + "/_meta.json"
        print(f"\nRegistro del lote junto al original ({meta_key}):")
        print(json.dumps(read_json(s3, b, meta_key), ensure_ascii=False, indent=2))


def cmd_lote(s3, b, args) -> None:
    run = read_json(s3, b, f"_control/ingestion/run_id={args.run_id}/_run.json")
    print(f"Lote {args.run_id}\n\nBronze  estado={run['status'] if run else 'no existe'}")
    if run:
        print(table([{"fuente": k, **v} for k, v in sorted(run["sources"].items())], width=110))
    state = read_json(s3, b, STATE_KEY) or {"processed_runs": {}}
    entry = state["processed_runs"].get(args.run_id)
    print(f"\nSilver  build vigente={state.get('build_id')}")
    if entry is None:
        print("  el lote NO esta publicado en el estado vigente de Silver")
    else:
        print(f"  procesamientos={entry['procesamientos']}")
        print(table([{"registro": k, **entry[k]} for k in ("primero", "ultimo")]))
    reports = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"_control/silver/run_id={args.run_id}/"):
        reports += page.get("Contents", [])
    print(f"\nReportes de procesamiento guardados: {len(reports)}")
    for obj in sorted(reports, key=lambda o: o["LastModified"]):
        print("  " + obj["Key"])


def cmd_silver(s3, b, args) -> None:
    state = read_json(s3, b, STATE_KEY)
    print(f"Manifiesto de Silver  build_id={state['build_id']}\n")
    parts = [{"conjunto": ds, "pub_date": day, "archivo_vigente": key}
             for ds, days in state["datasets"].items() for day, key in sorted(days.items())]
    print(table(parts, width=120))
    runs = []
    for run_id, e in state["processed_runs"].items():
        p, u = e["primero"], e["ultimo"]
        runs.append({"run_id": run_id, "procesamientos": e["procesamientos"], "nuevas_1ro": p["nuevas"],
                     "nuevas_ult": u["nuevas"], "repetidas_ult": u["repetidas"], "actualizadas_ult": u["actualizadas"],
                     "rechazados_ult": u["rechazados"], "comprehend_ult": u.get("textos_enviados_comprehend")})
    print("\nLotes publicados en el estado vigente\n" + table(runs))


def cmd_muestra(s3, b, args) -> None:
    state = read_json(s3, b, STATE_KEY)
    news = _dataset(s3, b, state, "news")
    print(f"silver_news: {len(news)} noticias vigentes, {len({n['news_id'] for n in news})} news_id distintos\n")
    cols = ["news_id", "id_strategy", "seccion", "pub_date", "published_at_utc", "pub_date_raw", "version", "title"]
    print(table(news[:args.n], cols, width=48))


def cmd_comprehend(s3, b, args) -> None:
    state = read_json(s3, b, STATE_KEY)
    news = _dataset(s3, b, state, "news")
    ents = _dataset(s3, b, state, "news_entities")
    kps = _dataset(s3, b, state, "news_key_phrases")
    print("Estado de procesamiento:", dict(Counter(n["nlp_status"] for n in news)),
          "| idioma:", dict(Counter(n["nlp_language"] for n in news)))
    print(f"news_entities: {len(ents)} filas {dict(Counter(e['entity_type'] for e in ents))}"
          f" | news_key_phrases: {len(kps)} filas\n")
    print(table(ents[:8], ["news_id", "entity_type", "entity_text", "score", "mentions"]))
    print()
    print(table(kps[:5], ["news_id", "phrase_text", "score", "mentions"]))
    sample = next(n for n in news if n["nlp_status"] == "analizado")
    cached = read_json(s3, b, cache_key(sample["text_hash"]))
    shown = {k: cached[k] for k in ("text_hash", "status", "language", "language_score", "analyzed_at_utc")}
    shown["nlp_text"] = cached["nlp_text"][:140]
    shown["entities"] = [{k: e[k] for k in ("Type", "Text", "Score")} for e in cached["entities"][:3]]
    print(f"\nCache inmutable {cache_key(sample['text_hash'])}")
    print(json.dumps(shown, ensure_ascii=False, indent=2))


def cmd_gold(s3, b, args) -> None:
    gold = read_json(s3, b, GOLD_STATE_KEY)
    print(f"Gold construido sobre el build de Silver {gold.get('silver_build_id')}\n")
    daily = [{**r, "pub_date": d} for d, k in sorted(gold["datasets"]["news_daily"].items()) for r in _rows(s3, b, k)]
    print(table(daily, ["pub_date", "medio", "seccion", "n_noticias"]))
    totals: dict[tuple, Counter] = defaultdict(Counter)
    labels: dict[tuple, str] = {}
    for d, k in gold["datasets"]["entity_daily"].items():
        for r in _rows(s3, b, k):
            totals[r["entity_type"]][r["entity_norm"]] += r["n_noticias"]
            labels[(r["entity_type"], r["entity_norm"])] = r["entity_text"]
    print()
    for etype in ("persona", "organizacion", "lugar"):
        top = [f"{labels[(etype, n)]} ({c})" for n, c in totals[etype].most_common(5)]
        print(f"{etype:13} {', '.join(top)}")


def cmd_cuarentena(s3, b, args) -> None:
    key = f"quarantine/silver/run_id={args.run_id}/rechazos.jsonl"
    rows = [json.loads(line) for line in s3.get_object(Bucket=b, Key=key)["Body"].read().decode().splitlines()]
    print(f"{key}: {len(rows)} registros\n")
    flat = [{"motivo": r["rejection_reason"], "fuente": r["source_id"], "lote_bronze": r["bronze_run_id"],
             "posicion": r["item_position"], "titulo_crudo": r["raw"]["title"], "fecha_cruda": r["raw"]["pubDate"],
             "enlace_crudo": r["raw"]["link"]} for r in rows]
    print(table(flat, width=45))
    print("\nArchivo original en Bronze:", rows[0]["bronze_uri"] if rows else "-")


def cmd_versiones(s3, b, args) -> None:
    state = read_json(s3, b, STATE_KEY)
    news = [n for n in _dataset(s3, b, state, "news") if n["version"] > 1]
    print(f"Noticias vigentes con version > 1: {len(news)}\n")
    print(table(news, ["news_id", "version", "first_seen_run_id", "content_run_id", "nlp_status",
                       "nlp_analyzed_at_utc", "title"], width=130))


def cmd_particiones(s3, b, args) -> None:
    glue = boto3.client("glue")
    rows = []
    for layer, ds, name, _ in tables():
        state = read_json(s3, b, LAYERS[layer][0]) or {"datasets": {}}
        manifest = {d: f"s3://{b}/{k.rsplit('/', 1)[0]}/" for d, k in state["datasets"].get(ds, {}).items()}
        registered = {}
        for page in glue.get_paginator("get_partitions").paginate(DatabaseName=DATABASE, TableName=name):
            registered.update({p["Values"][0]: p["StorageDescriptor"]["Location"] for p in page["Partitions"]})
        stale = sorted(d for d in set(manifest) & set(registered) if manifest[d] != registered[d])
        rows.append({"tabla": name, "en_glue": ", ".join(sorted(registered)) or "-",
                     "faltan_en_glue": ", ".join(sorted(set(manifest) - set(registered))) or "-",
                     "ubicacion_desactualizada": ", ".join(stale) or "-"})
    print(table(rows, width=48))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Evidencias del data lake (solo lectura)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("bronze").add_argument("--meta", metavar="RUN_ID")
    sub.add_parser("lote").add_argument("run_id")
    sub.add_parser("silver")
    sub.add_parser("muestra").add_argument("--n", type=int, default=6)
    sub.add_parser("comprehend")
    sub.add_parser("gold")
    sub.add_parser("cuarentena").add_argument("run_id")
    sub.add_parser("versiones")
    sub.add_parser("particiones")
    args = p.parse_args(argv)
    globals()[f"cmd_{args.cmd}"](boto3.client("s3"), bucket(), args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
