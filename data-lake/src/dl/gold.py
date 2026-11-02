"""Capa Gold: agregados por fecha de publicacion, derivados del manifiesto de Silver."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from dl.config import ControlledFault, bucket, fault_point
from dl.s3util import ConcurrentUpdate, put_json, put_json_cas, read_json_etag
from dl.silver import STATE_KEY as SILVER_STATE_KEY
from dl.silver import write_partition

log = logging.getLogger("dl.gold")

GOLD_STATE_KEY = "_control/gold/state.json"
SILVER_INPUTS = ("news", "news_entities", "news_key_phrases")

SCHEMAS = {
    "news_daily": pa.schema([
        ("medio", pa.string()),
        ("seccion", pa.string()),
        ("n_noticias", pa.int32()),
    ]),
    "entity_daily": pa.schema([
        ("seccion", pa.string()),
        ("entity_type", pa.string()),
        ("entity_norm", pa.string()),
        ("entity_text", pa.string()),
        ("n_noticias", pa.int32()),
        ("menciones", pa.int32()),
        ("score_promedio", pa.float64()),
    ]),
    "key_phrase_daily": pa.schema([
        ("seccion", pa.string()),
        ("phrase_norm", pa.string()),
        ("phrase_text", pa.string()),
        ("n_noticias", pa.int32()),
        ("menciones", pa.int32()),
    ]),
}


# ---------- logica pura ----------

def _group(rows: list[dict], section_of: dict[str, str], key_fields: tuple, text_field: str) -> list[dict]:
    acc: dict[tuple, dict] = defaultdict(lambda: {"news": set(), "menciones": 0, "scores": [], "textos": Counter()})
    for r in sorted(rows, key=lambda r: r["news_id"]):
        if r["news_id"] not in section_of:
            raise ValueError(f"fila sin noticia vigente: news_id={r['news_id']}")
        key = (section_of[r["news_id"]],) + tuple(r[f] for f in key_fields)
        a = acc[key]
        a["news"].add(r["news_id"])
        a["menciones"] += r["mentions"]
        a["scores"].append(r["score"])
        a["textos"][r[text_field]] += 1
    out = []
    for key, a in sorted(acc.items()):
        row = {"seccion": key[0], **dict(zip(key_fields, key[1:])),
               text_field: a["textos"].most_common(1)[0][0],
               "n_noticias": len(a["news"]), "menciones": a["menciones"],
               "score_promedio": round(sum(a["scores"]) / len(a["scores"]), 4)}
        out.append(row)
    return out


def aggregate_day(news: list[dict], entities: list[dict], phrases: list[dict]) -> dict[str, list[dict]]:
    section_of = {n["news_id"]: n["seccion"] for n in news}
    by_section = Counter((n["medio"], n["seccion"]) for n in news)
    return {
        "news_daily": [{"medio": m, "seccion": s, "n_noticias": c} for (m, s), c in sorted(by_section.items())],
        "entity_daily": _group(entities, section_of, ("entity_type", "entity_norm"), "entity_text"),
        "key_phrase_daily": _group(phrases, section_of, ("phrase_norm",), "phrase_text"),
    }


def reconcile(day: str, news: list[dict], entities: list[dict], phrases: list[dict], gold: dict) -> dict:
    """Cada metrica de Gold debe coincidir con el conteo distinto equivalente en Silver."""
    checks = {
        "noticias": (len({n["news_id"] for n in news}),
                     sum(r["n_noticias"] for r in gold["news_daily"])),
        "pares_noticia_entidad": (len({(e["news_id"], e["entity_type"], e["entity_norm"]) for e in entities}),
                                  sum(r["n_noticias"] for r in gold["entity_daily"])),
        "pares_noticia_frase": (len({(k["news_id"], k["phrase_norm"]) for k in phrases}),
                                sum(r["n_noticias"] for r in gold["key_phrase_daily"])),
    }
    failed = {k: v for k, v in checks.items() if v[0] != v[1]}
    if failed:
        raise ValueError(f"conciliacion Silver-Gold fallida en {day}: {failed}")
    return {k: v[0] for k, v in checks.items()}


# ---------- S3 ----------

def _read_rows(s3, bkt: str, key: str | None) -> list[dict]:
    if not key:
        return []
    return pq.read_table(io.BytesIO(s3.get_object(Bucket=bkt, Key=key)["Body"].read())).to_pylist()


def build(s3=None) -> dict:
    """Recalcula solo las fechas cuyas particiones de Silver cambiaron desde el ultimo build de Gold."""
    s3 = s3 or boto3.client("s3")
    bkt = bucket()
    silver, _ = read_json_etag(s3, bkt, SILVER_STATE_KEY)
    if not silver:
        raise ValueError("Silver no tiene estado, corre dl.silver primero")
    gold, etag = read_json_etag(s3, bkt, GOLD_STATE_KEY)
    gold = gold or {"datasets": {ds: {} for ds in SCHEMAS}, "inputs": {}}

    days = sorted(silver["datasets"]["news"])
    recomputed, conciliation = [], {}
    for day in days:
        inputs = {ds: silver["datasets"].get(ds, {}).get(day) for ds in SILVER_INPUTS}
        if gold["inputs"].get(day) == inputs:
            continue
        news, entities, phrases = (_read_rows(s3, bkt, inputs[ds]) for ds in SILVER_INPUTS)
        tables = aggregate_day(news, entities, phrases)
        conciliation[day] = reconcile(day, news, entities, phrases, tables)
        version = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()[:12]
        for ds, rows in tables.items():
            key = f"gold/{ds}/pub_date={day}/v={version}/part-00000.parquet"
            write_partition(s3, bkt, key, rows, SCHEMAS[ds])
            gold["datasets"][ds][day] = key
        gold["inputs"][day] = inputs
        recomputed.append(day)

    removed = sorted(set(gold["inputs"]) - set(days))
    for day in removed:
        gold["inputs"].pop(day)
        for ds in SCHEMAS:
            gold["datasets"][ds].pop(day, None)

    now = datetime.now(timezone.utc)
    report = {"built_at_utc": now.isoformat(timespec="seconds"), "silver_build_id": silver.get("build_id"),
              "fechas_recalculadas": recomputed, "fechas_eliminadas": removed,
              "fechas_sin_cambios": len(days) - len(recomputed), "conciliacion": conciliation}
    if recomputed or removed:
        gold.update(silver_build_id=silver.get("build_id"), updated_at_utc=report["built_at_utc"])
        fault_point("gold_antes_de_publicar")
        put_json_cas(s3, bkt, GOLD_STATE_KEY, gold, etag)
        put_json(s3, bkt, f"_control/gold/runs/{now.strftime('%Y%m%dT%H%M%SZ')}.json", report)
    log.info("task=gold recalculadas=%s eliminadas=%s sin_cambios=%s",
             recomputed, removed, report["fechas_sin_cambios"])
    return report


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Silver a Gold").parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("numexpr").setLevel(logging.WARNING)
    try:
        report = build()
    except ConcurrentUpdate as err:
        log.error("task=gold %s", err)
        return 3
    except ControlledFault as err:
        log.error("task=gold %s, no se publico nada", err)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
