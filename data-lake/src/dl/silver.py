"""Capa Silver: de un lote de Bronze a noticias vigentes enriquecidas, en Parquet."""
from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from dl.config import ControlledFault, bucket, fault_point, safe_run_id
from dl.enrich import PENDING, enrich_rows, entity_rows, key_phrase_rows
from dl.parse import parse_feed
from dl.s3util import ConcurrentUpdate, put_json, put_json_cas, read_json, read_json_etag

log = logging.getLogger("dl.silver")

STATE_KEY = "_control/silver/state.json"
STAGING = "_staging/silver"
OK = {"ok", "ok_no_items"}
KEEP_ON_UPDATE = ("source_id", "medio", "seccion", "first_seen_run_id")
TS_FIELDS = ("published_at_utc", "content_ingested_at_utc", "nlp_analyzed_at_utc")
DATASETS = ("news", "news_entities", "news_key_phrases")

NEWS_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("id_strategy", pa.string()),
    ("source_id", pa.string()),
    ("medio", pa.string()),
    ("seccion", pa.string()),
    ("sources_seen", pa.list_(pa.string())),
    ("link", pa.string()),
    ("guid", pa.string()),
    ("title", pa.string()),
    ("description", pa.string()),
    ("category", pa.string()),
    ("pub_date_raw", pa.string()),
    ("published_at_utc", pa.timestamp("ms")),
    ("nlp_text", pa.string()),
    ("text_hash", pa.string()),
    ("version", pa.int32()),
    ("first_seen_run_id", pa.string()),
    ("content_run_id", pa.string()),
    ("content_ingested_at_utc", pa.timestamp("ms")),
    ("bronze_uri", pa.string()),
    ("item_position", pa.int32()),
    ("nlp_status", pa.string()),
    ("nlp_language", pa.string()),
    ("nlp_language_score", pa.float64()),
    ("nlp_analyzed_at_utc", pa.timestamp("ms")),
])

ENTITY_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("text_hash", pa.string()),
    ("entity_type", pa.string()),
    ("entity_text", pa.string()),
    ("entity_norm", pa.string()),
    ("score", pa.float64()),
    ("mentions", pa.int32()),
])

KEY_PHRASE_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("text_hash", pa.string()),
    ("phrase_text", pa.string()),
    ("phrase_norm", pa.string()),
    ("score", pa.float64()),
    ("mentions", pa.int32()),
])

SCHEMAS = {"news": NEWS_SCHEMA, "news_entities": ENTITY_SCHEMA, "news_key_phrases": KEY_PHRASE_SCHEMA}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------- logica pura: sin S3, se prueba con pytest ----------

def dedup_batch(valid: list[dict]) -> tuple[dict[str, dict], int]:
    """Una fila por news_id dentro del lote. Devuelve (noticias unicas, duplicados descartados)."""
    unique: dict[str, dict] = {}
    duplicates = 0
    for r in sorted(valid, key=lambda r: (r["source_id"], r["item_position"])):
        current = unique.get(r["news_id"])
        if current is None:
            unique[r["news_id"]] = {**r, "sources_seen": [r["source_id"]]}
        else:
            duplicates += 1
            current["sources_seen"] = sorted(set(current["sources_seen"]) | {r["source_id"]})
    return unique, duplicates


def merge(state: dict[str, dict], batch: dict[str, dict], run_id: str) -> tuple[Counter, set[date]]:
    """Fusiona el lote con el estado vigente. Modifica state y devuelve (conteos, particiones tocadas)."""
    stats = Counter({"nuevas": 0, "repetidas": 0, "actualizadas": 0, "obsoletas": 0})
    touched: set[date] = set()
    for news_id, r in sorted(batch.items()):
        current = state.get(news_id)
        if current is None:
            state[news_id] = {**r, "version": 1, "first_seen_run_id": run_id,
                              "content_run_id": run_id, "content_ingested_at_utc": r["ingested_at"]}
            stats["nuevas"] += 1
            touched.add(r["pub_date"])
            continue

        sources = sorted(set(current["sources_seen"]) | set(r["sources_seen"]))
        changed = r["text_hash"] != current["text_hash"]
        if changed and r["ingested_at"] > current["content_ingested_at_utc"]:
            updated = {**r, **{k: current[k] for k in KEEP_ON_UPDATE}}
            updated.update(sources_seen=sources, version=current["version"] + 1,
                           content_run_id=run_id, content_ingested_at_utc=r["ingested_at"])
            state[news_id] = updated
            stats["actualizadas"] += 1
            touched |= {current["pub_date"], r["pub_date"]}
            continue

        stats["obsoletas" if changed else "repetidas"] += 1
        if sources != current["sources_seen"]:
            current["sources_seen"] = sources
            touched.add(current["pub_date"])
    return stats, touched


def check_invariants(report: dict) -> None:
    r = report
    assert r["items_recibidos"] == r["validos"] + r["rechazados"], "recibidos != validos + rechazados"
    assert r["validos"] == r["noticias_unicas_en_lote"] + r["duplicados_en_lote"], "validos != unicas + duplicados"
    assert r["noticias_unicas_en_lote"] == (r["nuevas"] + r["repetidas"] + r["actualizadas"]
                                            + r["obsoletas"]), "unicas != suma de clasificacion"


# ---------- lectura y escritura en S3 ----------

def _naive_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def write_partition(s3, bkt: str, key: str, rows: list[dict], schema: pa.Schema) -> None:
    prepared = []
    for r in rows:
        row = {f.name: r.get(f.name) for f in schema}
        for k in TS_FIELDS:
            if row.get(k) is not None:
                row[k] = _naive_utc(row[k])
        prepared.append(row)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(prepared, schema=schema), buf, compression="snappy")
    s3.put_object(Bucket=bkt, Key=key, Body=buf.getvalue(), ContentType="application/vnd.apache.parquet")


def _read_news(s3, bkt: str, key: str, day: str) -> list[dict]:
    """Lee una particion de noticias y restaura la zona UTC, que Parquet guarda sin zona."""
    body = s3.get_object(Bucket=bkt, Key=key)["Body"].read()
    rows = pq.read_table(io.BytesIO(body)).to_pylist()
    for r in rows:
        for k in TS_FIELDS:
            if r.get(k) is not None:
                r[k] = r[k].replace(tzinfo=timezone.utc)
        r["pub_date"] = date.fromisoformat(day)
    return rows


def load_current(s3, bkt: str, state: dict) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for day, key in state["datasets"]["news"].items():
        for r in _read_news(s3, bkt, key, day):
            rows[r["news_id"]] = r
    return rows


def load_batch(s3, bkt: str, run_id: str) -> tuple[dict, list[dict], list[dict], int, list[str]]:
    run = read_json(s3, bkt, f"_control/ingestion/run_id={run_id}/_run.json")
    if run is None:
        raise ValueError(f"No existe el lote de Bronze {run_id}")
    valid, rejected, received, sources = [], [], 0, []
    for source_id, info in sorted(run["sources"].items()):
        if info["status"] not in OK:
            continue
        raw_key = info["s3_uri"].split(f"s3://{bkt}/", 1)[1]
        meta = read_json(s3, bkt, raw_key.rsplit("/", 1)[0] + "/_meta.json")
        ingested_at = datetime.fromisoformat(meta["ingested_at_utc"])
        xml = s3.get_object(Bucket=bkt, Key=raw_key)["Body"].read()
        ok, bad = parse_feed(xml, source_id=source_id, medio=meta["medio"], seccion=meta["seccion"],
                             run_id=run_id, bronze_uri=info["s3_uri"], ingested_at=ingested_at)
        if len(ok) + len(bad) != meta["item_count"]:
            raise ValueError(f"{source_id}: Bronze reporta {meta['item_count']} items y se parsearon {len(ok) + len(bad)}")
        for r in ok:
            r["ingested_at"] = ingested_at
        valid += ok
        rejected += bad
        received += meta["item_count"]
        sources.append(source_id)
    return run, valid, rejected, received, sources


def build_id() -> str:
    """Id unico por build: nunca coincide con el vigente aunque se cree en el mismo segundo."""
    return "b" + utc_now().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def new_state(build: str) -> dict:
    return {"build_id": build, "datasets": {ds: {} for ds in DATASETS}, "processed_runs": {}}


# ---------- etapas: preparar (3), enriquecer (4) y publicar (5) ----------

INTERNAL = ("plan_id", "base_etag", "force", "stage", "archivos", "dias_vacios", "dias_con_filas")


def prepare(s3, bkt: str, state: dict, run_id: str) -> tuple[dict, dict[str, list[dict]]]:
    """Parseo, limpieza, validacion, deduplicacion y clasificacion contra el estado vigente. No publica nada."""
    for ds in DATASETS:
        state["datasets"].setdefault(ds, {})
    run, valid, rejected, received, sources = load_batch(s3, bkt, run_id)
    batch, duplicates = dedup_batch(valid)
    current = load_current(s3, bkt, state)
    stats, touched = merge(current, batch, run_id)

    by_day: dict[date, list[dict]] = defaultdict(list)
    for row in current.values():
        by_day[row["pub_date"]].append(row)
    retry_days = {d for d, rows in by_day.items()
                  if any(r.get("nlp_status") is None or r.get("nlp_status") in PENDING for r in rows)}
    to_write = touched | retry_days

    if rejected:
        lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in rejected) + "\n"
        s3.put_object(Bucket=bkt, Key=f"quarantine/silver/run_id={run_id}/rechazos.jsonl",
                      Body=lines.encode("utf-8"), ContentType="application/x-ndjson")

    rows_by_day = {d.isoformat(): sorted(by_day[d], key=lambda r: r["news_id"])
                   for d in sorted(to_write) if by_day.get(d)}
    plan = {
        "plan_id": uuid.uuid4().hex,
        "run_id": run_id,
        "build_id": state["build_id"],
        "bronze_status": run["status"],
        "fuentes_procesadas": sources,
        "items_recibidos": received,
        "validos": len(valid),
        "rechazados": len(rejected),
        "rechazos_por_motivo": dict(Counter(r["rejection_reason"] for r in rejected)),
        "duplicados_en_lote": duplicates,
        "noticias_unicas_en_lote": len(batch),
        **stats,
        "particiones_afectadas": sorted(d.isoformat() for d in touched),
        "particiones_escritas": sorted(d.isoformat() for d in to_write),
        "noticias_vigentes_total": len(current),
        "dias_con_filas": sorted(rows_by_day),
        "dias_vacios": sorted(d.isoformat() for d in to_write if not by_day.get(d)),
    }
    check_invariants(plan)
    return plan, rows_by_day


def enrich_and_write(s3, bkt: str, plan: dict, rows_by_day: dict[str, list[dict]], nlp=None) -> None:
    """Enriquece con Comprehend (con cache) y escribe cada particion en una ruta versionada nueva."""
    nlp_stats: Counter = Counter()
    entity_count = phrase_count = 0
    files: dict[str, dict[str, str]] = {ds: {} for ds in DATASETS}
    for day in plan["dias_con_filas"]:
        rows = rows_by_day[day]
        results, day_stats = enrich_rows(s3, bkt, rows, nlp)
        nlp_stats.update(day_stats)
        entities = [e for r in rows for e in entity_rows(r, results[r["text_hash"]])]
        phrases = [k for r in rows for k in key_phrase_rows(r, results[r["text_hash"]])]
        entity_count += len(entities)
        phrase_count += len(phrases)
        for ds, data in (("news", rows), ("news_entities", entities), ("news_key_phrases", phrases)):
            key = f"silver/{ds}/pub_date={day}/v={plan['build_id']}-{plan['run_id']}/part-00000.parquet"
            write_partition(s3, bkt, key, data, SCHEMAS[ds])
            files[ds][day] = key
    plan.update(archivos=files, nlp=dict(nlp_stats), entidades_en_particiones_escritas=entity_count,
                frases_en_particiones_escritas=phrase_count)


def commit(state: dict, plan: dict) -> dict:
    """Apunta el manifiesto en memoria a los archivos nuevos y registra el lote. Devuelve el reporte."""
    for ds, days in plan["archivos"].items():
        state["datasets"].setdefault(ds, {}).update(days)
    for day in plan["dias_vacios"]:
        for ds in DATASETS:
            state["datasets"].setdefault(ds, {}).pop(day, None)
    report = {k: v for k, v in plan.items() if k not in INTERNAL}
    report["processed_at_utc"] = utc_now().isoformat(timespec="seconds")
    summary = {k: report[k] for k in ("processed_at_utc", "build_id", "nuevas", "repetidas",
                                      "actualizadas", "obsoletas", "rechazados")}
    summary["textos_enviados_comprehend"] = plan["nlp"].get("textos_enviados", 0)
    summary["plan_id"] = plan["plan_id"]
    entry = state["processed_runs"].get(plan["run_id"])
    if entry is None or "primero" not in entry:
        state["processed_runs"][plan["run_id"]] = {"primero": summary, "ultimo": summary, "procesamientos": 1}
    else:
        entry.update(ultimo=summary, procesamientos=entry["procesamientos"] + 1)
    return report


def apply_run(s3, bkt: str, state: dict, run_id: str, nlp=None) -> dict:
    """Las tres etapas en memoria, sin confirmar el estado. La usan process_run y rebuild."""
    plan, rows_by_day = prepare(s3, bkt, state, run_id)
    enrich_and_write(s3, bkt, plan, rows_by_day, nlp)
    report = commit(state, plan)
    log.info("task=silver run_id=%s aplicado build=%s nuevas=%s repetidas=%s actualizadas=%s obsoletas=%s "
             "rechazados=%s particiones=%s comprehend_textos=%s cache_hits=%s", run_id, report["build_id"],
             report["nuevas"], report["repetidas"], report["actualizadas"], report["obsoletas"],
             report["rechazados"], report["particiones_escritas"], report["nlp"].get("textos_enviados", 0),
             report["nlp"].get("cache_hits", 0))
    return report


def report_key(report: dict) -> str:
    stamp = datetime.fromisoformat(report["processed_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    return f"_control/silver/run_id={report['run_id']}/{report['build_id']}__{stamp}.json"


def process_run(run_id: str, s3=None, force: bool = False, nlp=None) -> dict:
    """Procesa un lote completo sobre el estado vigente y confirma con compare-and-swap."""
    s3 = s3 or boto3.client("s3")
    bkt = bucket()
    state, etag = read_json_etag(s3, bkt, STATE_KEY)
    state = state or new_state(build_id())
    state.setdefault("build_id", "b0")
    if run_id in state["processed_runs"] and not force:
        log.info("task=silver run_id=%s ya procesado, se omite", run_id)
        return {**state["processed_runs"][run_id], "skipped": True}
    report = apply_run(s3, bkt, state, run_id, nlp)
    fault_point("silver_antes_de_publicar", f"run_id={run_id}")
    put_json_cas(s3, bkt, STATE_KEY, state, etag)
    put_json(s3, bkt, report_key(report), report)
    log.info("task=silver run_id=%s publicado build=%s", run_id, state["build_id"])
    return report


# ---------- etapas separadas para Airflow, con el plan guardado en S3 entre tareas ----------

def plan_key(run_id: str) -> str:
    return f"{STAGING}/run_id={run_id}/plan.json"


def staged_key(run_id: str, day: str) -> str:
    return f"{STAGING}/run_id={run_id}/news/pub_date={day}.parquet"


def stage_prepare(run_id: str, s3=None, force: bool = False) -> dict:
    s3 = s3 or boto3.client("s3")
    bkt = bucket()
    state, etag = read_json_etag(s3, bkt, STATE_KEY)
    state = state or new_state(build_id())
    if run_id in state["processed_runs"] and not force:
        plan = {"run_id": run_id, "stage": "omitido", "motivo": "el lote ya esta publicado en Silver"}
        put_json(s3, bkt, plan_key(run_id), plan)
        log.info("task=silver_preparar run_id=%s ya procesado, se omite", run_id)
        return plan
    plan, rows_by_day = prepare(s3, bkt, state, run_id)
    for day, rows in rows_by_day.items():
        write_partition(s3, bkt, staged_key(run_id, day), rows, NEWS_SCHEMA)
    plan.update(base_etag=etag, force=force, stage="preparado")
    put_json(s3, bkt, plan_key(run_id), plan)
    log.info("task=silver_preparar run_id=%s recibidos=%s validos=%s rechazados=%s duplicados=%s nuevas=%s "
             "repetidas=%s actualizadas=%s obsoletas=%s particiones=%s", run_id, plan["items_recibidos"],
             plan["validos"], plan["rechazados"], plan["duplicados_en_lote"], plan["nuevas"], plan["repetidas"],
             plan["actualizadas"], plan["obsoletas"], plan["particiones_escritas"])
    return plan


def _load_plan(s3, bkt: str, run_id: str) -> dict:
    plan = read_json(s3, bkt, plan_key(run_id))
    if plan is None:
        raise ValueError(f"No hay plan para el lote {run_id}, corre primero la etapa preparar")
    return plan


def stage_enrich(run_id: str, s3=None, nlp=None) -> dict:
    s3 = s3 or boto3.client("s3")
    bkt = bucket()
    plan = _load_plan(s3, bkt, run_id)
    if plan["stage"] in ("omitido", "publicado"):
        log.info("task=silver_enriquecer run_id=%s etapa=%s, no hay nada que enriquecer", run_id, plan["stage"])
        return plan
    rows_by_day = {day: _read_news(s3, bkt, staged_key(run_id, day), day) for day in plan["dias_con_filas"]}
    enrich_and_write(s3, bkt, plan, rows_by_day, nlp)
    plan["stage"] = "enriquecido"
    put_json(s3, bkt, plan_key(run_id), plan)
    log.info("task=silver_enriquecer run_id=%s comprehend_textos=%s cache_hits=%s particiones=%s", run_id,
             plan["nlp"].get("textos_enviados", 0), plan["nlp"].get("cache_hits", 0), plan["dias_con_filas"])
    return plan


def stage_publish(run_id: str, s3=None) -> dict:
    s3 = s3 or boto3.client("s3")
    bkt = bucket()
    plan = _load_plan(s3, bkt, run_id)
    if plan["stage"] == "omitido":
        log.info("task=silver_publicar run_id=%s omitido, el lote ya estaba publicado", run_id)
        return plan
    state, etag = read_json_etag(s3, bkt, STATE_KEY)
    entry = (state or {}).get("processed_runs", {}).get(run_id, {})
    if entry.get("ultimo", {}).get("plan_id") == plan["plan_id"]:
        log.info("task=silver_publicar run_id=%s este plan ya esta publicado, se omite", run_id)
        return plan
    if plan["stage"] != "enriquecido":
        raise ValueError(f"El lote {run_id} esta en la etapa {plan['stage']}, falta enriquecer")
    if etag != plan["base_etag"]:
        raise ConcurrentUpdate(f"{STATE_KEY} cambio desde que se preparo {run_id}, repite desde la etapa preparar")
    state = state or new_state(plan["build_id"])
    report = commit(state, plan)
    fault_point("silver_antes_de_publicar", f"run_id={run_id}")
    put_json_cas(s3, bkt, STATE_KEY, state, etag)
    put_json(s3, bkt, report_key(report), report)
    plan["stage"] = "publicado"
    put_json(s3, bkt, plan_key(run_id), plan)
    log.info("task=silver_publicar run_id=%s publicado build=%s nuevas=%s actualizadas=%s vigentes=%s", run_id,
             report["build_id"], report["nuevas"], report["actualizadas"], report["noticias_vigentes_total"])
    return report


def bronze_runs(s3, bkt: str) -> list[str]:
    """Todos los lotes registrados en Bronze, en orden cronologico de ingesta."""
    found = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bkt, Prefix="_control/ingestion/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            run_id = cp["Prefix"].split("run_id=", 1)[1].rstrip("/")
            start = read_json(s3, bkt, f"_control/ingestion/run_id={run_id}/_start.json")
            found.append((start["run_ts_utc"], run_id))
    return [run_id for _, run_id in sorted(found)]


def pending_runs(s3, bkt: str) -> list[str]:
    state, _ = read_json_etag(s3, bkt, STATE_KEY)
    done = set((state or {}).get("processed_runs", {}))
    return [r for r in bronze_runs(s3, bkt) if r not in done]


def rebuild(s3=None, nlp=None) -> list[dict]:
    """Reconstruye Silver completo desde Bronze en un build nuevo y lo confirma de una sola vez."""
    s3 = s3 or boto3.client("s3")
    bkt = bucket()
    _, etag = read_json_etag(s3, bkt, STATE_KEY)
    state = new_state(build_id())
    reports = [apply_run(s3, bkt, state, run_id, nlp) for run_id in bronze_runs(s3, bkt)]
    fault_point("silver_antes_de_publicar", "rebuild")
    put_json_cas(s3, bkt, STATE_KEY, state, etag)
    for report in reports:
        put_json(s3, bkt, report_key(report), report)
    log.info("task=silver rebuild build=%s lotes=%s vigentes=%s", state["build_id"], len(reports),
             reports[-1]["noticias_vigentes_total"] if reports else 0)
    return reports


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Bronze a Silver")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id")
    group.add_argument("--pending", action="store_true", help="procesa en orden los lotes sin procesar")
    group.add_argument("--rebuild", action="store_true", help="reconstruye Silver completo desde Bronze")
    p.add_argument("--stage", choices=("preparar", "enriquecer", "publicar"),
                   help="ejecuta una sola etapa del lote (la usa Airflow)")
    p.add_argument("--force", action="store_true", help="reprocesa aunque el lote ya este registrado")
    args = p.parse_args(argv)
    if args.stage and not args.run_id:
        p.error("--stage requiere --run-id")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("numexpr").setLevel(logging.WARNING)

    s3 = boto3.client("s3")
    try:
        if args.rebuild:
            reports = rebuild(s3=s3)
        elif args.stage:
            run_id = safe_run_id(args.run_id)
            stages = {"preparar": lambda: stage_prepare(run_id, s3=s3, force=args.force),
                      "enriquecer": lambda: stage_enrich(run_id, s3=s3),
                      "publicar": lambda: stage_publish(run_id, s3=s3)}
            reports = [stages[args.stage]()]
        else:
            run_ids = pending_runs(s3, bucket()) if args.pending else [safe_run_id(args.run_id)]
            if not run_ids:
                log.info("task=silver no hay lotes pendientes")
            reports = [process_run(r, s3=s3, force=args.force) for r in run_ids]
    except ConcurrentUpdate as err:
        log.error("task=silver %s", err)
        return 3
    except ControlledFault as err:
        log.error("task=silver %s, no se publico nada", err)
        return 1
    for r in reports:
        print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
