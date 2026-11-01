"""Ingesta de feeds RSS a la capa Bronze."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from xml.etree.ElementTree import ParseError

import boto3
import requests
import urllib3
from defusedxml import ElementTree as SafeET
from defusedxml.common import DefusedXmlException

from dl.config import DEFAULT_SOURCES, Source, bucket, load_sources, safe_run_id
from dl.s3util import head, put_json, put_once, read_json

log = logging.getLogger("dl.ingest")

USER_AGENT = "arq-datos-eia/0.1 (proyecto academico)"
TIMEOUT_S = (5, 20)
MAX_BYTES = 5 * 1024 * 1024
OK = {"ok", "ok_no_items"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def fetch(url: str) -> tuple[int, str, bytes]:
    with requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT_S, stream=True) as r:
        body = r.raw.read(MAX_BYTES + 1, decode_content=True)
        return r.status_code, r.headers.get("Content-Type", ""), body


def validate(body: bytes) -> tuple[str, int, str | None]:
    if not body.strip():
        return "empty_body", 0, "respuesta sin contenido"
    if len(body) > MAX_BYTES:
        return "too_large", 0, f"respuesta mayor a {MAX_BYTES} bytes"
    try:
        root = SafeET.fromstring(body, forbid_dtd=True)
    except (ParseError, DefusedXmlException) as err:
        return "malformed_xml", 0, f"{type(err).__name__}: {err}"[:300]
    if root.tag != "rss" or root.find("channel") is None:
        return "malformed_xml", 0, f"raiz inesperada <{root.tag}>"
    items = len(root.findall("./channel/item"))
    return ("ok" if items else "ok_no_items"), items, None


def start_run(s3, bkt: str, run_id: str) -> datetime:
    """Fija la fecha del lote en el primer intento y la reutiliza en los reintentos."""
    key = f"_control/ingestion/run_id={run_id}/_start.json"
    now = utc_now()
    body = json.dumps({"run_id": run_id, "run_ts_utc": iso(now)}).encode()
    if put_once(s3, bkt, key, body, "application/json"):
        return now
    return datetime.fromisoformat(read_json(s3, bkt, key)["run_ts_utc"])


def ingest_source(s3, bkt: str, src: Source, run_id: str, run_ts: datetime, attempt: int = 1,
                  fetcher=None, extra: dict | None = None) -> dict:
    day = run_ts.date().isoformat()
    part = f"source={src.source_id}/ingestion_date={day}/run_id={run_id}"
    raw_key = f"bronze/{part}/feed.xml"
    meta_key = f"bronze/{part}/_meta.json"
    control_key = f"_control/ingestion/run_id={run_id}/source={src.source_id}.json"
    ctx = f"task=ingest source={src.source_id} run_id={run_id}"

    previous = read_json(s3, bkt, control_key)
    if previous and previous["status"] in OK:
        log.info("%s ya ingerido, se omite", ctx)
        return {**previous, "skipped": True}

    rec = {"run_id": run_id, "source_id": src.source_id, "medio": src.medio,
           "seccion": src.seccion, "url": src.url, "ingestion_date": day, "attempt": attempt,
           **(extra or {})}

    stored = head(s3, bkt, raw_key)
    if stored:
        body = s3.get_object(Bucket=bkt, Key=raw_key)["Body"].read()
        rec.update(http_status=200, ingested_at_utc=iso(stored["LastModified"]),
                   note="original ya existia en Bronze, registro completado sin consultar la fuente")
    else:
        try:
            code, ctype, body = (fetcher or fetch)(src.url)
        except (requests.RequestException, urllib3.exceptions.HTTPError) as err:
            rec.update(status="network_error", http_status=None, error=f"{type(err).__name__}: {err}"[:300])
            put_json(s3, bkt, control_key, rec)
            log.error("%s status=network_error %s", ctx, rec["error"])
            return rec
        rec.update(http_status=code, content_type=ctype, ingested_at_utc=iso(utc_now()))
        if code != 200:
            rec.update(status="http_error", error=f"HTTP {code}", bytes=len(body))
            put_json(s3, bkt, control_key, rec)
            log.error("%s status=http_error code=%s", ctx, code)
            return rec

    status, items, err = validate(body)
    rec.update(status=status, item_count=items, bytes=len(body),
               sha256=hashlib.sha256(body).hexdigest(), error=err)

    if status in OK:
        if not put_once(s3, bkt, raw_key, body, "application/xml"):
            body = s3.get_object(Bucket=bkt, Key=raw_key)["Body"].read()
            rec.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                       note="otro proceso escribio primero, se conserva su original")
        rec["s3_uri"] = f"s3://{bkt}/{raw_key}"
        put_once(s3, bkt, meta_key, json.dumps(rec, ensure_ascii=False, indent=2).encode(), "application/json")
    else:
        q_key = f"quarantine/bronze/{part}/response.bin"
        s3.put_object(Bucket=bkt, Key=q_key, Body=body)
        rec["s3_uri"] = f"s3://{bkt}/{q_key}"

    put_json(s3, bkt, control_key, rec)
    level = logging.INFO if status in OK else logging.ERROR
    log.log(level, "%s status=%s items=%s sha256=%s", ctx, status, items, rec["sha256"][:12])
    return rec


def run_status(results: list[dict]) -> str:
    ok = sum(r["status"] in OK for r in results)
    if ok == len(results):
        return "complete"
    return "partial" if ok else "failed"


def ingest_all(sources: list[Source], run_id: str, attempt: int = 1, s3=None,
               fetcher=None, extra: dict | None = None) -> dict:
    s3 = s3 or boto3.client("s3")
    bkt = bucket()
    run_ts = start_run(s3, bkt, run_id)
    results = [ingest_source(s3, bkt, src, run_id, run_ts, attempt, fetcher, extra) for src in sources]
    summary = {
        "run_id": run_id,
        "run_ts_utc": iso(run_ts),
        "finished_at_utc": iso(utc_now()),
        "status": run_status(results),
        "sources": {r["source_id"]: {k: r.get(k) for k in ("status", "http_status", "item_count", "s3_uri")}
                    for r in results},
    }
    put_json(s3, bkt, f"_control/ingestion/run_id={run_id}/_run.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingesta RSS a Bronze")
    p.add_argument("--run-id")
    p.add_argument("--config", type=Path, default=DEFAULT_SOURCES)
    p.add_argument("--attempt", type=int, default=1, help="numero de intento, Airflow pasa try_number")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    run_id = safe_run_id(args.run_id or "manual__" + utc_now().strftime("%Y%m%dT%H%M%SZ"))
    summary = ingest_all(load_sources(args.config), run_id, attempt=args.attempt)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return {"complete": 0, "partial": 2, "failed": 1}[summary["status"]]


if __name__ == "__main__":
    sys.exit(main())
