"""Enriquecimiento con Amazon Comprehend, con cache inmutable por hash de texto."""
from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from dl.s3util import put_once, read_json

log = logging.getLogger("dl.enrich")

ENTITY_TYPES = {"PERSON": "persona", "ORGANIZATION": "organizacion", "LOCATION": "lugar"}
SUPPORTED_LANGUAGES = {"es"}
MIN_LANGUAGE_SCORE = 0.5
MAX_TEXT_BYTES = 5000
BATCH_SIZE = 25
CACHE_PREFIX = "cache/comprehend"
PENDING = {"error_api"}
WS = re.compile(r"\s+")


def comprehend_client():
    return boto3.client("comprehend", config=Config(retries={"max_attempts": 6, "mode": "adaptive"}))


def cache_key(text_hash: str) -> str:
    return f"{CACHE_PREFIX}/{text_hash[:2]}/{text_hash}.json"


def normalize(text: str) -> str:
    return WS.sub(" ", text).strip().casefold()


def precheck(text: str) -> str | None:
    if not text.strip():
        return "texto_vacio"
    if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
        return "texto_demasiado_largo"
    return None


def _result(text_hash: str, status: str, analyzed_at: str, language=None, score=None,
            entities=None, key_phrases=None, error=None) -> dict:
    return {"text_hash": text_hash, "status": status, "language": language, "language_score": score,
            "analyzed_at_utc": analyzed_at, "entities": entities or [], "key_phrases": key_phrases or [],
            "error": error}


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def analyze(nlp, texts: dict[str, str]) -> dict[str, dict]:
    """Analiza textos que no estan en cache (text_hash -> texto), en lotes de 25 por idioma."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    results: dict[str, dict] = {}
    candidates = []
    for h, text in texts.items():
        status = precheck(text)
        if status:
            results[h] = _result(h, status, now)
        else:
            candidates.append(h)

    detected: dict[str, tuple] = {}
    by_language: dict[str, list[str]] = defaultdict(list)
    for chunk in _chunks(candidates, BATCH_SIZE):
        resp = nlp.batch_detect_dominant_language(TextList=[texts[h] for h in chunk])
        for err in resp.get("ErrorList", []):
            h = chunk[err["Index"]]
            results[h] = _result(h, "error_api", now, error=err.get("ErrorCode"))
        for res in resp.get("ResultList", []):
            h = chunk[res["Index"]]
            best = max(res.get("Languages", []), key=lambda lang: lang["Score"], default=None)
            code, score = (best["LanguageCode"], best["Score"]) if best else (None, None)
            detected[h] = (code, score)
            if code in SUPPORTED_LANGUAGES and score >= MIN_LANGUAGE_SCORE:
                by_language[code].append(h)
            else:
                results[h] = _result(h, "idioma_no_soportado", now, code, score)

    for code, hashes in by_language.items():
        for chunk in _chunks(hashes, BATCH_SIZE):
            docs = [texts[h] for h in chunk]
            ents = nlp.batch_detect_entities(TextList=docs, LanguageCode=code)
            phrases = nlp.batch_detect_key_phrases(TextList=docs, LanguageCode=code)
            errors = {e["Index"]: e.get("ErrorCode")
                      for e in ents.get("ErrorList", []) + phrases.get("ErrorList", [])}
            ent_by_index = {r["Index"]: r["Entities"] for r in ents.get("ResultList", [])}
            kp_by_index = {r["Index"]: r["KeyPhrases"] for r in phrases.get("ResultList", [])}
            for i, h in enumerate(chunk):
                if i in errors:
                    results[h] = _result(h, "error_api", now, code, detected[h][1], error=errors[i])
                else:
                    results[h] = _result(h, "analizado", now, code, detected[h][1],
                                         ent_by_index[i], kp_by_index[i])
    return results


def enrich_rows(s3, bkt: str, rows: list[dict], nlp=None) -> tuple[dict[str, dict], Counter]:
    """Asigna el resultado de NLP a cada fila. Solo llama a Comprehend con textos que no estan en cache."""
    stats: Counter = Counter()
    results: dict[str, dict] = {}
    for h in sorted({r["text_hash"] for r in rows}):
        cached = read_json(s3, bkt, cache_key(h))
        if cached is not None:
            results[h] = cached
    missing = {r["text_hash"]: r["nlp_text"] for r in rows if r["text_hash"] not in results}
    stats["cache_hits"] = len(results)
    stats["textos_enviados"] = len(missing)
    if missing:
        fresh = analyze(nlp or comprehend_client(), missing)
        for h, res in fresh.items():
            if res["status"] not in PENDING:
                body = json.dumps({**res, "nlp_text": missing[h]}, ensure_ascii=False).encode("utf-8")
                put_once(s3, bkt, cache_key(h), body, "application/json")
        results.update(fresh)
    for r in rows:
        res = results[r["text_hash"]]
        r.update(nlp_status=res["status"], nlp_language=res["language"],
                 nlp_language_score=res["language_score"],
                 nlp_analyzed_at_utc=datetime.fromisoformat(res["analyzed_at_utc"]))
        stats[res["status"]] += 1
    return results, stats


def entity_rows(news_row: dict, result: dict) -> list[dict]:
    """Una fila por entidad distinta (tipo + texto normalizado) en la noticia."""
    out: dict[tuple[str, str], dict] = {}
    for e in result["entities"]:
        etype = ENTITY_TYPES.get(e["Type"])
        norm = normalize(e["Text"])
        if not etype or not norm:
            continue
        row = out.get((etype, norm))
        if row is None:
            out[(etype, norm)] = {"news_id": news_row["news_id"], "text_hash": news_row["text_hash"],
                                  "entity_type": etype, "entity_text": WS.sub(" ", e["Text"]).strip(),
                                  "entity_norm": norm, "score": float(e["Score"]), "mentions": 1}
        else:
            row["mentions"] += 1
            row["score"] = max(row["score"], float(e["Score"]))
    return sorted(out.values(), key=lambda r: (r["entity_type"], r["entity_norm"]))


def key_phrase_rows(news_row: dict, result: dict) -> list[dict]:
    """Una fila por frase clave distinta (texto normalizado) en la noticia."""
    out: dict[str, dict] = {}
    for k in result["key_phrases"]:
        norm = normalize(k["Text"])
        if not norm:
            continue
        row = out.get(norm)
        if row is None:
            out[norm] = {"news_id": news_row["news_id"], "text_hash": news_row["text_hash"],
                         "phrase_text": WS.sub(" ", k["Text"]).strip(), "phrase_norm": norm,
                         "score": float(k["Score"]), "mentions": 1}
        else:
            row["mentions"] += 1
            row["score"] = max(row["score"], float(k["Score"]))
    return sorted(out.values(), key=lambda r: r["phrase_norm"])
