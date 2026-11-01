"""Parseo y limpieza de items RSS: de Bronze a registros candidatos de Silver."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from defusedxml import ElementTree as SafeET

BOGOTA = ZoneInfo("America/Bogota")
ALLOWED_HOSTS = ("eltiempo.com",)
NUMERIC_ID = re.compile(r"-(\d{5,})/?$")
WS = re.compile(r"\s+")
FIELDS = ("title", "link", "guid", "description", "pubDate", "category")


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs) -> None:
        self.parts.append(" ")

    def handle_endtag(self, tag) -> None:
        self.parts.append(" ")

    def handle_data(self, data) -> None:
        self.parts.append(data)


def clean_text(raw: str | None) -> str:
    if not raw:
        return ""
    extractor = _TextExtractor()
    extractor.feed(raw)
    extractor.close()
    return WS.sub(" ", "".join(extractor.parts)).strip()


def normalize_link(link: str) -> str:
    p = urlsplit(link.strip())
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/"), "", ""))


def news_id_for(link: str) -> tuple[str, str]:
    match = NUMERIC_ID.search(urlsplit(link).path)
    if match:
        return match.group(1), "url_numeric_id"
    return "h_" + hashlib.sha256(link.encode("utf-8")).hexdigest()[:16], "link_hash"


def parse_date(raw: str | None) -> datetime | None:
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        try:
            dt = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def _host_allowed(host: str) -> bool:
    return any(host == h or host.endswith("." + h) for h in ALLOWED_HOSTS)


def _build(raw: dict, ingested_at: datetime) -> tuple[str | None, dict]:
    title = clean_text(raw["title"])
    description = clean_text(raw["description"])
    link_src = raw["link"].strip() or raw["guid"].strip()
    if not title:
        return "titulo_vacio", {}
    if not link_src:
        return "enlace_ausente", {}
    parts = urlsplit(link_src)
    if parts.scheme != "https" or not _host_allowed(parts.netloc.lower()):
        return "enlace_invalido", {}
    published = parse_date(raw["pubDate"])
    if published is None:
        return "fecha_invalida", {}
    if published > ingested_at + timedelta(days=1) or published.year < 2000:
        return "fecha_fuera_de_rango", {}

    link = normalize_link(link_src)
    news_id, id_strategy = news_id_for(link)
    nlp_text = f"{title}. {description}" if description else title
    return None, {
        "news_id": news_id,
        "id_strategy": id_strategy,
        "link": link,
        "guid": raw["guid"].strip(),
        "title": title,
        "description": description,
        "category": clean_text(raw["category"]),
        "pub_date_raw": raw["pubDate"].strip(),
        "published_at_utc": published,
        "pub_date": published.astimezone(BOGOTA).date(),
        "nlp_text": nlp_text,
        "text_hash": hashlib.sha256(nlp_text.encode("utf-8")).hexdigest(),
    }


def parse_feed(xml: bytes, *, source_id: str, medio: str, seccion: str, run_id: str,
               bronze_uri: str, ingested_at: datetime) -> tuple[list[dict], list[dict]]:
    root = SafeET.fromstring(xml, forbid_dtd=True)
    valid, rejected = [], []
    for pos, item in enumerate(root.findall("./channel/item"), start=1):
        raw = {f: item.findtext(f) or "" for f in FIELDS}
        lineage = {"source_id": source_id, "medio": medio, "seccion": seccion,
                   "bronze_run_id": run_id, "bronze_uri": bronze_uri, "item_position": pos}
        reason, record = _build(raw, ingested_at)
        if reason:
            rejected.append({**lineage, "rejection_reason": reason, "raw": raw})
        else:
            valid.append({**lineage, **record})
    return valid, rejected
