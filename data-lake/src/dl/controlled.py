"""Copia controlada de un RSS de Bronze para demostrar escenarios de calidad y de cambios (seccion 4.5).

El enunciado permite usar una copia controlada de un RSS almacenado en Bronze para no depender de que el
medio publique una actualizacion durante la demostracion. La copia queda marcada en el registro del lote
con origen=copia_controlada y la ruta del original del que se derivo.
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import sys
from xml.etree import ElementTree as ET

import boto3
from defusedxml import ElementTree as SafeET

from dl.config import Source, bucket, load_sources, safe_run_id
from dl.ingest import ingest_all
from dl.s3util import read_json

log = logging.getLogger("dl.controlled")

MARK = " [actualizacion controlada EIA]"
FOREIGN_LINK = "https://noticias-ajenas.example/politica/nota-9999999"
DEFECTS = (
    ("titulo_vacio", {"title": ""}),
    ("fecha_invalida", {"pubDate": "ayer en la tarde"}),
    ("enlace_invalido", {"link": FOREIGN_LINK, "guid": FOREIGN_LINK}),
)


def _set(item, tag: str, text: str) -> None:
    node = item.find(tag)
    if node is None:
        node = ET.SubElement(item, tag)
    node.text = text


def build_controlled(xml: bytes) -> tuple[bytes, dict]:
    """Modifica el titulo del primer item, duplica el segundo y agrega tres items defectuosos al final."""
    root = SafeET.fromstring(xml, forbid_dtd=True)
    channel = root.find("channel")
    items = channel.findall("item")
    if len(items) < 2:
        raise ValueError("El feed de origen necesita al menos dos items")
    first, second = items[0], items[1]
    before = first.findtext("title") or ""
    _set(first, "title", before + MARK)

    channel.append(copy.deepcopy(second))
    defects = []
    for reason, changes in DEFECTS:
        bad = copy.deepcopy(second)
        for tag, text in changes.items():
            _set(bad, tag, text)
        channel.append(bad)
        defects.append({"motivo_esperado": reason, **changes})

    summary = {
        "modificada": {"link": first.findtext("link"), "titulo_antes": before, "titulo_despues": before + MARK},
        "duplicada": second.findtext("link"),
        "defectuosos": defects,
        "items_originales": len(items),
        "items_copia": len(channel.findall("item")),
    }
    return ET.tostring(root, encoding="utf-8", xml_declaration=True), summary


def run_controlled(s3, bkt: str, src: Source, from_run: str, run_id: str) -> dict:
    control = read_json(s3, bkt, f"_control/ingestion/run_id={from_run}/source={src.source_id}.json")
    if not control or control.get("status") != "ok":
        raise ValueError(f"{src.source_id} no tiene un original valido en el lote {from_run}")
    origin_uri = control["s3_uri"]
    xml = s3.get_object(Bucket=bkt, Key=origin_uri.split(f"s3://{bkt}/", 1)[1])["Body"].read()
    body, summary = build_controlled(xml)
    result = ingest_all([src], run_id, s3=s3, fetcher=lambda url: (200, "application/xml", body),
                        extra={"origen": "copia_controlada", "derivado_de": origin_uri})
    return {"derivado_de": origin_uri, "cambios": summary, "ingesta": result}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingiere una copia controlada de un RSS de Bronze como lote nuevo")
    p.add_argument("--from-run", required=True, help="lote de Bronze del que se copia el RSS")
    p.add_argument("--source", default="eltiempo_politica")
    p.add_argument("--run-id", required=True)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    sources = {s.source_id: s for s in load_sources()}
    if args.source not in sources:
        p.error(f"fuente desconocida {args.source}")
    out = run_controlled(boto3.client("s3"), bucket(), sources[args.source], args.from_run, safe_run_id(args.run_id))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out["ingesta"]["status"] == "complete" else 2


if __name__ == "__main__":
    sys.exit(main())
