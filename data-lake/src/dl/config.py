"""Configuracion del data lake: fuentes RSS y nombres de recursos."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

DL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCES = DL_ROOT / "config" / "sources.json"


@dataclass(frozen=True)
class Source:
    source_id: str
    medio: str
    seccion: str
    url: str


def load_sources(path: Path = DEFAULT_SOURCES) -> list[Source]:
    sources = [Source(**d) for d in json.loads(Path(path).read_text(encoding="utf-8"))]
    ids = [s.source_id for s in sources]
    if len(ids) != len(set(ids)):
        raise ValueError("source_id repetido en la configuracion")
    for s in sources:
        if not re.fullmatch(r"[a-z0-9_]+", s.source_id):
            raise ValueError(f"source_id invalido: {s.source_id!r}")
        if not s.url.startswith("https://"):
            raise ValueError(f"La fuente {s.source_id} no usa https")
    return sources


def bucket() -> str:
    name = os.environ.get("DL_BUCKET")
    if not name:
        raise RuntimeError("Falta la variable DL_BUCKET")
    return name


def safe_run_id(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", raw)


class ControlledFault(RuntimeError):
    """Fallo inyectado a proposito para demostrar recuperacion. Se activa con la variable DL_FAULT."""


def fault_point(name: str, context: str = "") -> None:
    if os.environ.get("DL_FAULT") == name:
        raise ControlledFault(f"fallo controlado en {name} {context}".strip())
