#!/usr/bin/env python3
"""Extraccion por lote de RDS MySQL y DocumentDB a s3://<DW_BUCKET>/raw/<fuente>/<entidad>/batch_id=<id>/.

Un lote pasa por cuatro pasos y cada uno deja una huella en S3, asi un reintento continua donde quedo:

  1. plan.json        La ventana del lote: por entidad, desde la marca de agua anterior (menos un solape)
                      hasta la hora de la fuente al planificar. Se escribe una sola vez, asi todos los
                      reintentos del mismo lote leen exactamente la misma ventana.
  2. Parquet          Un archivo por entidad, creado solo si no existe. Un reintento conserva los archivos
                      de entidades que ya habian terminado y extrae solo las que faltan.
  3. batch.json y     El registro del lote (fecha, filas por entidad, ventana, bytes) y un manifiesto de
     manifiestos COPY COPY por entidad con el content_length que Redshift exige para Parquet.
  4. state.json       La marca de agua por entidad. Es el ultimo paso y se hace con compare-and-swap:
                      un lote solo cuenta como extraido cuando aparece en el historial de state.json.

Codigos de salida: 0 extraido, 99 el lote ya estaba extraido (no se toca nada), 1 error.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import s3util
from .entities import ENTITIES, SOURCES, Entity, by_source

UTC = timezone.utc
STATE_KEY = "_control/extraction/state.json"
BATCH_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
EXIT_YA_EXTRAIDO = 99
EMPTY_STATE = {"seq": 0, "ultimo_lote": None, "marcas": {}, "historial": []}
CHUNK = 5000


class StaleBatch(RuntimeError):
    """El lote se planifico sobre una marca de agua que otro lote ya avanzo."""


class ControlledFault(RuntimeError):
    """Fallo inyectado a proposito con DW_FAULT para demostrar la recuperacion."""


def fault_point(name: str, context: str = "") -> None:
    if os.environ.get("DW_FAULT") == name:
        raise ControlledFault(f"fallo controlado en {name} {context}".strip())


def safe_batch_id(raw: str) -> str:
    """Convierte un run_id de Airflow (con ':' y '+') en un identificador valido para rutas de S3."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", raw)[:128]


def batch_prefix(batch_id: str) -> str:
    return f"_control/extraction/batch_id={batch_id}"


def data_key(entity: Entity, batch_id: str) -> str:
    return f"raw/{entity.source}/{entity.name}/batch_id={batch_id}/part-00000.parquet"


def manifest_key(entity: Entity, batch_id: str) -> str:
    return f"_control/copy/batch_id={batch_id}/{entity.source}_{entity.name}.manifest"


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value else None


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def log(batch_id: str, msg: str, **fields) -> None:
    extra = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"batch_id={batch_id} {msg} {extra}".rstrip(), flush=True)


def to_parquet(entity: Entity, rows, batch_id: str, extracted_at: datetime) -> bytes:
    """Escribe por bloques para no tener toda la tabla en memoria como objetos de Python."""
    meta = {"_batch_id": batch_id, "_extracted_at": extracted_at, "_source": entity.id}
    buffer = io.BytesIO()
    with pq.ParquetWriter(buffer, entity.schema, compression="snappy") as writer:
        chunk = []
        for row in rows:
            chunk.append({**row, **meta})
            if len(chunk) == CHUNK:
                writer.write_table(pa.Table.from_pylist(chunk, schema=entity.schema))
                chunk = []
        # Siempre se escribe al menos un bloque, aunque este vacio, para que el archivo tenga el esquema.
        writer.write_table(pa.Table.from_pylist(chunk, schema=entity.schema))
    return buffer.getvalue()


@dataclass
class Extractor:
    s3: object
    bucket: str
    readers: dict
    overlap: timedelta = timedelta(minutes=5)
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    # ---------- estado ----------
    def state(self) -> tuple[dict, str | None]:
        state, etag = s3util.read_json_etag(self.s3, self.bucket, STATE_KEY)
        return state or json.loads(json.dumps(EMPTY_STATE)), etag

    @staticmethod
    def committed(state: dict, batch_id: str) -> bool:
        return any(h["batch_id"] == batch_id for h in state["historial"])

    # ---------- flujo ----------
    def _status(self, batch_id: str) -> tuple[dict, dict | None]:
        if not BATCH_ID.match(batch_id):
            raise ValueError(f"batch_id invalido: {batch_id!r}. Solo letras, digitos, punto, guion y guion bajo")
        state, _ = self.state()
        return state, s3util.read_json(self.s3, self.bucket, f"{batch_prefix(batch_id)}/batch.json")

    def run(self, batch_id: str, mode: str = "auto") -> dict:
        """Lote completo: todas las fuentes y la confirmacion. Es lo que corre la terminal."""
        for source in SOURCES:
            if self.extract_source(batch_id, mode, source)["resultado"] == "ya_extraido":
                record = s3util.read_json(self.s3, self.bucket, f"{batch_prefix(batch_id)}/batch.json")
                return {**record, "resultado": "ya_extraido"}
        return self.confirm(batch_id)

    def extract_source(self, batch_id: str, mode: str, source: str) -> dict:
        """Etapa de extraccion de una fuente. En el DAG cada fuente es una tarea y corren en paralelo."""
        state, record = self._status(batch_id)
        if record and self.committed(state, batch_id):
            log(batch_id, "ya extraido, no se consultan las fuentes", fuente=source, seq=record["seq"])
            return {"resultado": "ya_extraido"}
        if record:  # el registro ya existe: todas las entidades estaban completas, solo falta confirmar
            return {"resultado": "extraido"}
        plan = self._plan(batch_id, mode, state)
        if state["seq"] != plan["base_seq"]:
            raise StaleBatch(self._stale_msg(batch_id, plan["base_seq"], state["seq"]))
        pending = [e for e in by_source(source) if not self._entity_done(batch_id, e)]
        if pending:
            with self.readers[source].snapshot() as fetch:
                for entity in pending:
                    self._extract_entity(fetch, entity, batch_id, plan)
        else:
            log(batch_id, "la fuente ya estaba extraida en un intento anterior, no se vuelve a leer", fuente=source)
        if source == SOURCES[0]:
            fault_point("extraccion_a_mitad", f"lote {batch_id} despues de {source}")
        return {"resultado": "extraido"}

    def confirm(self, batch_id: str) -> dict:
        """Etapa de registro en S3: manifiestos COPY, batch.json y por ultimo la marca de agua."""
        state, record = self._status(batch_id)
        if record and self.committed(state, batch_id):
            log(batch_id, "ya extraido y confirmado", seq=record["seq"], filas=record["total_filas"])
            return {**record, "resultado": "ya_extraido"}
        if record is None:
            record = self._record(batch_id)
        else:
            log(batch_id, "registro del lote existe pero sin confirmar, se confirma")
        self._commit(batch_id, record)
        log(batch_id, "lote confirmado", seq=record["seq"], modo=record["modo"], filas=record["total_filas"])
        return {**record, "resultado": "extraido"}

    def _plan(self, batch_id: str, mode: str, state: dict) -> dict:
        key = f"{batch_prefix(batch_id)}/plan.json"
        plan = s3util.read_json(self.s3, self.bucket, key)
        if plan:
            log(batch_id, "reintento: se reutiliza la ventana planificada", modo=plan["modo"],
                base_seq=plan["base_seq"])
            return plan
        if mode == "auto":
            mode = "inicial" if state["seq"] == 0 else "incremental"
        if mode == "incremental" and state["seq"] == 0:
            raise ValueError("No hay extraccion inicial confirmada: corre primero el modo inicial")
        highs = {source: self.readers[source].clock() for source in SOURCES}
        windows = {}
        for entity in ENTITIES:
            previous = _dt(state["marcas"].get(entity.id)) if mode == "incremental" else None
            windows[entity.id] = {
                "marca_anterior": _iso(previous),
                "desde": _iso(previous - self.overlap) if previous else None,
                "hasta": _iso(highs[entity.source]),
            }
        plan = {"batch_id": batch_id, "modo": mode, "base_seq": state["seq"], "planificado_utc": _iso(self.now()),
                "solape_segundos": int(self.overlap.total_seconds()), "ventanas": windows}
        if not s3util.put_json_once(self.s3, self.bucket, key, plan):
            plan = s3util.read_json(self.s3, self.bucket, key)  # otro proceso planifico el mismo lote primero
        log(batch_id, "lote planificado", modo=plan["modo"], base_seq=plan["base_seq"])
        return plan

    def _record(self, batch_id: str) -> dict:
        plan = s3util.read_json(self.s3, self.bucket, f"{batch_prefix(batch_id)}/plan.json")
        if plan is None:
            raise RuntimeError(f"El lote {batch_id} no tiene plan: corre primero la extraccion de sus fuentes")
        summaries = {entity.id: self._entity_done(batch_id, entity) for entity in ENTITIES}
        missing = sorted(eid for eid, summary in summaries.items() if summary is None)
        if missing:
            raise RuntimeError(f"El lote {batch_id} no se puede confirmar, faltan entidades: {', '.join(missing)}")
        for entity in ENTITIES:
            self._write_manifest(entity, batch_id, summaries[entity.id])
        record = {
            "batch_id": batch_id, "modo": plan["modo"], "seq": plan["base_seq"] + 1,
            "planificado_utc": plan["planificado_utc"], "extraido_utc": _iso(self.now()),
            "solape_segundos": plan["solape_segundos"], "entidades": summaries,
            "total_filas": sum(s["filas"] for s in summaries.values()),
        }
        if not s3util.put_json_once(self.s3, self.bucket, f"{batch_prefix(batch_id)}/batch.json", record):
            record = s3util.read_json(self.s3, self.bucket, f"{batch_prefix(batch_id)}/batch.json")
        fault_point("extraccion_antes_de_confirmar", f"lote {batch_id}")
        return record

    def _entity_done(self, batch_id: str, entity: Entity) -> dict | None:
        return s3util.read_json(self.s3, self.bucket, f"{batch_prefix(batch_id)}/entidades/{entity.id}.json")

    def _extract_entity(self, fetch, entity: Entity, batch_id: str, plan: dict) -> None:
        window = plan["ventanas"][entity.id]
        key = data_key(entity, batch_id)
        if s3util.head(self.s3, self.bucket, key) is None:
            body = to_parquet(entity, fetch(entity, _dt(window["desde"]), _dt(window["hasta"])), batch_id, self.now())
            if not s3util.put_once(self.s3, self.bucket, key, body, "application/vnd.apache.parquet"):
                body = s3util.read_bytes(self.s3, self.bucket, key)
        else:
            # El archivo quedo de un intento anterior que fallo antes de registrar la entidad. S3 escribe cada
            # objeto de forma atomica, asi que el archivo esta completo: se reutiliza en vez de reescribirlo.
            body = s3util.read_bytes(self.s3, self.bucket, key)
        table = pq.read_table(io.BytesIO(body))
        previous = _dt(window["marca_anterior"])
        overlap = 0
        if previous is not None and table.num_rows:
            overlap = pc.sum(pc.less_equal(table["_cdc_ts"], pa.scalar(previous, type=table["_cdc_ts"].type))).as_py() or 0
        summary = {"entidad": entity.id, "filas": table.num_rows, "repetidas_por_solape": overlap,
                   "bytes": len(body), "s3": f"s3://{self.bucket}/{key}",
                   "desde": window["desde"], "hasta": window["hasta"]}
        s3util.put_json_once(self.s3, self.bucket, f"{batch_prefix(batch_id)}/entidades/{entity.id}.json", summary)
        log(batch_id, "entidad extraida", entidad=entity.id, filas=summary["filas"],
            solape=overlap, bytes=summary["bytes"])

    def _write_manifest(self, entity: Entity, batch_id: str, summary: dict) -> None:
        manifest = {"entries": [{"url": summary["s3"], "mandatory": True,
                                 "meta": {"content_length": summary["bytes"]}}]}
        s3util.put_json_once(self.s3, self.bucket, manifest_key(entity, batch_id), manifest)

    @staticmethod
    def _stale_msg(batch_id: str, planned: int, current: int) -> str:
        return (f"El lote {batch_id} se planifico sobre la marca seq={planned}, pero otro lote ya la avanzo a "
                f"seq={current}. Sus archivos no se cargan. Extrae un lote nuevo.")

    def _commit(self, batch_id: str, record: dict) -> None:
        for _ in range(3):
            state, etag = self.state()
            if self.committed(state, batch_id):
                return
            if state["seq"] != record["seq"] - 1:
                raise StaleBatch(self._stale_msg(batch_id, record["seq"] - 1, state["seq"]))
            marks = {**state["marcas"], **{eid: s["hasta"] for eid, s in record["entidades"].items()}}
            new_state = {
                "seq": record["seq"], "ultimo_lote": batch_id, "actualizado_utc": _iso(self.now()), "marcas": marks,
                "historial": state["historial"] + [{"seq": record["seq"], "batch_id": batch_id, "modo": record["modo"],
                                                     "filas": record["total_filas"],
                                                     "confirmado_utc": _iso(self.now())}],
            }
            try:
                s3util.put_json_cas(self.s3, self.bucket, STATE_KEY, new_state, etag)
                return
            except s3util.ConcurrentUpdate:
                continue
        raise s3util.ConcurrentUpdate(f"No se pudo confirmar el lote {batch_id}: state.json cambia sin parar")


def print_state(extractor: Extractor) -> None:
    state, _ = extractor.state()
    print(f"seq={state['seq']} ultimo_lote={state['ultimo_lote']}")
    for entity_id, mark in sorted(state["marcas"].items()):
        print(f"  marca {entity_id:30} {mark}")
    print(f"{'seq':>3}  {'modo':11} {'filas':>5}  batch_id")
    for h in state["historial"]:
        print(f"{h['seq']:>3}  {h['modo']:11} {h['filas']:>5}  {h['batch_id']}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--batch-id", help="Identificador del lote. Si se omite se genera manual_<UTC>.")
    parser.add_argument("--mode", choices=("auto", "inicial", "incremental"), default="auto")
    parser.add_argument("--estado", action="store_true", help="Muestra las marcas de agua y el historial")
    stage = parser.add_mutually_exclusive_group()
    stage.add_argument("--fuente", choices=SOURCES, help="Solo extrae esta fuente, sin confirmar el lote (DAG)")
    stage.add_argument("--confirmar", action="store_true", help="Solo registra y confirma el lote ya extraido (DAG)")
    args = parser.parse_args(argv)

    import boto3

    from .readers import default_readers

    bucket = os.environ.get("DW_BUCKET")
    if not bucket:
        print("Falta la variable DW_BUCKET", file=sys.stderr)
        return 1
    overlap = timedelta(seconds=int(os.environ.get("DW_CDC_SOLAPE_SEGUNDOS", "300")))
    extractor = Extractor(boto3.client("s3"), bucket, default_readers(), overlap=overlap)
    if args.estado:
        print_state(extractor)
        return 0
    batch_id = args.batch_id or f"manual_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    if args.fuente:
        result = extractor.extract_source(batch_id, args.mode, args.fuente)
        return EXIT_YA_EXTRAIDO if result["resultado"] == "ya_extraido" else 0
    result = extractor.confirm(batch_id) if args.confirmar else extractor.run(batch_id, args.mode)
    for entity_id, s in result["entidades"].items():
        print(f"  {entity_id:30} filas={s['filas']:>4} solape={s['repetidas_por_solape']:>3}  {s['s3']}")
    return EXIT_YA_EXTRAIDO if result["resultado"] == "ya_extraido" else 0


if __name__ == "__main__":
    sys.exit(main())
