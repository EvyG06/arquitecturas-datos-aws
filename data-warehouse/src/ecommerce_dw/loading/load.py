#!/usr/bin/env python3
"""Carga a Redshift los lotes confirmados en _control/extraction/state.json que aun no estan en raw.

Cada lote se carga en UNA transaccion de la Data API. Por entidad:
  1. COPY desde el manifiesto del lote a una tabla temporal con las mismas columnas del Parquet.
  2. Verificacion: si la tabla temporal no tiene exactamente las filas de batch.json, se divide por cero
     y la transaccion completa se revierte.
  3. DELETE de las filas de ese lote en raw y INSERT desde la temporal, con _loaded_at.
Al final registra el lote en raw._cargas. Recargar un lote reemplaza sus filas: nunca las duplica.

Codigos de salida: 0 cargado o sin pendientes, 99 el lote pedido ya estaba cargado, 1 error.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

from ecommerce_dw.extraction import s3util
from ecommerce_dw.extraction.entities import ENTITIES
from ecommerce_dw.extraction.extract import BATCH_ID, STATE_KEY, batch_prefix, fault_point, log, manifest_key

from . import ddl
from .redshift import DataApi

EXIT_YA_CARGADO = 99


@dataclass
class Loader:
    api: DataApi
    s3: object
    bucket: str
    copy_role_arn: str

    def committed(self) -> list[dict]:
        state = s3util.read_json(self.s3, self.bucket, STATE_KEY) or {"historial": []}
        return sorted(state["historial"], key=lambda h: h["seq"])

    def loaded(self) -> set[str]:
        return {row[0] for row in self.api.query(f"select distinct batch_id from {ddl.CONTROL_TABLE}", "dw-cargados")}

    def statements(self, record: dict) -> list[str]:
        batch_id = record["batch_id"]
        if not BATCH_ID.match(batch_id):  # el id va dentro del SQL: solo se acepta el alfabeto seguro
            raise ValueError(f"batch_id invalido: {batch_id!r}")
        sqls = []
        for n, entity in enumerate(ENTITIES):
            rows = int(record["entidades"][entity.id]["filas"])
            target, temp = ddl.table(entity), ddl.temp_table(entity)
            if rows:
                cols = ", ".join(name for name, _ in ddl.columns(entity))
                sqls += [
                    ddl.create_temp_table(entity),
                    f"copy {temp} from 's3://{self.bucket}/{manifest_key(entity, batch_id)}' "
                    f"iam_role '{self.copy_role_arn}' format as parquet manifest",
                    f"select 1 / (case when (select count(*) from {temp}) = {rows} then 1 else 0 end) as conteo_ok",
                    f"delete from {target} where _batch_id = '{batch_id}'",
                    f"insert into {target} ({cols}, _loaded_at) select {cols}, getdate() from {temp}",
                ]
            else:
                sqls.append(f"delete from {target} where _batch_id = '{batch_id}'")
            if n == 2 and os.environ.get("DW_FAULT") == "carga_a_mitad":
                sqls.append("select 1 / 0 as fallo_controlado_carga_a_mitad")
        values = ", ".join(
            f"('{batch_id}', {int(record['seq'])}, '{record['modo']}', '{entity.id}', "
            f"{int(record['entidades'][entity.id]['filas'])}, getdate())" for entity in ENTITIES)
        sqls += [f"delete from {ddl.CONTROL_TABLE} where batch_id = '{batch_id}'",
                 f"insert into {ddl.CONTROL_TABLE} values {values}"]
        return sqls

    def load_batch(self, entry: dict) -> None:
        record = s3util.read_json(self.s3, self.bucket, f"{batch_prefix(entry['batch_id'])}/batch.json")
        if record is None:
            raise RuntimeError(f"El lote {entry['batch_id']} esta en state.json pero no tiene batch.json")
        fault_point("carga_antes_de_copy", f"lote {record['batch_id']}")
        self.api.batch(self.statements(record), f"dw-carga-{record['batch_id']}")
        log(record["batch_id"], "lote cargado en raw", seq=record["seq"], filas=record["total_filas"])

    def run(self, batch_id: str | None = None, reload: bool = False) -> dict:
        self.api.batch(ddl.bootstrap(), "dw-esquema-raw")
        history = self.committed()
        loaded = self.loaded()
        if batch_id:
            entry = next((h for h in history if h["batch_id"] == batch_id), None)
            if entry is None:
                raise ValueError(f"El lote {batch_id} no esta confirmado en state.json: no se carga")
            if batch_id in loaded and not reload:
                log(batch_id, "ya cargado en raw, no se repite (usa --recargar para reemplazarlo)")
                return {"resultado": "ya_cargado", "lotes": []}
            targets = [entry]
        else:
            targets = [h for h in history if h["batch_id"] not in loaded]
        for entry in targets:
            self.load_batch(entry)
        if not targets:
            print("Sin lotes pendientes: todo lo confirmado en state.json ya esta en raw", flush=True)
        return {"resultado": "cargado" if targets else "sin_pendientes", "lotes": [t["batch_id"] for t in targets]}

    def report(self) -> None:
        print(f"{'seq':>3}  {'modo':11} {'filas':>5}  cargado_utc          batch_id")
        for seq, modo, filas, cargado, batch_id in self.api.query(
                f"select seq, modo, sum(filas), max(cargado_en)::varchar(19), batch_id from {ddl.CONTROL_TABLE} "
                "group by seq, modo, batch_id order by seq", "dw-reporte-cargas"):
            print(f"{seq:>3}  {modo:11} {filas:>5}  {cargado}  {batch_id}")
        union = " union all ".join(
            f"select '{ddl.table(e)}' as tabla, count(*) as filas, count(distinct _batch_id) as lotes from {ddl.table(e)}"
            for e in ENTITIES)
        print(f"\n{'tabla':32} {'filas':>6} {'lotes':>5}")
        for tabla, filas, lotes in self.api.query(union, "dw-reporte-raw"):
            print(f"{tabla:32} {filas:>6} {lotes:>5}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--batch-id", help="Carga solo este lote (debe estar confirmado en state.json)")
    parser.add_argument("--recargar", action="store_true", help="Reemplaza un lote que ya estaba cargado")
    parser.add_argument("--estado", action="store_true", help="Muestra los lotes cargados y las filas de raw")
    args = parser.parse_args(argv)

    import boto3

    from ecommerce_dw import aws_conn

    p = aws_conn.params()
    api = DataApi(boto3.client("redshift-data"), p["redshift_workgroup"], p["redshift_database"])
    loader = Loader(api, boto3.client("s3"), os.environ["DW_BUCKET"], p["redshift_copy_role_arn"])
    if args.estado:
        loader.report()
        return 0
    result = loader.run(args.batch_id, args.recargar)
    loader.report()
    return EXIT_YA_CARGADO if result["resultado"] == "ya_cargado" else 0


if __name__ == "__main__":
    sys.exit(main())
