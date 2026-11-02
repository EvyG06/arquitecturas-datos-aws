"""Tablas y particiones de Silver y Gold en AWS Glue Data Catalog."""
from __future__ import annotations

import argparse
import json
import logging
import sys

import boto3
import pyarrow as pa

from dl.config import bucket
from dl.gold import GOLD_STATE_KEY
from dl.gold import SCHEMAS as GOLD_SCHEMAS
from dl.s3util import read_json
from dl.silver import SCHEMAS as SILVER_SCHEMAS
from dl.silver import STATE_KEY as SILVER_STATE_KEY

log = logging.getLogger("dl.catalog")

DATABASE = "arq_dl"
LAYERS = {"silver": (SILVER_STATE_KEY, SILVER_SCHEMAS), "gold": (GOLD_STATE_KEY, GOLD_SCHEMAS)}
PARTITION_KEYS = [{"Name": "pub_date", "Type": "date", "Comment": "Dia de publicacion en America/Bogota"}]
PARQUET = {
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
}


def glue_type(t: pa.DataType) -> str:
    if pa.types.is_string(t):
        return "string"
    if pa.types.is_int32(t):
        return "int"
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_float64(t):
        return "double"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_list(t):
        return f"array<{glue_type(t.value_type)}>"
    raise TypeError(f"Tipo sin equivalente en Glue: {t}")


def tables():
    for layer, (_, schemas) in LAYERS.items():
        for ds, schema in schemas.items():
            yield layer, ds, f"{layer}_{ds}", schema


def plan(current: dict[str, str], desired: dict[str, str]) -> dict[str, list[str]]:
    """Diferencia entre particiones registradas y las que dicta el manifiesto (valor -> ubicacion)."""
    return {
        "crear": sorted(set(desired) - set(current)),
        "actualizar": sorted(d for d in set(desired) & set(current) if desired[d] != current[d]),
        "eliminar": sorted(set(current) - set(desired)),
    }


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def setup(glue, bkt: str) -> None:
    """Crea o actualiza la base y las tablas. Lo corre un administrador, no el pipeline."""
    try:
        glue.create_database(DatabaseInput={"Name": DATABASE, "Description": "Data lake de noticias RSS (EIA)"})
        log.info("base creada %s", DATABASE)
    except glue.exceptions.AlreadyExistsException:
        log.info("base ya existe %s", DATABASE)
    for layer, ds, name, schema in tables():
        table_input = {
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            "PartitionKeys": PARTITION_KEYS,
            "StorageDescriptor": {**PARQUET, "Location": f"s3://{bkt}/{layer}/{ds}/",
                                  "Columns": [{"Name": f.name, "Type": glue_type(f.type)} for f in schema]},
        }
        try:
            glue.create_table(DatabaseName=DATABASE, TableInput=table_input)
            log.info("tabla creada %s", name)
        except glue.exceptions.AlreadyExistsException:
            glue.update_table(DatabaseName=DATABASE, TableInput=table_input)
            log.info("tabla actualizada %s", name)


def sync(glue, s3, bkt: str) -> dict:
    """Alinea las particiones de Glue con los manifiestos de Silver y Gold. Lo corre el pipeline."""
    report = {}
    for layer, ds, name, _ in tables():
        state = read_json(s3, bkt, LAYERS[layer][0]) or {"datasets": {}}
        desired = {day: f"s3://{bkt}/{key.rsplit('/', 1)[0]}/"
                   for day, key in state["datasets"].get(ds, {}).items()}
        current = {}
        for page in glue.get_paginator("get_partitions").paginate(DatabaseName=DATABASE, TableName=name):
            for p in page["Partitions"]:
                current[p["Values"][0]] = p["StorageDescriptor"]["Location"]
        changes = plan(current, desired)
        sd = glue.get_table(DatabaseName=DATABASE, Name=name)["Table"]["StorageDescriptor"]

        def partition_input(day: str) -> dict:
            return {"Values": [day], "StorageDescriptor": {**sd, "Location": desired[day]}}

        for chunk in _chunks(changes["crear"], 100):
            resp = glue.batch_create_partition(DatabaseName=DATABASE, TableName=name,
                                               PartitionInputList=[partition_input(d) for d in chunk])
            if resp.get("Errors"):
                raise RuntimeError(f"{name}: error creando particiones {resp['Errors']}")
        for day in changes["actualizar"]:
            glue.update_partition(DatabaseName=DATABASE, TableName=name, PartitionValueList=[day],
                                  PartitionInput=partition_input(day))
        for chunk in _chunks(changes["eliminar"], 25):
            resp = glue.batch_delete_partition(DatabaseName=DATABASE, TableName=name,
                                               PartitionsToDelete=[{"Values": [d]} for d in chunk])
            if resp.get("Errors"):
                raise RuntimeError(f"{name}: error eliminando particiones {resp['Errors']}")
        report[name] = {**{k: len(v) for k, v in changes.items()}, "vigentes": len(desired)}
        log.info("task=catalog tabla=%s crear=%s actualizar=%s eliminar=%s", name,
                 len(changes["crear"]), len(changes["actualizar"]), len(changes["eliminar"]))
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Glue Data Catalog del data lake")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--setup", action="store_true", help="crea base y tablas (perfil admin)")
    group.add_argument("--sync", action="store_true", help="sincroniza particiones (perfil pipeline)")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("numexpr").setLevel(logging.WARNING)
    glue = boto3.client("glue")
    if args.setup:
        setup(glue, bucket())
    else:
        print(json.dumps(sync(glue, boto3.client("s3"), bucket()), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
