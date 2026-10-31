"""Funciones de extracción, carga y verificación usadas por Airflow."""

import logging
import os
import re
from pathlib import Path

from .raw_ingestion import (
    count_raw,
    extract_mongo,
    extract_mysql,
    load_mongo,
    load_mysql,
)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/warehouse/warehouse.duckdb")
LOGGER = logging.getLogger(__name__)


def artifact_directory(context):
    """Construye una carpeta de extracción distinta para cada ejecución."""
    run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])
    return str(Path(DUCKDB_PATH).parent / "extracts" / run_id)


def extract_mysql_task(**context):
    """Extrae las tablas MySQL y devuelve sus rutas y conteos mediante XCom."""
    return extract_mysql(artifact_directory(context))


def extract_mongo_task(**context):
    """Extrae las colecciones MongoDB y devuelve sus rutas y conteos mediante XCom."""
    return extract_mongo(artifact_directory(context))


def load_mysql_task(extracts):
    """Carga en raw los archivos MySQL de la ejecución."""
    load_mysql(extracts, DUCKDB_PATH)


def load_mongo_task(extracts):
    """Carga en raw los archivos MongoDB de la ejecución."""
    load_mongo(extracts, DUCKDB_PATH)


def verify_raw_counts(mysql_extracts, mongo_extracts):
    """Comprueba que cada tabla raw conserva el conteo extraído."""
    expected = {f"mysql_{table}": result["rows"] for table, result in mysql_extracts.items()}
    expected.update({f"mongo_{table}": result["rows"] for table, result in mongo_extracts.items()})
    actual = count_raw(DUCKDB_PATH)
    mismatches = {table: (expected[table], actual[table]) for table in expected if expected[table] != actual[table]}
    if mismatches:
        raise ValueError(f"Source/raw row counts differ: {mismatches}")
    LOGGER.info("Source and raw row counts match: %s", actual)
    return actual
