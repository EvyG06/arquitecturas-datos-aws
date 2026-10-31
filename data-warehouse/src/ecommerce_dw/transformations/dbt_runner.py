"""Ejecuta comandos de dbt desde tareas reutilizables de Airflow."""

import fcntl
import logging
import os
import subprocess
from pathlib import Path

DBT_EXECUTABLE = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = Path("/opt/airflow/src/dbt")
WAREHOUSE_PATH = Path(os.getenv("DUCKDB_PATH", "/opt/warehouse/warehouse.duckdb"))
LOGGER = logging.getLogger(__name__)


def run_dbt(command, selector=None):
    """Ejecuta dbt y serializa el acceso al archivo compartido de DuckDB."""
    arguments = [
        DBT_EXECUTABLE,
        command,
        "--project-dir", str(DBT_PROJECT),
        "--profiles-dir", str(DBT_PROJECT),
    ]
    if selector is not None:
        arguments.extend(["--select", selector])
    if command != "deps":
        arguments.extend(["--threads", "1"])

    LOGGER.info("Running dbt command: %s", " ".join(arguments))
    if command == "deps":
        subprocess.run(arguments, check=True)
        return

    with WAREHOUSE_PATH.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            subprocess.run(arguments, check=True)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
