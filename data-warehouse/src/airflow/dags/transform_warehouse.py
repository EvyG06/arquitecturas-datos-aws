"""Ilustra dependencias, reintentos y fallos en una cadena de tareas dbt.

Seeds y modelos intermedios (validaciones de calidad) preparan los snapshots.
La cuarentena se materializa después de la capa intermedia; luego se construyen
los marts y se ejecutan las pruebas. Solo una tarea accede a DuckDB cada vez.
El DAG se dispara manualmente y desactiva catchup.
"""

from datetime import datetime, timedelta, timezone

from airflow.operators.python import PythonOperator

from airflow import DAG
from ecommerce_dw.transformations.dbt_runner import run_dbt

with DAG(
    # Identificador y resumen mostrados en la interfaz de Airflow.
    dag_id="transform_warehouse",
    description="Build snapshots and marts with dbt, then validate warehouse data",
    # Fecha de referencia para habilitar corridas.
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    # Sin calendario propio: se dispara manualmente o desde el maestro.
    schedule=None,
    # No intenta recuperar intervalos anteriores.
    catchup=False,
    # Un solo DAG activo y una sola tarea activa protegen el archivo DuckDB.
    max_active_runs=1,
    max_active_tasks=1,
    # Política heredada: dos reintentos con espera inicial de treinta segundos.
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        # Duplica la espera tras cada fallo, con límite de cinco minutos.
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
    },
) as dag:
    # task_id nombra la tarea; python_callable ejecuta dbt; op_args pasa el comando.
    packages = PythonOperator(task_id="dbt_deps", python_callable=run_dbt, op_args=["deps"])
    # deps instala paquetes y seed carga los catálogos CSV.
    seed = PythonOperator(task_id="dbt_seed", python_callable=run_dbt, op_args=["seed"])
    intermediate = PythonOperator(
        task_id="dbt_run_intermediate",
        python_callable=run_dbt,
        # El selector limita run a los modelos intermedios.
        op_args=["run", "path:models/intermediate"],
    )
    # snapshot registra versiones antes de construir las dimensiones finales.
    # quarantine guarda las filas rechazadas por calidad, con su motivo.
    quarantine = PythonOperator(
        task_id="dbt_run_quarantine",
        python_callable=run_dbt,
        op_args=["run", "path:models/quarantine"],
    )
    snapshots = PythonOperator(task_id="dbt_snapshot", python_callable=run_dbt, op_args=["snapshot"])
    marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=run_dbt,
        # El selector limita run a dimensiones y hechos.
        op_args=["run", "path:models/marts"],
    )
    # test valida el modelo al terminar las transformaciones.
    tests = PythonOperator(task_id="dbt_test", python_callable=run_dbt, op_args=["test"])

    # La cadena evita operaciones concurrentes sobre DuckDB y corta ante fallos.
    packages >> seed >> intermediate >> quarantine >> snapshots >> marts >> tests
