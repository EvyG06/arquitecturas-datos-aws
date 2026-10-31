"""Ilustra TaskGroups por fuente, extracciones paralelas y XCom entre tareas.

Las cargas a DuckDB son secuenciales; los reintentos cubren fallos transitorios.
El DAG se dispara manualmente y desactiva catchup.
"""

from datetime import datetime, timedelta, timezone

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from ecommerce_dw.ingestion.airflow_tasks import (
    extract_mongo_task,
    extract_mysql_task,
    load_mongo_task,
    load_mysql_task,
    verify_raw_counts,
)

with DAG(
    # Identificador y resumen mostrados en la interfaz de Airflow.
    dag_id="extract_to_raw",
    description="Extract MySQL and MongoDB concurrently and load raw tables sequentially",
    # Fecha de referencia para habilitar corridas.
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    # Sin calendario propio: se dispara manualmente o desde el maestro.
    schedule=None,
    # No intenta recuperar intervalos anteriores.
    catchup=False,
    # Evita ejecuciones simultáneas del mismo DAG.
    max_active_runs=1,
    # Cada tarea reintenta dos veces tras esperar treinta segundos.
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
) as dag:
    # group_id agrupa tareas por fuente; tooltip describe el grupo en la UI.
    with TaskGroup(group_id="mysql", tooltip="Extract and load relational source tables"):
        # task_id identifica la tarea; python_callable apunta a lógica reutilizable.
        mysql_extract = PythonOperator(task_id="extract", python_callable=extract_mysql_task)
        mysql_load = PythonOperator(
            task_id="load_raw",
            python_callable=load_mysql_task,
            # op_args recibe desde XCom la salida de la extracción.
            op_args=[mysql_extract.output],
        )

    with TaskGroup(group_id="mongodb", tooltip="Extract and load nested documents"):
        mongo_extract = PythonOperator(task_id="extract", python_callable=extract_mongo_task)
        mongo_load = PythonOperator(
            task_id="load_raw",
            python_callable=load_mongo_task,
            op_args=[mongo_extract.output],
        )

    verify = PythonOperator(
        task_id="verify_raw_counts",
        python_callable=verify_raw_counts,
        # La verificación recibe ambas salidas publicadas por XCom.
        op_args=[mysql_extract.output, mongo_extract.output],
    )

    # Las extracciones pueden coincidir; las escrituras en DuckDB van en serie.
    [mysql_extract, mongo_extract] >> mysql_load >> mongo_load >> verify
