"""Ilustra TriggerDagRunOperator, espera entre DAGs, programación y catchup."""

import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow import DAG

with DAG(
    # Identificador visible en Airflow y resumen mostrado en la interfaz.
    dag_id="ecommerce_pipeline",
    description="Run source extraction followed by warehouse transformations",
    # Fecha de referencia para calcular la programación en hora de Bogotá.
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Bogota"),
    # Expresión cron: una ejecución diaria a las 06:00.
    schedule="0 6 * * *",
    # No crea corridas atrasadas desde start_date.
    catchup=False,
    # Impide que dos ejecuciones del maestro se solapen.
    max_active_runs=1,
) as dag:
    # La primera tarea dispara el DAG de ingesta y espera su resultado.
    extract = TriggerDagRunOperator(
        # task_id nombra esta tarea; trigger_dag_id identifica el DAG hijo.
        task_id="extract_to_raw",
        trigger_dag_id="extract_to_raw",
        # Espera al DAG hijo y consulta su estado cada cinco segundos.
        wait_for_completion=True,
        poke_interval=5,
        # Solo success permite continuar; failed hace fallar esta tarea.
        allowed_states=["success"],
        failed_states=["failed"],
    )
    # El segundo disparo usa los mismos criterios para transformar los datos.
    transform = TriggerDagRunOperator(
        task_id="transform_warehouse",
        trigger_dag_id="transform_warehouse",
        wait_for_completion=True,
        poke_interval=5,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # La transformación comienza solo cuando la ingesta termina correctamente.
    extract >> transform
