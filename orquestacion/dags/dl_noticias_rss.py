"""
### Data lake de noticias RSS (El Tiempo)

Coordina las nueve etapas de la seccion 4.6 del enunciado. Cada tarea ejecuta el mismo modulo de
Python que se usa desde la terminal (`python -m dl.<modulo>`), dentro de un venv propio de la imagen,
con el rol `arq-datos-pipeline`.

| Parametro | Uso |
|---|---|
| `reprocesar_run_id` | Reprocesa un lote existente desde Bronze, sin consultar los RSS |
| `fallo_controlado` | Inyecta un fallo solo en el primer intento, para demostrar la recuperacion |

Control de concurrencia: `max_active_runs=1`, el pool `dl_escritura` con un cupo para todas las
tareas que escriben, y compare-and-swap con ETag sobre los manifiestos de Silver y Gold en S3.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

PYTHON_DL = "/home/airflow/dl-venv/bin/python"
POOL = "dl_escritura"
FALLOS = ["", "silver_antes_de_publicar", "gold_antes_de_publicar"]

# Los valores del usuario viajan como variables de entorno y nunca se interpolan dentro del comando,
# asi un parametro malicioso no puede inyectar codigo en bash. El patron del parametro es una segunda capa.
ENV = {
    "PYTHONPATH": "/opt/dl/src",
    "PYTHONDONTWRITEBYTECODE": "1",
    "LOTE": "{{ params.reprocesar_run_id or run_id }}",
    "REPROCESAR": "{{ params.reprocesar_run_id }}",
    "INTENTO": "{{ ti.try_number }}",
    "MAX_INTENTOS": "{{ task.retries + 1 }}",
    "DL_FAULT": "{{ params.fallo_controlado if ti.try_number == 1 else '' }}",
}


def tarea(task_id: str, comando: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=comando, env=ENV, append_env=True, **kwargs)


def _seguro(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def registrar_estado(**context) -> None:
    """Etapa 9. Deja en S3 el resultado de la ejecucion y falla el DAG si alguna etapa fallo."""
    import boto3

    dag_run = context["dag_run"]
    params = context["params"]
    lote = _seguro(params.get("reprocesar_run_id") or dag_run.run_id)
    estados = {ti.task_id: getattr(ti.state, "value", ti.state)
               for ti in dag_run.get_task_instances() if ti.task_id != context["ti"].task_id}
    fallidas = sorted(t for t, s in estados.items() if s in ("failed", "upstream_failed"))

    s3 = boto3.client("s3")
    bucket = os.environ["DL_BUCKET"]
    try:
        body = s3.get_object(Bucket=bucket, Key=f"_control/ingestion/run_id={lote}/_run.json")["Body"].read()
        ingesta = json.loads(body)
    except s3.exceptions.NoSuchKey:
        ingesta = None

    if fallidas:
        resultado = "fallida"
    elif ingesta and ingesta.get("status") == "partial":
        resultado = "parcial"
    else:
        resultado = "completa"

    registro = {
        "dag_run_id": dag_run.run_id,
        "lote": lote,
        "modo": "reproceso" if params.get("reprocesar_run_id") else "normal",
        "resultado": resultado,
        "tareas_fallidas": fallidas,
        "estado_tareas": estados,
        "fuentes": (ingesta or {}).get("sources"),
        "inicio_utc": dag_run.start_date,
        "registrado_utc": pendulum.now("UTC"),
    }
    key = f"_control/pipeline/run_id={lote}/{_seguro(dag_run.run_id)}.json"
    s3.put_object(Bucket=bucket, Key=key, ContentType="application/json",
                  Body=json.dumps(registro, ensure_ascii=False, indent=2, default=str).encode("utf-8"))
    log.info("task=registrar_estado run_id=%s resultado=%s fallidas=%s registro=s3://%s/%s",
             lote, resultado, fallidas, bucket, key)
    if fallidas:
        raise AirflowException(f"Ejecucion fallida en {fallidas}")


with DAG(
    dag_id="dl_noticias_rss",
    description="RSS de El Tiempo a Bronze, Silver con Comprehend, Gold, Glue y validaciones en Athena",
    doc_md=__doc__,
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "eve",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    params={
        "reprocesar_run_id": Param("", type="string", pattern=r"^[A-Za-z0-9_.:+-]*$",
                                   description="run_id de un lote de Bronze. Vacio para una ingesta normal."),
        "fallo_controlado": Param("", type="string", enum=FALLOS,
                                  description="Punto donde fallar en el primer intento. Vacio para no fallar."),
    },
    tags=["data-lake", "rss", "eia"],
) as dag:
    # Sin reintentos: un error de configuracion o un lote inexistente no se arregla esperando, y los
    # errores transitorios de las APIs de AWS ya los reintenta boto3 por dentro.
    validar = tarea("validar_configuracion",
                    f'exec {PYTHON_DL} -m dl.preflight ${{REPROCESAR:+--reprocesar "$REPROCESAR"}}', retries=0)

    extraer = tarea(
        "extraer_feeds_a_bronze",
        'if [ -n "$REPROCESAR" ]; then\n'
        '  echo "Reproceso del lote $REPROCESAR desde Bronze, no se consultan los RSS"\n'
        "  exit 99\n"
        "fi\n"
        f'exec {PYTHON_DL} -m dl.ingest --run-id "$LOTE" --attempt "$INTENTO" --max-attempts "$MAX_INTENTOS"',
        skip_on_exit_code=99,
    )

    limpiar = tarea(
        "limpiar_validar_deduplicar",
        f'exec {PYTHON_DL} -m dl.silver --run-id "$LOTE" --stage preparar ${{REPROCESAR:+--force}}',
        trigger_rule="none_failed", pool=POOL,
    )
    enriquecer = tarea("enriquecer_con_comprehend",
                       f'exec {PYTHON_DL} -m dl.silver --run-id "$LOTE" --stage enriquecer', pool=POOL)
    publicar_silver = tarea("publicar_silver",
                            f'exec {PYTHON_DL} -m dl.silver --run-id "$LOTE" --stage publicar', pool=POOL)
    construir_gold = tarea("construir_gold", f"exec {PYTHON_DL} -m dl.gold", pool=POOL)
    actualizar_catalogo = tarea("actualizar_glue_catalog", f"exec {PYTHON_DL} -m dl.catalog --sync", pool=POOL)
    validar_athena = tarea("validaciones_athena", f'exec {PYTHON_DL} -m dl.validate --run-id "$LOTE"')

    registrar = PythonOperator(task_id="registrar_estado", python_callable=registrar_estado,
                               trigger_rule="all_done", retries=0)

    (validar >> extraer >> limpiar >> enriquecer >> publicar_silver >> construir_gold
     >> actualizar_catalogo >> validar_athena >> registrar)
