"""
### Data warehouse de e-commerce (RDS MySQL y DocumentDB a Redshift)

Coordina las siete etapas de la seccion 3.4 del enunciado. Cada tarea ejecuta el mismo modulo que se usa
desde la terminal, dentro de los venvs de la imagen (dw-venv y dbt-venv), con el rol `arq-datos-pipeline`.

| Etapa | Tareas |
|---|---|
| 1. Validacion de conexiones y parametros | `validar_configuracion` (sin reintentos) |
| 2. Extraccion de las fuentes | `extraer_mysql` y `extraer_documentdb`, en paralelo |
| 3. Escritura en S3 | los Parquet los escribe la extraccion; `registrar_lote_s3` escribe manifiestos, `batch.json` y la marca de agua |
| 4. Carga a Redshift | `cargar_redshift` (COPY por lote en una transaccion) |
| 5. Transformaciones y snapshots | `dbt_seed`, `dbt_run_intermediate`, `dbt_run_quarantine`, `dbt_snapshot`, `dbt_run_marts` |
| 6. Pruebas de calidad | `dbt_test` y `validar_conciliacion` (MySQL contra marts) |
| 7. Registro del resultado | `registrar_resultado`, en `_control/pipeline/` del bucket del DW |

**Lote.** Cada ejecucion es un lote cuyo id es el `run_id` convertido a caracteres seguros. El lote cuenta como
extraido solo cuando `registrar_lote_s3` lo agrega al historial de `_control/extraction/state.json`: en ese
momento avanza la marca de agua. Cuenta como cargado cuando su fila entra a `raw._cargas`, en la misma
transaccion que sus datos.

| Parametro | Uso |
|---|---|
| `reprocesar_batch_id` | Recarga un lote ya extraido desde S3 y reconstruye el DW, sin consultar las fuentes |
| `fallo_controlado` | Inyecta un fallo solo en el primer intento de la etapa elegida, para demostrar la recuperacion |
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

PY = "/home/airflow/dw-venv/bin/python"
DBT = "bash /opt/dw/scripts/aws/dbt_aws.sh"
FALLOS = ["", "extraccion_a_mitad", "extraccion_antes_de_confirmar", "carga_a_mitad"]


def lote_seguro(raw: str) -> str:
    """Mismo criterio que ecommerce_dw.extraction.extract.safe_batch_id."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", raw)[:128]


# Los valores del usuario viajan como variables de entorno y nunca se interpolan dentro del comando, asi un
# parametro malicioso no puede inyectar codigo en bash. El patron y el enum de los Param son la segunda capa.
ENV = {
    "PYTHONPATH": "/opt/dw/src",
    "PYTHONDONTWRITEBYTECODE": "1",
    "LOTE": "{{ params.reprocesar_batch_id or (run_id | lote_seguro) }}",
    "REPROCESAR": "{{ params.reprocesar_batch_id }}",
    "INTENTO": "{{ ti.try_number }}",
    "DW_FAULT": "{{ params.fallo_controlado if ti.try_number == 1 else '' }}",
}
# Primera linea de cada log: identifica el lote, la tarea y el intento.
CABECERA = 'echo "batch_id=$LOTE tarea={{ task.task_id }} intento=$INTENTO${REPROCESAR:+ modo=reproceso}"\n'
OMITIR_EN_REPROCESO = ('if [ -n "$REPROCESAR" ]; then\n'
                       '  echo "Reproceso del lote $REPROCESAR desde S3: no se consultan las fuentes"\n'
                       '  exit 99\n'
                       'fi\n')


def tarea(task_id: str, comando: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=CABECERA + comando, env=ENV, append_env=True, **kwargs)


def dbt(task_id: str, args: str, **kwargs) -> BashOperator:
    # El resumen de run_results.json deja al final del log los conteos y el mensaje completo de cada error.
    return tarea(task_id, f"{DBT} {args}\nrc=$?\n{PY} -m ecommerce_dw.transformations.resumen_dbt || true\nexit $rc",
                 **kwargs)


def registrar_resultado(**context) -> None:
    """Etapa 7. Deja en S3 el resultado de la ejecucion y falla el DAG si alguna etapa fallo."""
    import boto3

    dag_run, params = context["dag_run"], context["params"]
    lote = params.get("reprocesar_batch_id") or lote_seguro(dag_run.run_id)
    estados = {ti.task_id: getattr(ti.state, "value", ti.state)
               for ti in dag_run.get_task_instances() if ti.task_id != context["ti"].task_id}
    fallidas = sorted(t for t, s in estados.items() if s in ("failed", "upstream_failed"))
    s3, bucket = boto3.client("s3"), os.environ["DW_BUCKET"]
    try:
        body = s3.get_object(Bucket=bucket, Key=f"_control/extraction/batch_id={lote}/batch.json")["Body"].read()
        lote_registrado = json.loads(body)
    except s3.exceptions.NoSuchKey:
        lote_registrado = None
    registro = {
        "dag_run_id": dag_run.run_id,
        "lote": lote,
        "modo": "reproceso" if params.get("reprocesar_batch_id") else "normal",
        "fallo_controlado": params.get("fallo_controlado") or None,
        "resultado": "fallida" if fallidas else "completa",
        "tareas_fallidas": fallidas,
        "estado_tareas": estados,
        "seq_lote": (lote_registrado or {}).get("seq"),
        "filas_extraidas": {e: s["filas"] for e, s in ((lote_registrado or {}).get("entidades") or {}).items()},
        "inicio_utc": dag_run.start_date,
        "registrado_utc": pendulum.now("UTC"),
    }
    key = f"_control/pipeline/batch_id={lote}/{lote_seguro(dag_run.run_id)}.json"
    s3.put_object(Bucket=bucket, Key=key, ContentType="application/json",
                  Body=json.dumps(registro, ensure_ascii=False, indent=2, default=str).encode("utf-8"))
    log.info("batch_id=%s tarea=registrar_resultado resultado=%s fallidas=%s registro=s3://%s/%s",
             lote, registro["resultado"], fallidas, bucket, key)
    if fallidas:
        raise AirflowException(f"Lote {lote}: fallaron {fallidas}")


with DAG(
    dag_id="dw_ecommerce",
    description="RDS MySQL y DocumentDB a S3, COPY a Redshift, dbt, pruebas y registro del lote",
    doc_md=__doc__,
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Bogota"),
    catchup=False,
    max_active_runs=1,
    user_defined_filters={"lote_seguro": lote_seguro},
    default_args={
        "owner": "eve",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "reprocesar_batch_id": Param("", type="string", pattern=r"^[A-Za-z0-9_.-]*$",
                                     description="Lote ya extraido a reprocesar desde S3. Vacio para un lote nuevo."),
        "fallo_controlado": Param("", type="string", enum=FALLOS,
                                  description="Etapa donde fallar en el primer intento. Vacio para no fallar."),
    },
    tags=["data-warehouse", "redshift", "dbt", "eia"],
) as dag:
    validar = tarea("validar_configuracion",
                    f'exec {PY} -m ecommerce_dw.pipeline.preflight ${{REPROCESAR:+--reprocesar "$REPROCESAR"}}',
                    retries=0)

    extraer = [
        tarea(f"extraer_{fuente}",
              OMITIR_EN_REPROCESO + f'exec {PY} -m ecommerce_dw.extraction.extract --batch-id "$LOTE" --fuente {fuente}',
              skip_on_exit_code=99)
        for fuente in ("mysql", "documentdb")
    ]
    registrar_lote = tarea("registrar_lote_s3",
                           OMITIR_EN_REPROCESO + f'exec {PY} -m ecommerce_dw.extraction.extract --batch-id "$LOTE" --confirmar',
                           skip_on_exit_code=99, trigger_rule="none_failed")
    # Normal: carga todos los lotes confirmados que falten, incluido uno que haya quedado de una ejecucion fallida.
    # Reproceso: reemplaza las filas del lote pedido, sin duplicarlas.
    cargar = tarea("cargar_redshift",
                   f'if [ -n "$REPROCESAR" ]; then\n'
                   f'  exec {PY} -m ecommerce_dw.loading.load --batch-id "$REPROCESAR" --recargar\n'
                   f'fi\n'
                   f'exec {PY} -m ecommerce_dw.loading.load',
                   trigger_rule="none_failed")

    seed = dbt("dbt_seed", "seed", retries=1)
    intermedia = dbt("dbt_run_intermediate", "run --select path:models/intermediate", retries=1)
    cuarentena = dbt("dbt_run_quarantine", "run --select path:models/quarantine", retries=1)
    snapshots = dbt("dbt_snapshot", "snapshot", retries=1)
    marts = dbt("dbt_run_marts", "run --select path:models/marts", retries=1)
    # Una prueba de datos que falla no se arregla reintentando.
    pruebas = dbt("dbt_test", "test", retries=0)
    conciliar = tarea("validar_conciliacion",
                      f"exec {PY} -m ecommerce_dw.loading.consultas conciliacion huella cuarentena", retries=1)

    registrar = PythonOperator(task_id="registrar_resultado", python_callable=registrar_resultado,
                               trigger_rule="all_done", retries=0)

    validar >> extraer >> registrar_lote >> cargar
    cargar >> seed >> intermedia >> cuarentena >> snapshots >> marts >> pruebas >> conciliar >> registrar
