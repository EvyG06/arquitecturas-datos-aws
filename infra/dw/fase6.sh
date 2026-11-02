#!/usr/bin/env bash
# Fase 6 del DW: el DAG dw_ecommerce en Airflow, sin intervencion manual entre etapas.
#   1. Ejecucion programada que procesa un lote de negocio nuevo desde las fuentes.
#   2. Fallo controlado en la carga a Redshift: el reintento recupera sin perder ni duplicar.
#   3. Reproceso del lote del paso 2 desde S3, sin consultar las fuentes: la huella no cambia.
# Corre en el Mac despues de arq. La salida queda en .logs/ y cada escenario en evidencias/dw/dag/.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
: "${AIRFLOW_INSTANCE:?Corre arq primero, sola en su propia linea}"
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
EV=evidencias/dw/dag
mkdir -p .logs "$EV"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG=".logs/fase6_$STAMP.log"
exec > >(tee -a "$LOG") 2>&1

DAG=dw_ecommerce
RUN_FALLO=manual__fallo_carga_$STAMP
RUN_REPRO=manual__reproceso_$STAMP
ORQ=arquitecturas-datos-aws/orquestacion
paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
remoto() { ./infra/ec2/remoto.sh "$@"; }
dw() { remoto "$ORQ/dw.sh $*"; }
dag() { remoto "$ORQ/dag.sh $*"; }
evidencia() {
  local file="$EV/$1"; shift
  set +e; "$@" 2>&1 | tee "$file"; local rc=${PIPESTATUS[0]}; set -e
  return "$rc"
}
exige() { case " $1 " in *" $2 "*) ;; *) echo "Codigo $2 inesperado (se esperaba $1)"; exit 1 ;; esac; }
filas() { awk -v t="$2" '$1 == t {print $2; exit}' "$EV/$1"; }  # filas <archivo de huella> <tabla>
crecio() { # crecio <antes> <despues> <filas esperadas por hecho>
  local ok=1 t
  for t in fct_sales fct_reviews fct_navigation_events; do
    local a b; a=$(filas "$1" "$t"); b=$(filas "$2" "$t")
    echo "$t: $a -> $b (esperado +$3)"
    [ "$((b - a))" = "$3" ] || ok=0
  done
  [ "$ok" = 1 ] && echo "OK: cada hecho crecio exactamente $3 filas" || { echo "ERROR: los hechos no crecieron en $3"; return 1; }
}

paso "0. Pruebas y commit"
python -m pip install -q -r data-warehouse/requirements-aws.txt "moto[s3,ssm,secretsmanager]" pytest
PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q -p no:cacheprovider
python -m pytest data-lake/tests -q -p no:cacheprovider
if [ -n "$(git status --porcelain -- . ':!evidencias')" ]; then
  git add -A -- . ':!evidencias'
  git commit -q -F - <<'MSG'
dw: dag dw_ecommerce con extraccion por fuente, carga, dbt, pruebas y registro del lote

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX
MSG
fi
git push -q
git log --oneline -1

paso "1. Encender EC2 y fuentes, codigo nuevo en la EC2 y DAG sin errores de importacion"
./infra/encender.sh
remoto "set -o pipefail; cd arquitecturas-datos-aws && git pull --ff-only && ./orquestacion/setup_ec2.sh 2>&1 | tail -3 && cd orquestacion && docker compose exec -T airflow-scheduler airflow dags reserialize >/dev/null 2>&1; docker compose exec -T airflow-scheduler airflow dags list-import-errors 2>/dev/null"

paso "2. Ejecucion programada con un lote de negocio nuevo (6) desde las fuentes"
evidencia 01a_huella_antes.txt dw loading.consultas huella
dw sources.append_batch --batch-id 6 || echo "append_batch fallo: si el lote de negocio 6 ya existia, se continua"
# Al quitar la pausa, con catchup=False Airflow crea la ejecucion del ultimo intervalo diario ya cumplido.
dag activar "$DAG"
RUN_PROG=""
for i in $(seq 1 18); do
  RUN_PROG=$(dag ultima "$DAG" | grep -E '^scheduled__' | tail -1 || true)
  [ -n "$RUN_PROG" ] && break
  sleep 10
done
[ -n "$RUN_PROG" ] || { echo "Airflow no creo la ejecucion programada"; exit 1; }
echo "Ejecucion programada: $RUN_PROG"
rc=0; evidencia 01b_ejecucion_programada.txt dag esperar "$DAG" "$RUN_PROG" || rc=$?
evidencia 01c_tareas_programada.txt dag tareas "$DAG" "$RUN_PROG"
exige "0" "$rc"
evidencia 01d_huella_despues.txt dw loading.consultas huella
crecio 01a_huella_antes.txt 01d_huella_despues.txt 3 | tee -a "$EV/01d_huella_despues.txt"

paso "3. Fallo controlado en la carga ($RUN_FALLO) con lote de negocio 7"
dw sources.append_batch --batch-id 7 || echo "append_batch fallo: si el lote de negocio 7 ya existia, se continua"
remoto "$ORQ/dag.sh disparar $DAG $RUN_FALLO '{\"fallo_controlado\": \"carga_a_mitad\"}'"
rc=0; evidencia 02a_ejecucion_fallo.txt dag esperar "$DAG" "$RUN_FALLO" || rc=$?
evidencia 02b_tareas_fallo.txt dag tareas "$DAG" "$RUN_FALLO"
evidencia 02c_log_carga.txt remoto "$ORQ/dag.sh log $DAG $RUN_FALLO cargar_redshift 'tarea=|division by zero|sentencia que fallo|lote cargado|Marking task as'"
exige "0" "$rc"
evidencia 02d_huella_despues_fallo.txt dw loading.consultas huella
crecio 01d_huella_despues.txt 02d_huella_despues_fallo.txt 3 | tee -a "$EV/02d_huella_despues_fallo.txt"

paso "4. Reproceso del lote $RUN_FALLO desde S3 ($RUN_REPRO)"
remoto "$ORQ/dag.sh disparar $DAG $RUN_REPRO '{\"reprocesar_batch_id\": \"$RUN_FALLO\"}'"
rc=0; evidencia 03a_ejecucion_reproceso.txt dag esperar "$DAG" "$RUN_REPRO" || rc=$?
evidencia 03b_tareas_reproceso.txt dag tareas "$DAG" "$RUN_REPRO"
exige "0" "$rc"
evidencia 03c_huella_reproceso.txt dw loading.consultas huella
crecio 02d_huella_despues_fallo.txt 03c_huella_reproceso.txt 0 | tee -a "$EV/03c_huella_reproceso.txt"

paso "5. Historial de ejecuciones y registros del resultado en S3"
evidencia 04_ejecuciones.txt dag ejecuciones "$DAG"
evidencia 05_registros_s3.txt sh -c "aws s3 ls 's3://$DW_BUCKET/_control/pipeline/' --recursive && aws s3 cp 's3://$DW_BUCKET/_control/pipeline/batch_id=$RUN_FALLO/$RUN_FALLO.json' -"
dag pausar "$DAG"

paso "6. Commit de las evidencias"
git add evidencias
git diff --cached --quiet || git commit -q -m "dw: evidencias del dag dw_ecommerce" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"
git push -q
git log --oneline -1

paso "Fase 6 completa"
echo "Log: $LOG"
echo "El DAG quedo en pausa. EC2 y fuentes siguen encendidas. Para apagarlas: ./infra/detener.sh"
