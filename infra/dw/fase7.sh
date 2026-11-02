#!/usr/bin/env bash
# Fase 7 del DW: QuickSight Enterprise (prueba de 30 dias) sobre el Redshift privado, por conexion VPC.
#   1. Suscripcion, autor ADMIN, red y conexion VPC (pide tu correo una vez).
#   2. Usuario bi_lector en Redshift y la tabla marts.ventas_dashboard, la unica que puede leer.
#   3. Fuente, dataset SPICE, analisis y dashboard por API.
#   4. Demostracion de punta a punta: lote de negocio nuevo, DAG completo y refresco de SPICE desde el DAG.
# Corre en el Mac despues de arq. La salida queda en .logs/ y las evidencias en evidencias/dw/quicksight/.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
: "${AIRFLOW_INSTANCE:?Corre arq primero, sola en su propia linea}"
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
EV=evidencias/dw/quicksight
mkdir -p .logs "$EV"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG=".logs/fase7_$STAMP.log"
exec > >(tee -a "$LOG") 2>&1

DAG=dw_ecommerce
RUN_DEMO=manual__demo_dashboard_$STAMP
ORQ=arquitecturas-datos-aws/orquestacion
paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
remoto() { ./infra/ec2/remoto.sh "$@"; }
dw() { remoto "$ORQ/dw.sh $*"; }
dag() { remoto "$ORQ/dag.sh $*"; }
dbt() { remoto "set -o pipefail; $ORQ/dbt.sh $* 2>&1 | tail -n 25"; }
evidencia() {
  local file="$EV/$1"; shift
  set +e; "$@" 2>&1 | tee "$file"; local rc=${PIPESTATUS[0]}; set -e
  return "$rc"
}
exige() { case " $1 " in *" $2 "*) ;; *) echo "Codigo $2 inesperado (se esperaba $1)"; exit 1 ;; esac; }
lineas_total() { awk '$1 == "Todos" && $2 == "los" {print $4; exit}' "$EV/$1"; }

paso "0. Pruebas y commit"
python -m pip install -q -r data-warehouse/requirements-aws.txt "moto[s3,ssm,secretsmanager]" pytest
PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q -p no:cacheprovider
python -m pytest data-lake/tests -q -p no:cacheprovider
python infra/dw/08_quicksight_recursos.py --validar
if [ -n "$(git status --porcelain -- . ':!evidencias')" ]; then
  git add -A -- . ':!evidencias'
  git commit -q -F - <<'MSG'
dw: quicksight enterprise por conexion vpc, tabla del dashboard y refresco de spice desde el dag

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX
MSG
fi
git push -q
git log --oneline -1

paso "1. QuickSight Enterprise, autor, red y conexion VPC al Redshift privado"
./infra/dw/07_quicksight.sh

paso "2. Usuario bi_lector en Redshift"
python infra/dw/06_bi_lector.py

paso "3. Codigo nuevo en la EC2 y tabla marts.ventas_dashboard con permiso solo para bi_lector"
./infra/encender.sh
remoto "set -o pipefail; cd arquitecturas-datos-aws && git pull --ff-only && ./orquestacion/setup_ec2.sh 2>&1 | tail -3"
rc=0; evidencia 01_dbt_ventas_dashboard.txt dbt build --select ventas_dashboard || rc=$?; exige "0" "$rc"

paso "4. Fuente de datos, dataset SPICE, analisis y dashboard"
evidencia 02_recursos_quicksight.txt python infra/dw/08_quicksight_recursos.py
evidencia 03_indicador_inicial.txt dw loading.consultas indicador

paso "5. Demostracion: lote de negocio 8 desde las fuentes hasta el dashboard, sin intervencion manual"
dw sources.append_batch --batch-id 8 || echo "append_batch fallo: si el lote de negocio 8 ya existia, se continua"
dag activar "$DAG"
remoto "$ORQ/dag.sh disparar $DAG $RUN_DEMO"
rc=0; evidencia 04a_ejecucion_demo.txt dag esperar "$DAG" "$RUN_DEMO" || rc=$?
evidencia 04b_tareas_demo.txt dag tareas "$DAG" "$RUN_DEMO"
evidencia 04c_log_refresco.txt remoto "$ORQ/dag.sh log $DAG $RUN_DEMO refrescar_quicksight 'batch_id=|SPICE|Marking task as'"
dag pausar "$DAG"
exige "0" "$rc"
evidencia 05_indicador_tras_lote.txt dw loading.consultas indicador
A=$(lineas_total 03_indicador_inicial.txt); B=$(lineas_total 05_indicador_tras_lote.txt)
if [ "$((B - A))" = 3 ]; then
  echo "OK: el indicador paso de $A a $B lineas con el lote nuevo" | tee -a "$EV/05_indicador_tras_lote.txt"
else
  echo "ERROR: se esperaban 3 lineas nuevas y hubo $((B - A))" | tee -a "$EV/05_indicador_tras_lote.txt"; exit 1
fi

paso "6. Commit de las evidencias"
git add evidencias
git diff --cached --quiet || git commit -q -m "dw: evidencias de quicksight" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"
git push -q
git log --oneline -1

paso "Fase 7 completa"
echo "Log: $LOG"
echo "Abre el dashboard con el mismo usuario IAM que corrio este script:"
echo "  https://${AWS_REGION}.quicksight.aws.amazon.com/sn/dashboards/arq-datos-ventas"
echo "Compara el KPI con la fila 'Todos los canales' de $EV/05_indicador_tras_lote.txt y prueba el filtro de canal."
echo "Al entregar, cancela QuickSight con ./infra/dw/90_quicksight_baja.sh para que no cobre al terminar la prueba."
