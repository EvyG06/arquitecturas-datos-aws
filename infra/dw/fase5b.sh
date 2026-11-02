#!/usr/bin/env bash
# Fase 5b del DW: dbt en Redshift y los escenarios de la seccion 3.5. Carga inicial, registros defectuosos,
# repeticion de un lote exitoso, nuevo lote incremental, cambio SCD2 y conciliacion con la fuente.
# El fallo controlado con reejecucion se demuestra en la Fase 6 desde el DAG.
# Corre en el Mac despues de arq. La salida queda en .logs/ y cada escenario en evidencias/dw/dbt/.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
: "${AIRFLOW_INSTANCE:?Corre arq primero, sola en su propia linea}"
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
EV=evidencias/dw/dbt
mkdir -p .logs "$EV"
LOG=".logs/fase5b_$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "$LOG") 2>&1

paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
remoto() { ./infra/ec2/remoto.sh "$@"; }
dw() { remoto "arquitecturas-datos-aws/orquestacion/dw.sh $*"; }
# La salida por SSM se corta a 24.000 caracteres: se muestra solo el final de dbt y despues el resumen de
# run_results.json, que trae los conteos y el mensaje completo de cada error.
dbt() { remoto "set -o pipefail; arquitecturas-datos-aws/orquestacion/dbt.sh $* 2>&1 | tail -n 40"; }
resumen() { dw transformations.resumen_dbt; }
# dbt_build <evidencia>: corre dbt build, guarda el final y el resumen, y corta el script si algo fallo.
dbt_build() {
  local rc=0 rc2=0
  evidencia "$1" dbt build || rc=$?
  set +e; resumen 2>&1 | tee -a "$EV/$1"; rc2=${PIPESTATUS[0]}; set -e
  exige "0" "$rc"; exige "0" "$rc2"
}
evidencia() {
  local file="$EV/$1"; shift
  set +e; "$@" 2>&1 | tee "$file"; local rc=${PIPESTATUS[0]}; set -e
  return "$rc"
}
exige() { case " $1 " in *" $2 "*) ;; *) echo "Codigo $2 inesperado (se esperaba $1)"; exit 1 ;; esac; }
s3_existe() { aws s3api head-object --bucket "$DW_BUCKET" --key "$1" >/dev/null 2>&1; }
huella() { awk '/^## huella/{f=1; next} f && /^[[:space:]]*$/{exit} f' "$1"; }
# Extrae y carga un lote nuevo. El lote de negocio solo se agrega si ese lote de extraccion aun no existe.
nuevo_lote() { # nuevo_lote <batch_id> <comando que cambia las fuentes>
  local batch=$1; shift
  if ! s3_existe "_control/extraction/batch_id=$batch/batch.json"; then
    dw "$@" || echo "El cambio en las fuentes fallo: si ya se habia aplicado en una corrida anterior, se continua"
  fi
  local rc=0; dw extraction.extract --batch-id "$batch" || rc=$?; exige "0 99" "$rc"
  rc=0; dw loading.load || rc=$?; exige "0" "$rc"
}

paso "0. Pruebas y commit"
python -m pip install -q -r data-warehouse/requirements-aws.txt "moto[s3,ssm,secretsmanager]" pytest
PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q -p no:cacheprovider
python -m pytest data-lake/tests -q -p no:cacheprovider
if [ -n "$(git status --porcelain -- . ':!evidencias')" ]; then
  git add -A -- . ':!evidencias'
  git commit -q -F - <<'MSG'
dw: consultas de validacion y escenarios de dbt en redshift

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX
MSG
fi
git push -q
git log --oneline -1

paso "1. Encender EC2 y fuentes, y codigo nuevo en la EC2"
./infra/encender.sh
remoto 'set -o pipefail; cd arquitecturas-datos-aws && git pull --ff-only && ./orquestacion/setup_ec2.sh 2>&1 | tail -3'

paso "2. Carga inicial: dbt build (seeds, snapshots, modelos y pruebas) sobre los lotes cargados"
dbt_build 01_dbt_build_inicial.txt

paso "3. Datos en los modelos, rechazos trazables y conciliacion con MySQL"
rc=0; evidencia 02_huella_inicial.txt dw loading.consultas huella capas || rc=$?; exige "0" "$rc"
rc=0; evidencia 03_cuarentena.txt dw loading.consultas cuarentena || rc=$?; exige "0" "$rc"
rc=0; evidencia 04_conciliacion.txt dw loading.consultas conciliacion || rc=$?; exige "0" "$rc"

paso "4. Repetir un lote exitoso: recargar ext_04 y dbt build, sin hechos ni versiones nuevas"
rc=0; dw loading.load --batch-id ext_04_incremental_negocio4 --recargar || rc=$?; exige "0" "$rc"
dbt_build 05a_dbt_build_repeticion.txt
rc=0; evidencia 05b_huella_repeticion.txt dw loading.consultas huella || rc=$?; exige "0" "$rc"
if diff <(huella "$EV/02_huella_inicial.txt") <(huella "$EV/05b_huella_repeticion.txt"); then
  echo "OK: la huella de hechos y dimensiones es identica antes y despues de repetir el lote" | tee -a "$EV/05b_huella_repeticion.txt"
else
  echo "ERROR: la huella cambio al repetir el lote" | tee -a "$EV/05b_huella_repeticion.txt"; exit 1
fi

paso "5. Nuevo lote incremental: lote de negocio 5"
nuevo_lote ext_05_incremental_negocio5 sources.append_batch --batch-id 5
dbt_build 06a_dbt_build_incremental.txt
rc=0; evidencia 06b_huella_incremental.txt dw loading.consultas huella capas || rc=$?; exige "0" "$rc"

paso "6. Cambio en un atributo historizado: cliente 1 a Medellin y producto 1 a Hogar desde 2025-07-01"
nuevo_lote ext_06_scd2_cliente1_producto1 sources.simulate_changes
dbt_build 07a_dbt_build_scd2.txt
rc=0; evidencia 07b_scd2.txt dw loading.consultas scd2 huella || rc=$?; exige "0" "$rc"
rc=0; evidencia 08_conciliacion_final.txt dw loading.consultas conciliacion || rc=$?; exige "0" "$rc"
evidencia 09_cargas.txt dw loading.load --estado

paso "7. Commit de las evidencias"
git add evidencias
git diff --cached --quiet || git commit -q -m "dw: evidencias de dbt en redshift" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"
git push -q
git log --oneline -1

paso "Fase 5b completa"
echo "Log: $LOG"
echo "EC2 y fuentes siguen encendidas. Para apagarlas: ./infra/detener.sh"
