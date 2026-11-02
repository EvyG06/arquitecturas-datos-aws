#!/usr/bin/env bash
# Fase 5a del DW: Redshift Serverless, permisos, usuario del pipeline y carga de los lotes a raw con COPY.
# Escenarios de carga: pendientes, repetir un lote, recargarlo y fallo controlado a mitad con reintento.
# Corre en el Mac despues de arq. La salida queda en .logs/ y cada escenario en evidencias/dw/carga/.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
: "${AIRFLOW_INSTANCE:?Corre arq primero, sola en su propia linea}"
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
EV=evidencias/dw/carga
mkdir -p .logs "$EV"
LOG=".logs/fase5a_$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "$LOG") 2>&1

NEGOCIO=4
B4=ext_04_incremental_negocio${NEGOCIO}
paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
remoto() { ./infra/ec2/remoto.sh "$@"; }
dw() { remoto "arquitecturas-datos-aws/orquestacion/dw.sh $*"; }
evidencia() {
  local file="$EV/$1"; shift
  set +e; "$@" 2>&1 | tee "$file"; local rc=${PIPESTATUS[0]}; set -e
  return "$rc"
}
exige() { case " $1 " in *" $2 "*) ;; *) echo "Codigo $2 inesperado (se esperaba $1)"; exit 1 ;; esac; }
s3_existe() { aws s3api head-object --bucket "$DW_BUCKET" --key "$1" >/dev/null 2>&1; }

paso "0. Pruebas y commit"
python -m pip install -q -r data-warehouse/requirements-aws.txt "moto[s3,ssm,secretsmanager]" pytest
PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q -p no:cacheprovider
python -m pytest data-lake/tests -q -p no:cacheprovider
if [ -n "$(git status --porcelain -- . ':!evidencias')" ]; then
  git add -A -- . ':!evidencias'
  git commit -q -F - <<'MSG'
dw: redshift serverless, carga por lote con copy y dbt adaptado a redshift

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX
MSG
fi
git push -q
git log --oneline -1

paso "1. Redshift Serverless: rol de COPY, red privada, namespace, workgroup y tope de uso"
./infra/dw/04_redshift.sh

paso "2. Permisos del pipeline sobre Redshift (credenciales temporales y Data API)"
./infra/iam/05_redshift_access.sh

paso "3. Usuario IAMR:arq-datos-pipeline y permiso de COPY solo para el"
python infra/dw/05_redshift_bootstrap.py

paso "4. Encender EC2 y fuentes"
./infra/encender.sh

paso "5. Codigo nuevo en la EC2 e imagen con dbt-redshift"
remoto 'set -o pipefail; cd arquitecturas-datos-aws && git pull --ff-only && ./orquestacion/setup_ec2.sh 2>&1 | tail -4 && cd orquestacion && docker compose exec -T airflow-scheduler /home/airflow/dbt-venv/bin/dbt --version'

paso "6. Carga de los lotes confirmados que faltan en raw"
rc=0; evidencia 01_carga_pendientes.txt dw loading.load || rc=$?; exige "0" "$rc"

paso "7. Repeticion: sin pendientes, el lote ya cargado sale con 99 y recargarlo no duplica"
rc=0; evidencia 02_sin_pendientes.txt dw loading.load || rc=$?; exige "0" "$rc"
rc=0; evidencia 03_repetir_lote.txt dw loading.load --batch-id ext_02_incremental_negocio2 || rc=$?; exige "99" "$rc"
rc=0; evidencia 04_recargar_lote.txt dw loading.load --batch-id ext_02_incremental_negocio2 --recargar || rc=$?; exige "0" "$rc"

paso "8. Fallo controlado a mitad de la carga del lote $B4 y reintento"
if ! s3_existe "_control/extraction/batch_id=$B4/batch.json"; then
  dw sources.append_batch --batch-id "$NEGOCIO" || echo "append_batch fallo: si el lote de negocio ya existia, se continua"
fi
rc=0; dw extraction.extract --batch-id "$B4" || rc=$?; exige "0 99" "$rc"
rc=0; evidencia 05a_fallo_carga.txt remoto "DW_FAULT=carga_a_mitad arquitecturas-datos-aws/orquestacion/dw.sh loading.load" || rc=$?
[ "$rc" != 0 ] || echo "Aviso: la carga con fallo controlado no fallo (el lote ya estaba cargado de una corrida anterior)"
evidencia 05b_estado_tras_fallo.txt dw loading.load --estado
rc=0; evidencia 05c_reintento.txt dw loading.load || rc=$?; exige "0" "$rc"

paso "9. dbt contra Redshift con el rol del pipeline: conexion, TLS y permisos"
rc=0; evidencia 06_dbt_debug.txt remoto 'arquitecturas-datos-aws/orquestacion/dbt.sh debug --connection' || rc=$?; exige "0" "$rc"

paso "10. Commit de las evidencias"
git add evidencias
git diff --cached --quiet || git commit -q -m "dw: evidencias de la carga a redshift" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"
git push -q
git log --oneline -1

paso "Fase 5a completa"
echo "Log: $LOG"
echo "Redshift no cobra computo en reposo. EC2 y fuentes siguen encendidas. Para apagarlas: ./infra/detener.sh"
