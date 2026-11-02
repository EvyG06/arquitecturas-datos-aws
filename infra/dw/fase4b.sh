#!/usr/bin/env bash
# Fase 4b del DW: extraccion inicial e incremental de RDS y DocumentDB a S3, con sus escenarios:
# repetir un lote, nuevo lote de negocio, fallo controlado a mitad y reintento. Corre en el Mac despues de arq.
# Los lotes tienen nombre fijo: si el script se corta, al volver a correrlo los lotes ya confirmados salen
# con codigo 99 (ya extraido) y continua. La salida queda en .logs/ y cada escenario en evidencias/dw/extraccion/.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
: "${AIRFLOW_INSTANCE:?Corre arq primero, sola en su propia linea}"
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
EV=evidencias/dw/extraccion
mkdir -p .logs "$EV"
LOG=".logs/fase4b_$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "$LOG") 2>&1

NEGOCIO=${NEGOCIO:-2}            # lote de negocio de append_batch (1 a 365) para el incremental
B1=ext_01_inicial
B2=ext_02_incremental_negocio${NEGOCIO}
B3=ext_03_fallo_y_reintento_negocio$((NEGOCIO + 1))

paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
remoto() { ./infra/ec2/remoto.sh "$@"; }
dw() { remoto "arquitecturas-datos-aws/orquestacion/dw.sh $*"; }
# Corre un comando, guarda su salida como evidencia y devuelve su codigo sin cortar el script.
evidencia() {
  local file="$EV/$1"; shift
  set +e; "$@" 2>&1 | tee "$file"; local rc=${PIPESTATUS[0]}; set -e
  return "$rc"
}
exige() { # exige <codigos aceptados> <codigo>
  case " $1 " in *" $2 "*) ;; *) echo "Codigo $2 inesperado (se esperaba $1)"; exit 1 ;; esac
}
s3_existe() { aws s3api head-object --bucket "$DW_BUCKET" --key "$1" >/dev/null 2>&1; }

paso "0. Pruebas y commit"
python -m pip install -q -r data-warehouse/requirements-aws.txt "moto[s3,ssm,secretsmanager]" pytest
PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q -p no:cacheprovider
python -m pytest data-lake/tests -q -p no:cacheprovider
if [ -n "$(git status --porcelain -- . ':!evidencias')" ]; then
  git add -A -- . ':!evidencias'
  git commit -q -F - <<'MSG'
dw: extraccion inicial e incremental a s3 con marca de agua cdc

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX
MSG
fi
git push -q
git log --oneline -1

paso "0b. Simulacion IAM del rol de pipeline, una decision por recurso"
# Con varios recursos, la decision de arriba es la combinada: el detalle esta en ResourceSpecificResults.
param() { aws ssm get-parameter --name "/arq-datos/dw/$1" --query Parameter.Value --output text; }
simular() { # simular <accion> <recursos...>
  local accion=$1; shift
  aws iam simulate-principal-policy --policy-source-arn "arn:aws:iam::${ACCOUNT_ID}:role/arq-datos-pipeline" \
    --action-names "$accion" --resource-arns "$@" \
    --query 'EvaluationResults[0].ResourceSpecificResults[].[EvalResourceName, EvalResourceDecision]' --output text |
    awk -v a="$accion" '{printf "%-30s %-13s %s\n", a, $2, $1}'
}
{
  echo "# Rol arq-datos-pipeline: allowed = permitido, implicitDeny = sin permiso"
  simular secretsmanager:GetSecretValue "$(param mysql_secret_arn)" "$(param docdb_secret_arn)" \
    "$(param mysql_reader_secret_arn)" "$(param docdb_reader_secret_arn)"
  simular s3:DeleteObject "arn:aws:s3:::${DW_BUCKET}/raw/mysql/orders/batch_id=x/part-00000.parquet" \
    "arn:aws:s3:::${DW_BUCKET}/_control/extraction/batch_id=x/plan.json"
  simular s3:PutObject "arn:aws:s3:::${DW_BUCKET}/raw/mysql/orders/batch_id=x/part-00000.parquet"
} | tee "evidencias/dw/fuentes/03_simulacion_iam.txt"

paso "1. Encender EC2, RDS y DocumentDB"
./infra/encender.sh

paso "2. Codigo nuevo en la EC2 y DW_BUCKET en el contenedor"
remoto 'set -o pipefail; cd arquitecturas-datos-aws && git pull --ff-only && ./orquestacion/setup_ec2.sh 2>&1 | tail -4 && cd orquestacion && docker compose exec -T airflow-scheduler printenv DW_BUCKET'

paso "3. Extraccion inicial ($B1)"
rc=0; evidencia 01_extraccion_inicial.txt dw extraction.extract --batch-id "$B1" --mode inicial || rc=$?
exige "0 99" "$rc"

paso "4. Repetir el lote $B1: no consulta las fuentes ni reescribe nada"
rc=0; evidencia 02_repetir_lote.txt dw extraction.extract --batch-id "$B1" || rc=$?
exige "99" "$rc"

paso "5. Nuevo lote de negocio $NEGOCIO en las fuentes y extraccion incremental ($B2)"
if ! s3_existe "_control/extraction/batch_id=$B2/batch.json"; then
  dw sources.append_batch --batch-id "$NEGOCIO" || echo "append_batch fallo: si el lote de negocio ya existia de una corrida anterior, se continua"
fi
rc=0; evidencia 03_extraccion_incremental.txt dw extraction.extract --batch-id "$B2" || rc=$?
exige "0 99" "$rc"

paso "6. Fallo controlado a mitad de la extraccion ($B3) y reintento"
if ! s3_existe "_control/extraction/batch_id=$B3/batch.json"; then
  dw sources.append_batch --batch-id "$((NEGOCIO + 1))" || echo "append_batch fallo: si el lote de negocio ya existia de una corrida anterior, se continua"
  rc=0; evidencia 04a_fallo_controlado.txt remoto "DW_FAULT=extraccion_a_mitad arquitecturas-datos-aws/orquestacion/dw.sh extraction.extract --batch-id $B3" || rc=$?
  [ "$rc" != 0 ] || { echo "El fallo controlado no fallo"; exit 1; }
fi
rc=0; evidencia 04b_reintento.txt dw extraction.extract --batch-id "$B3" || rc=$?
exige "0 99" "$rc"

paso "7. Marcas de agua e historial"
evidencia 05_estado.txt dw extraction.extract --estado

paso "8. Objetos en S3: raw por fuente, entidad y lote, y el control de cada lote"
evidencia 06_s3.txt aws s3 ls "s3://$DW_BUCKET/" --recursive

paso "9. Registro y manifiesto COPY del lote incremental"
evidencia 07_registro_lote.txt sh -c "aws s3 cp 's3://$DW_BUCKET/_control/extraction/batch_id=$B2/batch.json' - && aws s3 cp 's3://$DW_BUCKET/_control/copy/batch_id=$B2/mysql_orders.manifest' -"

paso "10. S3 rechaza sobrescribir el archivo de otro lote"
./infra/dw/evidencia_sin_sobrescritura.sh

paso "11. Commit de las evidencias"
git add evidencias
git diff --cached --quiet || git commit -q -m "dw: evidencias de la extraccion a s3" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"
git push -q
git log --oneline -1

paso "Fase 4b completa"
echo "Log: $LOG"
echo "EC2 y bases siguen encendidas para la Fase 5. Para apagar todo: ./infra/detener.sh"
