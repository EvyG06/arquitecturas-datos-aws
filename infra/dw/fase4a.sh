#!/usr/bin/env bash
# Fase 4a del DW, de punta a punta: pruebas y commit, secretos de los lectores, roles, permisos, bucket,
# arranque de la infraestructura, columna CDC, datos y usuarios de solo lectura, y la evidencia final.
# Corre en el Mac despues de arq. Idempotente: si falla a mitad, se corrige y se vuelve a correr completo.
# Toda la salida queda tambien en .logs/ (fuera de git) para revisarla despues.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
: "${AIRFLOW_INSTANCE:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
mkdir -p .logs
LOG=".logs/fase4a_$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "$LOG") 2>&1

paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
remoto() { ./infra/ec2/remoto.sh "$@"; }
dw() { remoto "arquitecturas-datos-aws/orquestacion/dw.sh $*"; }
param() { aws ssm get-parameter --name "/arq-datos/dw/$1" --query Parameter.Value --output text; }

paso "0. Pruebas y commit"
python -m pip install -q -r data-warehouse/requirements-aws.txt "moto[ssm,secretsmanager]" pytest
PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q -p no:cacheprovider
python -m pytest data-lake/tests -q -p no:cacheprovider
if [ -n "$(git status --porcelain)" ]; then
  git add -A
  git commit -q -F - <<'MSG'
dw: rol de fuentes, usuarios lectores, cdc y bucket del dw

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX
MSG
fi
git push -q
git log --oneline -1

paso "1. Secretos de los usuarios lectores"
python infra/dw/03_secretos_lectores.py

paso "2. Rol arq-datos-fuentes"
./infra/iam/04_rol_fuentes.sh

paso "3. Rol de la instancia: puede asumir pipeline y fuentes"
./infra/iam/02_ec2_role.sh

paso "4. Permisos del pipeline sobre el DW"
./infra/iam/03_dw_access.sh

paso "5. Bucket del DW"
./infra/dw/02_bucket.sh

paso "6. Simulacion IAM: el pipeline solo lee los secretos de los lectores y solo borra en _control/"
PIPE="arn:aws:iam::${ACCOUNT_ID}:role/arq-datos-pipeline"
aws iam simulate-principal-policy --policy-source-arn "$PIPE" --action-names secretsmanager:GetSecretValue \
  --resource-arns "$(param mysql_secret_arn)" "$(param docdb_secret_arn)" \
                  "$(param mysql_reader_secret_arn)" "$(param docdb_reader_secret_arn)" \
  --query 'EvaluationResults[].[EvalResourceName, EvalDecision]' --output table
aws iam simulate-principal-policy --policy-source-arn "$PIPE" --action-names s3:DeleteObject \
  --resource-arns "arn:aws:s3:::${DW_BUCKET}/raw/mysql/orders/batch_id=x/part-0.parquet" \
                  "arn:aws:s3:::${DW_BUCKET}/_control/extraccion/x.json" \
  --query 'EvaluationResults[].[EvalResourceName, EvalDecision]' --output table

paso "7. Encender EC2, RDS y DocumentDB"
./infra/encender.sh

paso "8. Codigo nuevo en la EC2, perfil fuentes y DAG del data lake en pausa"
remoto 'set -o pipefail; cd arquitecturas-datos-aws && git pull --ff-only && ./orquestacion/setup_ec2.sh 2>&1 | tail -6 && cd orquestacion && docker compose exec -T airflow-scheduler airflow dags pause dl_noticias_rss'

paso "9. Columna CDC en MySQL"
dw sources.cdc

paso "10. Datos regenerados con marca CDC (seed 42, mismos conteos)"
dw sources.generate_data

paso "11. Usuarios de solo lectura en las dos fuentes"
dw sources.create_readers

paso "12. Evidencia: TLS, conteos, CDC y permisos del lector"
dw sources.inspect_sources

paso "13. Logs del intento con fallo controlado de la fila 5 del DAG del data lake"
FILA5=$(cat <<'CMD'
cd arquitecturas-datos-aws/orquestacion
files=$(docker compose exec -T airflow-scheduler sh -c 'ls /opt/airflow/logs/dag_id=dl_noticias_rss/run_id=manual__2026-11-02T19:38:59*/task_id=publicar_silver/attempt=*.log' 2>/dev/null) || true
[ -n "$files" ] || { echo "No hay logs de esa ejecucion"; exit 0; }
for f in $files; do
  echo "== ${f##*/}"
  docker compose exec -T airflow-scheduler grep -h -i -E 'controlad|Marking task as|return code' "$f" | cut -c1-220
done
CMD
)
remoto "$FILA5" || echo "No se pudieron leer los logs de la fila 5, no bloquea la fase"

paso "Fase 4a completa"
echo "Log: $LOG"
echo "EC2 y bases quedan encendidas para la Fase 4b. Para apagar todo: ./infra/detener.sh"
