#!/usr/bin/env bash
# Fase 8: documentacion de los dos proyectos, estimacion de costos con el gasto real y eliminacion en simulacion.
#   1. Pruebas de los dos proyectos y del script de eliminacion, y commit de la documentacion.
#   2. Evidencia de costos: precios de la Price List API, uso medido, escenarios y Cost Explorer.
#   3. Inventario de eliminacion SIN borrar nada, para revisar que cubre todos los recursos del proyecto.
# No necesita la EC2 ni las bases encendidas. Corre en el Mac despues de arq. Salida en .logs/.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
mkdir -p .logs evidencias/costos evidencias/eliminacion
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG=".logs/fase8_$STAMP.log"
exec > >(tee -a "$LOG") 2>&1
paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
FIRMA="Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"

paso "0. Pruebas"
python -m pip install -q -r data-warehouse/requirements-aws.txt pytest \
  "moto[ec2,s3,iam,ssm,secretsmanager,glue,athena,logs,budgets,quicksight,resourcegroupstaggingapi]"
PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q -p no:cacheprovider
python -m pytest data-lake/tests -q -p no:cacheprovider
python -m pytest infra/tests -q -p no:cacheprovider
python infra/dw/08_quicksight_recursos.py --validar
bash -n infra/dw/demo.sh && bash -n data-lake/evidencias/demo.sh

paso "1. Commit de la documentacion y los scripts"
chmod 755 infra/dw/demo.sh infra/dw/fase8.sh data-lake/evidencias/demo.sh infra/eliminar_todo.py infra/costos.py
if [ -n "$(git status --porcelain -- . ':!evidencias')" ]; then
  git add -A -- . ':!evidencias'
  git commit -q -m "docs: documentacion del data lake y del data warehouse, costos, seguridad y eliminacion" -m "$FIRMA"
fi
git push -q
git log --oneline -1

paso "2. Costos: precios, uso medido, escenarios y gasto real"
python infra/costos.py | tee evidencias/costos/01_costos.txt

paso "3. Inventario de eliminacion, sin borrar nada"
python infra/eliminar_todo.py | tee evidencias/eliminacion/01_inventario.txt

paso "4. Commit de las evidencias"
git add evidencias
git diff --cached --quiet || git commit -q -m "evidencias: costos e inventario de eliminacion" -m "$FIRMA"
git push -q
git log --oneline -1

paso "Fase 8 completa"
echo "Log: $LOG"
echo "Nada quedo encendido por esta fase. Si encendiste algo antes: ./infra/detener.sh"
