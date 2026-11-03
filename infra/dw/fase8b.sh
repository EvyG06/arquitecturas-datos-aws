#!/usr/bin/env bash
# Fase 8b: quita el rol Pro de QuickSight y repite costos e inventario con los scripts corregidos.
#   1. El usuario IAM quedo como ADMIN_PRO al crear la suscripcion. Un usuario Pro activa 250 USD al mes por cuenta.
#   2. costos.py ahora cobra cada rol por su precio y toma el precio bajo demanda de Redshift, no el de reserva.
#   3. eliminar_todo.py ahora reconoce los secretos administrados por RDS y los clusters listados por id de recurso.
# No necesita nada encendido. Corre en el Mac despues de arq. Las evidencias 01 de la fase 8 no se tocan.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
mkdir -p .logs evidencias/costos evidencias/eliminacion evidencias/dw/quicksight
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG=".logs/fase8b_$STAMP.log"
exec > >(tee -a "$LOG") 2>&1
paso() { printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"; }
FIRMA="Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"

paso "0. Pruebas del script de eliminacion"
python -m pytest infra/tests -q -p no:cacheprovider

paso "1. Commit de los scripts y la documentacion corregidos"
chmod 755 infra/dw/09_quicksight_sin_pro.sh infra/dw/fase8b.sh
if [ -n "$(git status --porcelain -- . ':!evidencias')" ]; then
  git add -A -- . ':!evidencias'
  git commit -q -m "infra: quicksight sin usuarios pro, costos por rol e inventario con secretos administrados" -m "$FIRMA"
fi
git push -q
git log --oneline -1

paso "2. QuickSight sin usuarios Pro"
./infra/dw/09_quicksight_sin_pro.sh | tee evidencias/dw/quicksight/06_sin_usuarios_pro.txt

paso "3. Costos despues del cambio"
python infra/costos.py | tee evidencias/costos/02_costos_sin_pro.txt

paso "4. Inventario de eliminacion, sin borrar nada"
python infra/eliminar_todo.py | tee evidencias/eliminacion/02_inventario.txt

paso "5. Commit de las evidencias"
git add evidencias
git diff --cached --quiet || git commit -q -m "evidencias: quicksight sin pro, costos e inventario" -m "$FIRMA"
git push -q
git log --oneline -1

paso "Fase 8b completa"
echo "Log: $LOG"
