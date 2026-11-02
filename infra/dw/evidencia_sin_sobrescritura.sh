#!/usr/bin/env bash
# Demuestra que S3 rechaza sobrescribir el Parquet de un lote existente cuando la escritura es condicional
# (If-None-Match: *), que es como escribe la extraccion. Guarda la evidencia y la sube a git.
set -euo pipefail
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
cd "$(dirname "$0")/../.."
KEY="raw/mysql/orders/batch_id=ext_01_inicial/part-00000.parquet"
EV=evidencias/dw/extraccion/08_sin_sobrescritura.txt
VACIO=$(mktemp)
trap 'rm -f "$VACIO"' EXIT

ANTES=$(aws s3api head-object --bucket "$DW_BUCKET" --key "$KEY" --query '[ETag, ContentLength]' --output text)
SALIDA=$(aws s3api put-object --bucket "$DW_BUCKET" --key "$KEY" --body "$VACIO" --if-none-match '*' 2>&1 || true)
DESPUES=$(aws s3api head-object --bucket "$DW_BUCKET" --key "$KEY" --query '[ETag, ContentLength]' --output text)
{
  echo "# put-object con If-None-Match: * de un archivo vacio sobre un Parquet existente"
  echo "objeto:  s3://$DW_BUCKET/$KEY"
  echo "antes:   $ANTES"
  echo "intento: $SALIDA"
  echo "despues: $DESPUES"
  if [[ "$SALIDA" == *PreconditionFailed* ]] && [ "$ANTES" = "$DESPUES" ]; then
    echo "OK: S3 respondio 412 PreconditionFailed y el objeto no cambio"
  else
    echo "REVISAR: resultado inesperado"
  fi
} | tee "$EV"
git add "$EV" infra/dw
git diff --cached --quiet || git commit -q -m "dw: evidencia de que s3 no sobrescribe lotes" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01BjX2qi4h5xcRYhncWx8JHX"
git push -q
