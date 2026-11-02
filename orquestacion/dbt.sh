#!/usr/bin/env bash
# Corre EN LA EC2. dbt contra Redshift dentro del contenedor del scheduler, con el rol de pipeline. Ejemplos:
#   ./orquestacion/dbt.sh debug
#   ./orquestacion/dbt.sh build
set -euo pipefail
cd "$(dirname "$0")"
EXTRA=()
if [ -n "${DW_FAULT:-}" ]; then EXTRA+=(-e "DW_FAULT=$DW_FAULT"); fi
exec docker compose exec -T -e AWS_PROFILE=pipeline ${EXTRA[@]+"${EXTRA[@]}"} airflow-scheduler \
  bash /opt/dw/scripts/aws/dbt_aws.sh "$@"
