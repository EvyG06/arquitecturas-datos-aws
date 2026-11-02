#!/usr/bin/env bash
# Corre EN LA EC2. Ejecuta un modulo del data warehouse dentro del contenedor del scheduler, con el rol de
# pipeline y el venv del DW. Ejemplos:
#   ./orquestacion/dw.sh sources.create_schema
#   ./orquestacion/dw.sh sources.generate_data --seed 42
set -euo pipefail
cd "$(dirname "$0")"
[ $# -gt 0 ] || { echo "Uso: $0 <modulo de ecommerce_dw> [argumentos]"; exit 2; }
MODULE="$1"
shift
exec docker compose exec -T -e PYTHONPATH=/opt/dw/src -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler \
  /home/airflow/dw-venv/bin/python -m "ecommerce_dw.$MODULE" "$@"
