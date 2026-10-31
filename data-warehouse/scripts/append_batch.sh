#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
[[ "$#" -eq 2 && "$1" == "--batch-id" ]] || {
  printf 'Usage: %s --batch-id NUMBER\n' "$0" >&2
  exit 2
}
printf 'Appending daily source batch %s...\n' "$2"
run_airflow_image python /opt/airflow/src/ecommerce_dw/sources/append_batch.py "$@"
