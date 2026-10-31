#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Generating source data...\n'
run_airflow_image python /opt/airflow/src/ecommerce_dw/sources/generate_data.py "$@"
