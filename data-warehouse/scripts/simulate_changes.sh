#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Applying repeatable source changes...\n'
run_airflow_image python /opt/airflow/src/ecommerce_dw/sources/simulate_changes.py "$@"
