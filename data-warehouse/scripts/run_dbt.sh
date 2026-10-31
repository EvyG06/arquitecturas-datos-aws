#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
[[ -f "$PROJECT_ROOT/warehouse/warehouse.duckdb" ]] || { printf 'Error: warehouse/warehouse.duckdb is missing. Run scripts/run_pipeline.sh first.\n' >&2; exit 1; }
printf 'Installing locked dbt packages...\n'
dbt_command deps
printf 'Loading seeds...\n'
dbt_command seed --threads 1
printf 'Building intermediate models...\n'
dbt_command run --select path:models/intermediate --threads 1
printf 'Updating snapshots...\n'
dbt_command snapshot --threads 1
printf 'Building warehouse models...\n'
dbt_command run --threads 1
printf 'Running dbt data tests...\n'
dbt_command test --threads 1
printf 'dbt workflow completed successfully.\n'
