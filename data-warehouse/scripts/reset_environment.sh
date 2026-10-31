#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Removing containers, named volumes, and local warehouse files...\n'
compose down --volumes --remove-orphans
rm -f "$PROJECT_ROOT/warehouse/warehouse.duckdb" "$PROJECT_ROOT/warehouse/warehouse.wal" "$PROJECT_ROOT/warehouse/warehouse.lock"
rm -rf "$PROJECT_ROOT/warehouse/extracts"
rm -rf "$PROJECT_ROOT/src/dbt/target" "$PROJECT_ROOT/src/dbt/logs" "$PROJECT_ROOT/src/dbt/dbt_packages"
rm -f "$PROJECT_ROOT/src/dbt/.user.yml"
find "$PROJECT_ROOT/src" -type d -name __pycache__ -prune -exec rm -rf {} +
printf 'Environment reset complete. Run scripts/start_infrastructure.sh to rebuild it.\n'
