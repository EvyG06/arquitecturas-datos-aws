#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file

use_existing_sources=false
if [[ "${1:-}" == "--existing-sources" ]]; then
  [[ "$#" -eq 1 ]] || {
    printf 'Error: --existing-sources does not accept generation options.\n' >&2
    exit 2
  }
  use_existing_sources=true
fi

scripts_dir="$PROJECT_ROOT/scripts"
if [[ "$use_existing_sources" == true ]]; then
  printf 'Starting existing services without rebuilding the image.\n'
  compose up -d
else
  "$scripts_dir/start_infrastructure.sh"
fi
"$scripts_dir/create_source_schema.sh"
if [[ "$use_existing_sources" == true ]]; then
  printf 'Using existing source data without regeneration.\n'
else
  "$scripts_dir/generate_data.sh" "$@"
fi

dag_available=false
for _ in {1..60}; do
  if compose exec -T airflow-scheduler python -c 'from airflow.models.dag import DagModel; from airflow.settings import Session; session = Session(); ready = session.query(DagModel).filter(DagModel.dag_id.in_(["ecommerce_pipeline", "extract_to_raw", "transform_warehouse"])).count() == 3; session.close(); raise SystemExit(0 if ready else 1)' >/dev/null 2>&1; then
    dag_available=true
    break
  fi
  sleep 2
done
[[ "$dag_available" == true ]] || {
  printf 'Error: project DAGs were not registered in Airflow metadata. Check scheduler logs.\n' >&2
  exit 1
}

compose exec -T airflow-scheduler airflow dags unpause --yes --treat-dag-id-as-regex '^(ecommerce_pipeline|extract_to_raw|transform_warehouse)$' >/dev/null

execution_date="$(date -u +'%Y-%m-%dT%H:%M:%S+00:00')"
printf 'Triggering ecommerce_pipeline at %s...\n' "$execution_date"
compose exec -T airflow-scheduler airflow dags trigger --exec-date "$execution_date" ecommerce_pipeline >/dev/null

for _ in {1..180}; do
  state="$(compose exec -T airflow-scheduler airflow dags state ecommerce_pipeline "$execution_date" 2>/dev/null | tail -n 1 | tr -d '\r')"
  case "$state" in
    success)
      printf 'ecommerce_pipeline completed successfully.\n'
      exit 0
      ;;
    failed)
      printf 'Error: ecommerce_pipeline failed. Check Airflow task logs.\n' >&2
      exit 1
      ;;
  esac
  sleep 5
done
printf 'Error: ecommerce_pipeline did not finish within 15 minutes.\n' >&2
exit 1
