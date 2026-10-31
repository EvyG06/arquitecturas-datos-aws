#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose.yml"

compose() {
  docker compose --project-directory "$PROJECT_ROOT" -f "$COMPOSE_FILE" "$@"
}

require_docker() {
  command -v docker >/dev/null 2>&1 || {
    printf 'Error: Docker is required.\n' >&2
    exit 1
  }
  docker compose version >/dev/null 2>&1 || {
    printf 'Error: Docker Compose v2 is required.\n' >&2
    exit 1
  }
}

require_env_file() {
  [[ -f "$PROJECT_ROOT/.env" ]] || {
    printf 'Error: .env is missing. Run scripts/configure_environment.sh first.\n' >&2
    exit 1
  }
}

load_env_file() {
  require_env_file
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
}

run_airflow_image() {
  local executable="$1"
  shift
  compose run --rm --no-deps --entrypoint "$executable" airflow-scheduler "$@"
}

dbt_command() {
  run_airflow_image /home/airflow/dbt-venv/bin/dbt "$@" \
    --project-dir /opt/airflow/src/dbt \
    --profiles-dir /opt/airflow/src/dbt
}
