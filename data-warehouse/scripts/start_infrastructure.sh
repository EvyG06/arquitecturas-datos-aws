#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Building the Airflow image and starting project services...\n'
compose up --build -d
printf 'Infrastructure is running.\n'
compose ps
