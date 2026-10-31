#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
require_env_file
printf 'Stopping project services while preserving local data...\n'
compose down
printf 'Infrastructure stopped.\n'
