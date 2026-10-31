#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_docker
load_env_file
for variable in MYSQL_ROOT_PASSWORD MYSQL_DATABASE MONGO_ROOT_USERNAME MONGO_ROOT_PASSWORD MONGO_DATABASE; do
  [[ -n "${!variable:-}" ]] || { printf 'Error: %s must be set in .env.\n' "$variable" >&2; exit 1; }
done

printf 'Starting MySQL and MongoDB source services...\n'
compose up -d mysql mongodb
for _ in {1..60}; do
  mysql_ready="$(compose exec -T mysql mysqladmin ping -h localhost -p"$MYSQL_ROOT_PASSWORD" --silent 2>/dev/null || true)"
  mongo_ready="$(compose exec -T mongodb mongosh --quiet --username "$MONGO_ROOT_USERNAME" --password "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin --eval 'db.adminCommand({ping:1}).ok' 2>/dev/null || true)"
  if [[ "$mysql_ready" == *alive* && "$mongo_ready" == *1* ]]; then
    break
  fi
  sleep 2
done
[[ "$mysql_ready" == *alive* && "$mongo_ready" == *1* ]] || {
  printf 'Error: source services did not become ready within 120 seconds.\n' >&2
  exit 1
}

compose exec -T mysql sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE"' \
  < "$PROJECT_ROOT/src/ecommerce_dw/sources/mysql/schema.sql"
compose exec -T mongodb mongosh --quiet \
  --username "$MONGO_ROOT_USERNAME" \
  --password "$MONGO_ROOT_PASSWORD" \
  --authenticationDatabase admin \
  "$MONGO_DATABASE" \
  --eval 'for (const name of ["reviews", "navigation_events"]) { if (!db.getCollectionNames().includes(name)) db.createCollection(name); }'
printf 'MySQL tables and MongoDB collections are ready.\n'
