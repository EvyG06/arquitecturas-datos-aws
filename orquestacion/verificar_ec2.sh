#!/usr/bin/env bash
# Corre EN LA EC2. Verifica contenedores, DAGs y el acceso del pipeline a cada servicio desde el contenedor.
set -uo pipefail
cd "$(dirname "$0")"

echo "== Esperando a que la interfaz web este sana (hasta 3 minutos)"
for _ in $(seq 1 36); do
  docker compose ps --format '{{.Service}} {{.Health}}' | grep -q '^airflow-webserver healthy' && break
  sleep 5
done
docker compose ps --format 'table {{.Service}}\t{{.Status}}'

echo
echo "== Errores de importacion de DAGs (debe decir que no hay)"
docker compose exec -T airflow-scheduler airflow dags list-import-errors

echo
echo "== DAG registrado"
docker compose exec -T airflow-scheduler airflow dags list -o plain | grep -E "dag_id|dl_noticias_rss"

echo
echo "== Pool de escrituras"
docker compose exec -T airflow-scheduler airflow pools get dl_escritura

echo
echo "== Etapa 1 del DAG ejecutada a mano dentro del contenedor (credenciales encadenadas)"
docker compose exec -T -e PYTHONPATH=/opt/dl/src airflow-scheduler \
  /home/airflow/dl-venv/bin/python -m dl.preflight
echo "preflight exit=$?"
