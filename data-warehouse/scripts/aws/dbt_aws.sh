#!/usr/bin/env bash
# Corre DENTRO del contenedor. El proyecto dbt llega montado en solo lectura, asi que se copia a un volumen
# escribible donde dbt deja target/, logs/ y los paquetes. El endpoint de Redshift se lee de SSM.
set -euo pipefail
SRC=/opt/dw/src/dbt
W=/opt/dbt-work/project
mkdir -p "$W"
find "$W" -mindepth 1 -maxdepth 1 ! -name dbt_packages ! -name target ! -name logs -exec rm -rf {} +
cp -a "$SRC/." "$W/"
DW_REDSHIFT_HOST=$(PYTHONPATH=/opt/dw/src /home/airflow/dw-venv/bin/python -c \
  'from ecommerce_dw import aws_conn; print(aws_conn.params()["redshift_host"])')
export DW_REDSHIFT_HOST
cd "$W"
if [ ! -d dbt_packages/dbt_utils ] || ! cmp -s "$SRC/package-lock.yml" "$W/.package-lock.instalado"; then
  /home/airflow/dbt-venv/bin/dbt deps --profiles-dir "$W" --target aws
  cp "$SRC/package-lock.yml" "$W/.package-lock.instalado"
fi
exec /home/airflow/dbt-venv/bin/dbt "$@" --profiles-dir "$W" --target aws
