#!/usr/bin/env bash
# Apaga lo que cobra por hora: la EC2 de Airflow y las fuentes del DW. Los datos se conservan.
# AWS vuelve a encender solas las bases detenidas despues de 7 dias: si pasa una semana, corre esto otra vez.
set -euo pipefail
: "${AIRFLOW_INSTANCE:?Corre arq primero}"
RDS=arq-datos-dw-mysql
DOCDB=arq-datos-dw-docdb

if [ "$(aws ec2 describe-instances --instance-ids "$AIRFLOW_INSTANCE" --query 'Reservations[0].Instances[0].State.Name' --output text)" = running ]; then
  aws ec2 stop-instances --instance-ids "$AIRFLOW_INSTANCE" >/dev/null && echo "EC2 apagandose"
fi
if [ "$(aws rds describe-db-instances --db-instance-identifier "$RDS" --query 'DBInstances[0].DBInstanceStatus' --output text)" = available ]; then
  aws rds stop-db-instance --db-instance-identifier "$RDS" >/dev/null && echo "RDS MySQL apagandose"
fi
if [ "$(aws docdb describe-db-clusters --db-cluster-identifier "$DOCDB" --query 'DBClusters[0].Status' --output text)" = available ]; then
  aws docdb stop-db-cluster --db-cluster-identifier "$DOCDB" >/dev/null && echo "DocumentDB apagandose"
fi
echo "Listo. Lo que ya estaba apagado no se toca."
