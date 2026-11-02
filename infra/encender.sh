#!/usr/bin/env bash
# Enciende la EC2 de Airflow y las dos fuentes del DW, y espera a que todo acepte conexiones. Idempotente.
# Da las tres ordenes de arranque antes de esperar, asi arrancan en paralelo (unos 5 a 10 minutos).
set -euo pipefail
: "${AIRFLOW_INSTANCE:?Corre arq primero}"
RDS=arq-datos-dw-mysql
DOCDB=arq-datos-dw-docdb

ec2_estado() { aws ec2 describe-instances --instance-ids "$AIRFLOW_INSTANCE" --query 'Reservations[0].Instances[0].State.Name' --output text; }
rds_estado() { aws rds describe-db-instances --db-instance-identifier "$RDS" --query 'DBInstances[0].DBInstanceStatus' --output text; }
docdb_estado() { aws docdb describe-db-clusters --db-cluster-identifier "$DOCDB" --query 'DBClusters[0].Status' --output text; }
ssm_estado() { aws ssm describe-instance-information --filters Key=InstanceIds,Values="$AIRFLOW_INSTANCE" --query 'InstanceInformationList[0].PingStatus' --output text; }

# Solo se arranca lo que esta detenido. Si algo esta "stopping", la siguiente vuelta lo encuentra detenido.
arrancar() {
  if [ "$1" = stopped ]; then aws ec2 start-instances --instance-ids "$AIRFLOW_INSTANCE" >/dev/null; echo "EC2 encendiendo"; fi
  if [ "$2" = stopped ]; then aws rds start-db-instance --db-instance-identifier "$RDS" >/dev/null; echo "RDS MySQL encendiendo"; fi
  if [ "$3" = stopped ]; then aws docdb start-db-cluster --db-cluster-identifier "$DOCDB" >/dev/null; echo "DocumentDB encendiendo"; fi
}

for i in $(seq 1 120); do
  e=$(ec2_estado); r=$(rds_estado); d=$(docdb_estado)
  arrancar "$e" "$r" "$d"
  s=None
  if [ "$e" = running ]; then s=$(ssm_estado); fi
  if [ "$s" = Online ] && [ "$r" = available ] && [ "$d" = available ]; then
    aws docdb wait db-instance-available --db-instance-identifier "${DOCDB}-1"
    echo "$(date +%H:%M:%S) listo: ec2=$e ssm=$s rds=$r docdb=$d"
    exit 0
  fi
  if [ $((i % 3)) = 1 ]; then echo "$(date +%H:%M:%S) ec2=$e ssm=$s rds=$r docdb=$d"; fi
  sleep 20
done
echo "Despues de 40 minutos no todo quedo disponible"
exit 1
