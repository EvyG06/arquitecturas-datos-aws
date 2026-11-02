#!/usr/bin/env bash
# Fuentes del data warehouse: RDS for MySQL y DocumentDB. Privadas, cifradas, con TLS y con la contrasena
# maestra administrada por Secrets Manager. Solo aceptan conexiones desde el security group de Airflow.
# Idempotente: si un recurso ya existe, lo reutiliza. Tarda entre 10 y 20 minutos la primera vez.
set -euo pipefail
: "${AWS_REGION:?Corre arq primero}"
cd "$(dirname "$0")/../.."

P=arq-datos-dw
TAGS=(Key=Project,Value=arquitecturas-datos Key=Component,Value=dw)
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
AIRFLOW_SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=arq-datos-airflow Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
[ "$AIRFLOW_SG" != "None" ] || { echo "Falta el security group de Airflow, corre infra/ec2/01_instancia.sh"; exit 1; }
SUBNETS=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true \
  --query 'Subnets[].SubnetId' --output text)

# 1. Red. Las fuentes solo aceptan 3306 y 27017 desde el security group de Airflow, nunca desde una IP.
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$P-fuentes" Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws ec2 create-security-group --group-name "$P-fuentes" --vpc-id "$VPC" \
    --description "Fuentes del DW, solo desde Airflow" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$P-fuentes},{Key=Project,Value=arquitecturas-datos},{Key=Component,Value=dw}]" \
    --query GroupId --output text)
  for port in 3306 27017; do
    aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port "$port" --source-group "$AIRFLOW_SG" >/dev/null
  done
  echo "Security group creado: $SG"
fi

aws rds describe-db-subnet-groups --db-subnet-group-name "$P-mysql" >/dev/null 2>&1 ||
  aws rds create-db-subnet-group --db-subnet-group-name "$P-mysql" --db-subnet-group-description "Subredes de MySQL del DW" \
    --subnet-ids $SUBNETS --tags "${TAGS[@]}" >/dev/null
aws docdb describe-db-subnet-groups --db-subnet-group-name "$P-docdb" >/dev/null 2>&1 ||
  aws docdb create-db-subnet-group --db-subnet-group-name "$P-docdb" --db-subnet-group-description "Subredes de DocumentDB del DW" \
    --subnet-ids $SUBNETS --tags "${TAGS[@]}" >/dev/null

# 2. RDS for MySQL 8.4, misma version mayor que el mysql:8.4 local. TLS obligatorio en el servidor.
read -r MYSQL_VERSION MYSQL_FAMILY < <(aws rds describe-db-engine-versions --engine mysql \
  --query "DBEngineVersions[?starts_with(EngineVersion, '8.4')] | [-1].[EngineVersion, DBParameterGroupFamily]" --output text)
PG="$P-mysql84"
aws rds describe-db-parameter-groups --db-parameter-group-name "$PG" >/dev/null 2>&1 ||
  aws rds create-db-parameter-group --db-parameter-group-name "$PG" --db-parameter-group-family "$MYSQL_FAMILY" \
    --description "MySQL del DW con TLS obligatorio" --tags "${TAGS[@]}" >/dev/null
aws rds modify-db-parameter-group --db-parameter-group-name "$PG" \
  --parameters "ParameterName=require_secure_transport,ParameterValue=1,ApplyMethod=immediate" >/dev/null

if ! aws rds describe-db-instances --db-instance-identifier "$P-mysql" >/dev/null 2>&1; then
  aws rds create-db-instance --db-instance-identifier "$P-mysql" \
    --engine mysql --engine-version "$MYSQL_VERSION" --db-instance-class db.t4g.micro \
    --allocated-storage 20 --storage-type gp3 --storage-encrypted \
    --db-name ecommerce --master-username dw_admin --manage-master-user-password \
    --db-subnet-group-name "$P-mysql" --vpc-security-group-ids "$SG" --no-publicly-accessible \
    --db-parameter-group-name "$PG" --backup-retention-period 1 --no-multi-az \
    --no-deletion-protection --copy-tags-to-snapshot --tags "${TAGS[@]}" >/dev/null
  echo "RDS MySQL $MYSQL_VERSION en creacion"
fi

# 3. DocumentDB 5.0 con una instancia db.t3.medium, la clase incluida en la prueba gratuita del servicio.
if ! aws docdb describe-db-clusters --db-cluster-identifier "$P-docdb" >/dev/null 2>&1; then
  aws docdb create-db-cluster --db-cluster-identifier "$P-docdb" --engine docdb --engine-version 5.0.0 \
    --master-username dw_admin --manage-master-user-password \
    --db-subnet-group-name "$P-docdb" --vpc-security-group-ids "$SG" \
    --storage-encrypted --backup-retention-period 1 --no-deletion-protection --tags "${TAGS[@]}" >/dev/null
  echo "Cluster DocumentDB en creacion"
fi
if ! aws docdb describe-db-instances --db-instance-identifier "$P-docdb-1" >/dev/null 2>&1; then
  aws docdb create-db-instance --db-instance-identifier "$P-docdb-1" --db-cluster-identifier "$P-docdb" \
    --engine docdb --db-instance-class db.t3.medium --tags "${TAGS[@]}" >/dev/null
  echo "Instancia DocumentDB en creacion"
fi

echo "Esperando a que ambas fuentes esten disponibles (10 a 20 minutos)..."
aws rds wait db-instance-available --db-instance-identifier "$P-mysql"
aws docdb wait db-instance-available --db-instance-identifier "$P-docdb-1"

# 4. Los endpoints y los ARN de los secretos van a Parameter Store. No son secretos, pero tampoco van al repo.
MYSQL_HOST=$(aws rds describe-db-instances --db-instance-identifier "$P-mysql" --query 'DBInstances[0].Endpoint.Address' --output text)
MYSQL_SECRET=$(aws rds describe-db-instances --db-instance-identifier "$P-mysql" --query 'DBInstances[0].MasterUserSecret.SecretArn' --output text)
DOCDB_HOST=$(aws docdb describe-db-clusters --db-cluster-identifier "$P-docdb" --query 'DBClusters[0].Endpoint' --output text)
DOCDB_SECRET=$(aws docdb describe-db-clusters --db-cluster-identifier "$P-docdb" --query 'DBClusters[0].MasterUserSecret.SecretArn' --output text)
for kv in "mysql_host=$MYSQL_HOST" "mysql_secret_arn=$MYSQL_SECRET" "docdb_host=$DOCDB_HOST" "docdb_secret_arn=$DOCDB_SECRET"; do
  name="/arq-datos/dw/${kv%%=*}"
  aws ssm put-parameter --name "$name" --value "${kv#*=}" --type String --overwrite >/dev/null
  aws ssm add-tags-to-resource --resource-type Parameter --resource-id "$name" --tags "${TAGS[@]}" >/dev/null
done

echo
aws rds describe-db-instances --db-instance-identifier "$P-mysql" --query 'DBInstances[0].{
  motor: Engine, version: EngineVersion, clase: DBInstanceClass, estado: DBInstanceStatus, cifrado: StorageEncrypted,
  publico: PubliclyAccessible, parametros: DBParameterGroups[0].DBParameterGroupName}' --output table
aws docdb describe-db-clusters --db-cluster-identifier "$P-docdb" --query 'DBClusters[0].{
  motor: Engine, version: EngineVersion, estado: Status, cifrado: StorageEncrypted, instancias: length(DBClusterMembers)}' --output table
echo "Reglas de entrada del security group de las fuentes"
aws ec2 describe-security-groups --group-ids "$SG" \
  --query 'SecurityGroups[0].IpPermissions[].{puerto: FromPort, origen: UserIdGroupPairs[0].GroupId, cidr: IpRanges[0].CidrIp}' --output table
echo "Parametros en /arq-datos/dw/"
aws ssm get-parameters-by-path --path /arq-datos/dw/ --query 'Parameters[].Name' --output text | tr '\t' '\n'
