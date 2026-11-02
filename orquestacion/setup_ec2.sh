#!/usr/bin/env bash
# Prepara y levanta Airflow en la EC2. Idempotente. Se corre como ec2-user dentro de la instancia.
set -euo pipefail
cd "$(dirname "$0")"
REGION="${AWS_REGION:-us-east-2}"

# La cuenta se lee del documento de identidad de la instancia con IMDSv2. No hay credenciales en disco.
TOKEN=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
ACCOUNT_ID=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/dynamic/instance-identity/document |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["accountId"])')

if [ ! -f .env ]; then
  (
    umask 077
    cat > .env <<ENV
AIRFLOW_UID=50000
AWS_REGION=${REGION}
DL_BUCKET=eve-dl-${ACCOUNT_ID}-${REGION}
POSTGRES_PASSWORD=$(openssl rand -hex 24)
AIRFLOW_FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')
AIRFLOW_SECRET_KEY=$(openssl rand -hex 32)
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 18 | tr -d '/+=')
ENV
  )
  echo ".env creado con secretos aleatorios y permisos 600"
else
  echo ".env ya existe, se conservan los secretos"
fi

# Los contenedores asumen arq-datos-pipeline encadenando desde el rol de la instancia.
mkdir -p aws
cat > aws/config <<CFG
[profile pipeline]
role_arn = arn:aws:iam::${ACCOUNT_ID}:role/arq-datos-pipeline
credential_source = Ec2InstanceMetadata
role_session_name = airflow-ec2
region = ${REGION}
CFG
chmod 755 aws
chmod 644 aws/config

docker compose build
docker compose up -d
docker compose ps
echo
echo "Interfaz web: usuario admin, contrasena en orquestacion/.env (AIRFLOW_ADMIN_PASSWORD)"
