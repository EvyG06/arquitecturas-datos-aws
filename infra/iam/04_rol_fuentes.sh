#!/usr/bin/env bash
# Rol que simula el sistema operativo de origen: genera datos, aplica cambios y administra usuarios en las
# fuentes con las credenciales maestras. El pipeline de extraccion NO puede asumirlo ni leer esas credenciales.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero}"
: "${AWS_REGION:?Corre arq primero}"

ROLE=arq-datos-fuentes
ME=$(aws sts get-caller-identity --query Arn --output text)
[[ "$ME" == *":user/"* ]] || { echo "Corre esto como usuario admin, no como $ME"; exit 1; }
EC2_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/arq-datos-ec2"

TRUST=$(cat <<JSON
{"Version": "2012-10-17", "Statement": [
  {"Sid": "DesarrolloLocal", "Effect": "Allow", "Principal": {"AWS": "${ME}"}, "Action": "sts:AssumeRole"},
  {"Sid": "InstanciaAirflow", "Effect": "Allow", "Principal": {"AWS": "${EC2_ROLE_ARN}"}, "Action": "sts:AssumeRole"}]}
JSON
)
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "$ROLE" --policy-document "$TRUST"
else
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document "$TRUST" \
    --description "Sistema de origen simulado del DW, credenciales maestras de las fuentes" \
    --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=dw >/dev/null
  echo "Rol creado: $ROLE"
fi

param() { aws ssm get-parameter --name "/arq-datos/dw/$1" --query Parameter.Value --output text; }
PARAMS="arn:aws:ssm:${AWS_REGION}:${ACCOUNT_ID}:parameter/arq-datos/dw"
POLICY=$(cat <<JSON
{"Version": "2012-10-17", "Statement": [
  {"Sid": "ParametrosDw", "Effect": "Allow",
   "Action": ["ssm:GetParametersByPath", "ssm:GetParameter", "ssm:GetParameters"],
   "Resource": ["${PARAMS}", "${PARAMS}/*"]},
  {"Sid": "SecretosMaestrosYLectores", "Effect": "Allow",
   "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
   "Resource": ["$(param mysql_secret_arn)", "$(param docdb_secret_arn)",
                "$(param mysql_reader_secret_arn)", "$(param docdb_reader_secret_arn)"]}]}
JSON
)
aws iam put-role-policy --role-name "$ROLE" --policy-name fuentes-access --policy-document "$POLICY"
echo "Politica fuentes-access aplicada a $ROLE"
echo "Ahora corre ./infra/iam/02_ec2_role.sh para que la instancia pueda asumir este rol"
