#!/usr/bin/env bash
# Permisos del rol de pipeline para el data warehouse. Se amplia en cada fase del DW.
# Fase 3: leer los parametros de conexion y los dos secretos de las fuentes, y nada mas.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero}"
: "${AWS_REGION:?Corre arq primero}"

ROLE=arq-datos-pipeline
MYSQL_SECRET=$(aws ssm get-parameter --name /arq-datos/dw/mysql_secret_arn --query Parameter.Value --output text)
DOCDB_SECRET=$(aws ssm get-parameter --name /arq-datos/dw/docdb_secret_arn --query Parameter.Value --output text)
PARAMS="arn:aws:ssm:${AWS_REGION}:${ACCOUNT_ID}:parameter/arq-datos/dw"

POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "ParametrosDw", "Effect": "Allow",
     "Action": ["ssm:GetParametersByPath", "ssm:GetParameter", "ssm:GetParameters"],
     "Resource": ["${PARAMS}", "${PARAMS}/*"]},
    {"Sid": "SecretosFuentesDw", "Effect": "Allow",
     "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
     "Resource": ["${MYSQL_SECRET}", "${DOCDB_SECRET}"]}
  ]
}
JSON
)
aws iam put-role-policy --role-name "$ROLE" --policy-name dw-access --policy-document "$POLICY"
echo "Politica dw-access aplicada a $ROLE"
aws iam get-role-policy --role-name "$ROLE" --policy-name dw-access \
  --query 'PolicyDocument.Statement[].[Sid, join(`", "`, Action)]' --output table
