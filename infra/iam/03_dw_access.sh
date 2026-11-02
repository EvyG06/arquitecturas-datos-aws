#!/usr/bin/env bash
# Permisos del rol de pipeline para el data warehouse. El pipeline solo lee las fuentes con usuarios de
# solo lectura: no tiene acceso a las credenciales maestras, que son del rol arq-datos-fuentes.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero}"
: "${AWS_REGION:?Corre arq primero}"
: "${DW_BUCKET:?Corre arq primero}"

ROLE=arq-datos-pipeline
param() { aws ssm get-parameter --name "/arq-datos/dw/$1" --query Parameter.Value --output text; }
PARAMS="arn:aws:ssm:${AWS_REGION}:${ACCOUNT_ID}:parameter/arq-datos/dw"
B="arn:aws:s3:::${DW_BUCKET}"

POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "ParametrosDw", "Effect": "Allow",
     "Action": ["ssm:GetParametersByPath", "ssm:GetParameter", "ssm:GetParameters"],
     "Resource": ["${PARAMS}", "${PARAMS}/*"]},
    {"Sid": "SecretosLectores", "Effect": "Allow",
     "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
     "Resource": ["$(param mysql_reader_secret_arn)", "$(param docdb_reader_secret_arn)"]},
    {"Sid": "DwBucket", "Effect": "Allow",
     "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"], "Resource": "${B}"},
    {"Sid": "DwLeerEscribir", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
     "Resource": "${B}/*"},
    {"Sid": "DwBorrarSoloControl", "Effect": "Allow", "Action": "s3:DeleteObject", "Resource": "${B}/_control/*"}
  ]
}
JSON
)
aws iam put-role-policy --role-name "$ROLE" --policy-name dw-access --policy-document "$POLICY"
echo "Politica dw-access aplicada a $ROLE"
aws iam get-role-policy --role-name "$ROLE" --policy-name dw-access \
  --query 'PolicyDocument.Statement[].[Sid, join(`", "`, to_array(Action))]' --output table
