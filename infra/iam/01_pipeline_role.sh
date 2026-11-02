#!/usr/bin/env bash
set -euo pipefail
: "${ACCOUNT_ID:?Corre source infra/env.sh primero}"
: "${AWS_REGION:?Corre source infra/env.sh primero}"
: "${DL_BUCKET:?Corre source infra/env.sh primero}"

ROLE=arq-datos-pipeline
ME=$(aws sts get-caller-identity --query Arn --output text)
[[ "$ME" == *":user/"* ]] || { echo "Corre esto como usuario admin, no como $ME"; exit 1; }

# Confia en tu usuario (desarrollo local) y, si ya existe, en el rol de la instancia de Airflow.
# No confia en el servicio EC2 en general: ninguna otra instancia de la cuenta puede asumirlo.
EC2_ROLE=arq-datos-ec2
EC2_STMT=""
if aws iam get-role --role-name "$EC2_ROLE" >/dev/null 2>&1; then
  EC2_STMT=",{\"Sid\": \"InstanciaAirflow\", \"Effect\": \"Allow\", \"Principal\": {\"AWS\": \"arn:aws:iam::${ACCOUNT_ID}:role/${EC2_ROLE}\"}, \"Action\": \"sts:AssumeRole\"}"
fi
TRUST=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "DesarrolloLocal", "Effect": "Allow",
     "Principal": {"AWS": "${ME}"}, "Action": "sts:AssumeRole"}${EC2_STMT}
  ]
}
JSON
)

if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "$ROLE" --policy-document "$TRUST"
else
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document "$TRUST" \
    --description "Pipeline de los proyectos DL y DW" \
    --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=shared >/dev/null
fi

B="arn:aws:s3:::${DL_BUCKET}"
G="arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}"
POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "DlBucket", "Effect": "Allow",
     "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
     "Resource": "${B}"},
    {"Sid": "DlLeerEscribir", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
     "Resource": "${B}/*"},
    {"Sid": "DlBorrarExceptoBronze", "Effect": "Allow",
     "Action": "s3:DeleteObject",
     "Resource": ["${B}/silver/*", "${B}/gold/*", "${B}/quarantine/*", "${B}/_control/*", "${B}/athena-results/*"]},
    {"Sid": "Comprehend", "Effect": "Allow",
     "Action": ["comprehend:DetectDominantLanguage", "comprehend:DetectEntities", "comprehend:DetectKeyPhrases",
                "comprehend:BatchDetectDominantLanguage", "comprehend:BatchDetectEntities", "comprehend:BatchDetectKeyPhrases"],
     "Resource": "*"},
    {"Sid": "GlueCatalogoDl", "Effect": "Allow",
     "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables",
                "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
                "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition",
                "glue:DeletePartition", "glue:BatchDeletePartition"],
     "Resource": ["${G}:catalog", "${G}:database/arq_dl", "${G}:table/arq_dl/*"]},
    {"Sid": "AthenaWorkgroupDl", "Effect": "Allow",
     "Action": ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
                "athena:StopQueryExecution", "athena:GetWorkGroup"],
     "Resource": "arn:aws:athena:${AWS_REGION}:${ACCOUNT_ID}:workgroup/arq-dl"}
  ]
}
JSON
)

aws iam put-role-policy --role-name "$ROLE" --policy-name dl-access --policy-document "$POLICY"
echo "Rol listo: arn:aws:iam::${ACCOUNT_ID}:role/${ROLE}"
