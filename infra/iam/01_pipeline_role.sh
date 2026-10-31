#!/usr/bin/env bash
set -euo pipefail
: "${ACCOUNT_ID:?Corre source infra/env.sh primero}"
: "${AWS_REGION:?Corre source infra/env.sh primero}"
: "${DL_BUCKET:?Corre source infra/env.sh primero}"

ROLE=arq-datos-pipeline
ME=$(aws sts get-caller-identity --query Arn --output text)
[[ "$ME" == *":user/"* ]] || { echo "Corre esto como usuario admin, no como $ME"; exit 1; }

TRUST=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "InstanciaEC2", "Effect": "Allow",
     "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"},
    {"Sid": "DesarrolloLocal", "Effect": "Allow",
     "Principal": {"AWS": "${ME}"}, "Action": "sts:AssumeRole"}
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
