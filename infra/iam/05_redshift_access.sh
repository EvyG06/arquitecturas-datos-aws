#!/usr/bin/env bash
# Permisos del rol de pipeline sobre Redshift Serverless. Sin contrasenas: dbt y la carga piden credenciales
# temporales con GetCredentials y Redshift ve al rol como el usuario IAMR:arq-datos-pipeline.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero}"
: "${AWS_REGION:?Corre arq primero}"

ROLE=arq-datos-pipeline
WG_ARN=$(aws redshift-serverless get-workgroup --workgroup-name arq-datos-dw --query workgroup.workgroupArn --output text)
POLICY=$(cat <<JSON
{"Version": "2012-10-17", "Statement": [
  {"Sid": "CredencialesTemporales", "Effect": "Allow",
   "Action": ["redshift-serverless:GetCredentials", "redshift-serverless:GetWorkgroup"], "Resource": "${WG_ARN}"},
  {"Sid": "DataApiEjecutar", "Effect": "Allow",
   "Action": ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement"], "Resource": "${WG_ARN}"},
  {"Sid": "DataApiSoloSusSentencias", "Effect": "Allow",
   "Action": ["redshift-data:DescribeStatement", "redshift-data:GetStatementResult",
              "redshift-data:CancelStatement", "redshift-data:ListStatements"],
   "Resource": "*", "Condition": {"StringEquals": {"redshift-data:statement-owner-iam-userid": "\${aws:userid}"}}}]}
JSON
)
aws iam put-role-policy --role-name "$ROLE" --policy-name dw-redshift --policy-document "$POLICY"
echo "Politica dw-redshift aplicada a $ROLE"
aws iam get-role-policy --role-name "$ROLE" --policy-name dw-redshift \
  --query 'PolicyDocument.Statement[].[Sid, join(`", "`, to_array(Action))]' --output table
