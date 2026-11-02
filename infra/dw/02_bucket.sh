#!/usr/bin/env bash
# Bucket privado del data warehouse: raw por fuente, entidad y lote, mas el control de extracciones.
# Mismos controles que el bucket del data lake. Idempotente.
set -euo pipefail
: "${DW_BUCKET:?Corre arq primero}"
: "${AWS_REGION:?Corre arq primero}"

if aws s3api head-bucket --bucket "$DW_BUCKET" 2>/dev/null; then
  echo "Bucket ya existe: $DW_BUCKET"
else
  aws s3api create-bucket --bucket "$DW_BUCKET" --region "$AWS_REGION" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null
  echo "Bucket creado: $DW_BUCKET"
fi
aws s3api put-public-access-block --bucket "$DW_BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-versioning --bucket "$DW_BUCKET" --versioning-configuration Status=Enabled
aws s3api put-bucket-tagging --bucket "$DW_BUCKET" \
  --tagging 'TagSet=[{Key=Project,Value=arquitecturas-datos},{Key=Component,Value=dw}]'
aws s3api put-bucket-policy --bucket "$DW_BUCKET" --policy "$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
    "Resource": ["arn:aws:s3:::${DW_BUCKET}", "arn:aws:s3:::${DW_BUCKET}/*"],
    "Condition": {"Bool": {"aws:SecureTransport": "false"}}
  }]
}
JSON
)"
aws s3api put-bucket-lifecycle-configuration --bucket "$DW_BUCKET" --lifecycle-configuration '{
  "Rules": [{"ID": "versiones-viejas-7d", "Status": "Enabled", "Filter": {},
             "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
             "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]}'
echo "Controles aplicados: bloqueo publico, versionado, solo TLS y ciclo de vida"
