#!/usr/bin/env bash
set -euo pipefail
: "${DL_BUCKET:?Corre source infra/env.sh primero}"
: "${AWS_REGION:?Corre source infra/env.sh primero}"

if aws s3api head-bucket --bucket "$DL_BUCKET" 2>/dev/null; then
  echo "Bucket ya existe: $DL_BUCKET"
else
  aws s3api create-bucket --bucket "$DL_BUCKET" --region "$AWS_REGION" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION"
fi

aws s3api put-public-access-block --bucket "$DL_BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-versioning --bucket "$DL_BUCKET" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-tagging --bucket "$DL_BUCKET" \
  --tagging 'TagSet=[{Key=Project,Value=arquitecturas-datos},{Key=Component,Value=dl}]'

aws s3api put-bucket-policy --bucket "$DL_BUCKET" --policy "$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "DenyInsecureTransport",
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:*",
    "Resource": ["arn:aws:s3:::${DL_BUCKET}", "arn:aws:s3:::${DL_BUCKET}/*"],
    "Condition": {"Bool": {"aws:SecureTransport": "false"}}
  }]
}
JSON
)"

aws s3api put-bucket-lifecycle-configuration --bucket "$DL_BUCKET" \
  --lifecycle-configuration '{
    "Rules": [
      {"ID": "versiones-viejas-7d", "Status": "Enabled", "Filter": {},
       "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
       "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}},
      {"ID": "athena-results-7d", "Status": "Enabled", "Filter": {"Prefix": "athena-results/"},
       "Expiration": {"Days": 7}}
    ]
  }'

echo "Listo: $DL_BUCKET"
