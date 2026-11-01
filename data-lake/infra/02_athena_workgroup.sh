#!/usr/bin/env bash
set -euo pipefail
: "${DL_BUCKET:?Corre source infra/env.sh primero}"

WG=arq-dl
OUT="s3://${DL_BUCKET}/athena-results/"
LIMIT=104857600

if aws athena get-work-group --work-group "$WG" >/dev/null 2>&1; then
  aws athena update-work-group --work-group "$WG" --configuration-updates \
    "ResultConfigurationUpdates={OutputLocation=${OUT},EncryptionConfiguration={EncryptionOption=SSE_S3}},EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true,BytesScannedCutoffPerQuery=${LIMIT}"
  echo "Workgroup actualizado: $WG"
else
  aws athena create-work-group --name "$WG" --description "Data lake de noticias RSS (EIA)" \
    --configuration "ResultConfiguration={OutputLocation=${OUT},EncryptionConfiguration={EncryptionOption=SSE_S3}},EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true,BytesScannedCutoffPerQuery=${LIMIT}" \
    --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=dl
  echo "Workgroup creado: $WG"
fi

aws athena get-work-group --work-group "$WG" \
  --query 'WorkGroup.Configuration.{salida:ResultConfiguration.OutputLocation,cifrado:ResultConfiguration.EncryptionConfiguration.EncryptionOption,forzado:EnforceWorkGroupConfiguration,limite_bytes:BytesScannedCutoffPerQuery}'
