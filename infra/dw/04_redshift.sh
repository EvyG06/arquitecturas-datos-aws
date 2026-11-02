#!/usr/bin/env bash
# Data warehouse en Redshift Serverless. Privado (solo Airflow llega al 5439), cifrado, TLS obligatorio,
# capacidad fija de 4 RPU y un limite diario de RPU-hora que desactiva el workgroup si se supera.
# La contrasena del admin la administra Secrets Manager: no pasa por la terminal. Idempotente.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero}"
: "${AWS_REGION:?Corre arq primero}"
: "${DW_BUCKET:?Corre arq primero}"
cd "$(dirname "$0")/../.."

P=arq-datos-dw
DB=ecommerce_dw
COPY_ROLE=arq-datos-redshift-s3
COPY_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${COPY_ROLE}"
LIMITE_RPU_HORAS_DIA=${LIMITE_RPU_HORAS_DIA:-10}   # 4 RPU durante 2,5 horas de consultas por dia
RS_TAGS=(key=Project,value=arquitecturas-datos key=Component,value=dw)
B="arn:aws:s3:::${DW_BUCKET}"

# 1. Rol que Redshift asume durante COPY. Solo lee los Parquet de raw y los manifiestos, nada mas del bucket.
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["redshift.amazonaws.com","redshift-serverless.amazonaws.com"]},"Action":"sts:AssumeRole"}]}'
if aws iam get-role --role-name "$COPY_ROLE" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "$COPY_ROLE" --policy-document "$TRUST"
else
  aws iam create-role --role-name "$COPY_ROLE" --assume-role-policy-document "$TRUST" \
    --description "Redshift Serverless lee los lotes del DW en S3 para COPY" \
    --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=dw >/dev/null
  echo "Rol creado: $COPY_ROLE"
fi
aws iam put-role-policy --role-name "$COPY_ROLE" --policy-name leer-lotes-dw --policy-document "$(cat <<JSON
{"Version": "2012-10-17", "Statement": [
  {"Sid": "LeerParquetYManifiestos", "Effect": "Allow", "Action": "s3:GetObject",
   "Resource": ["${B}/raw/*", "${B}/_control/copy/*"]},
  {"Sid": "ListarSoloEsosPrefijos", "Effect": "Allow", "Action": "s3:ListBucket", "Resource": "${B}",
   "Condition": {"StringLike": {"s3:prefix": ["raw/*", "_control/copy/*"]}}},
  {"Sid": "Region", "Effect": "Allow", "Action": "s3:GetBucketLocation", "Resource": "${B}"}]}
JSON
)"

# 2. Red: solo el security group de Airflow puede abrir conexiones al 5439.
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
AIRFLOW_SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=arq-datos-airflow Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
[ "$AIRFLOW_SG" != "None" ] || { echo "Falta el security group de Airflow"; exit 1; }
read -r -a SUBNETS <<< "$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true \
  --query 'Subnets[].SubnetId' --output text)"
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$P-redshift" Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws ec2 create-security-group --group-name "$P-redshift" --vpc-id "$VPC" \
    --description "Redshift Serverless del DW, solo desde Airflow" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$P-redshift},{Key=Project,Value=arquitecturas-datos},{Key=Component,Value=dw}]" \
    --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 5439 --source-group "$AIRFLOW_SG" >/dev/null
  echo "Security group creado: $SG"
fi

# 3. Namespace: la base de datos, el admin con contrasena administrada y el rol de COPY.
if ! aws redshift-serverless get-namespace --namespace-name "$P" >/dev/null 2>&1; then
  for intento in 1 2 3 4 5 6; do
    # Un rol recien creado tarda unos segundos en ser valido para otros servicios.
    if aws redshift-serverless create-namespace --namespace-name "$P" --db-name "$DB" --admin-username dw_admin \
        --manage-admin-password --iam-roles "$COPY_ROLE_ARN" --default-iam-role-arn "$COPY_ROLE_ARN" \
        --log-exports connectionlog userlog --tags "${RS_TAGS[@]}" >/dev/null; then
      echo "Namespace creado: $P"; break
    fi
    [ "$intento" -lt 6 ] || exit 1
    echo "Reintento en 10 s ($intento de 6)"; sleep 10
  done
fi
until [ "$(aws redshift-serverless get-namespace --namespace-name "$P" --query namespace.status --output text)" = AVAILABLE ]; do sleep 10; done

# 4. Workgroup: el computo. 4 RPU de base y de maximo, asi no escala por su cuenta.
crear_workgroup() {
  aws redshift-serverless create-workgroup --workgroup-name "$P" --namespace-name "$P" \
    --base-capacity 4 --max-capacity "$1" --subnet-ids "${SUBNETS[@]}" --security-group-ids "$SG" \
    --no-publicly-accessible \
    --config-parameters parameterKey=require_ssl,parameterValue=true parameterKey=max_query_execution_time,parameterValue=1800 \
    --tags "${RS_TAGS[@]}" >/dev/null
}
if ! aws redshift-serverless get-workgroup --workgroup-name "$P" >/dev/null 2>&1; then
  if ! SALIDA=$(crear_workgroup 4 2>&1); then
    case "$SALIDA" in
      *[Mm]ax*[Cc]apacity*) echo "Max capacity de 4 no aceptado, se usa 8: $SALIDA"; crear_workgroup 8 ;;
      *) echo "$SALIDA"; exit 1 ;;
    esac
  fi
  echo "Workgroup creado: $P (tarda unos minutos en quedar disponible)"
fi
for i in $(seq 1 90); do
  ESTADO=$(aws redshift-serverless get-workgroup --workgroup-name "$P" --query workgroup.status --output text)
  [ "$ESTADO" = AVAILABLE ] && break
  [ $((i % 6)) = 1 ] && echo "$(date +%H:%M:%S) workgroup $ESTADO"
  sleep 10
done
[ "$ESTADO" = AVAILABLE ] || { echo "El workgroup no quedo disponible en 15 minutos"; exit 1; }

# 5. Tope de costo: si el dia supera el limite de RPU-hora, Redshift desactiva el workgroup hasta el dia siguiente.
WG_ARN=$(aws redshift-serverless get-workgroup --workgroup-name "$P" --query workgroup.workgroupArn --output text)
# list-usage-limits no acepta --resource-arn y --usage-type juntos: se filtra el tipo en la consulta. Va fuera
# del if para que un error de la API corte el script (set -e no corta dentro de una condicion).
LIMITES=$(aws redshift-serverless list-usage-limits --resource-arn "$WG_ARN" \
  --query 'length(usageLimits[?usageType==`serverless-compute`])' --output text)
if [ "$LIMITES" = 0 ]; then
  aws redshift-serverless create-usage-limit --resource-arn "$WG_ARN" --usage-type serverless-compute \
    --amount "$LIMITE_RPU_HORAS_DIA" --period daily --breach-action deactivate >/dev/null
  echo "Limite de uso creado: $LIMITE_RPU_HORAS_DIA RPU-hora por dia, al superarlo se desactiva"
fi

# 6. Parametros para el pipeline: endpoint, workgroup, base, rol de COPY y el ARN del secreto del admin.
put_param() {
  aws ssm put-parameter --name "/arq-datos/dw/$1" --value "$2" --type String --overwrite >/dev/null
  aws ssm add-tags-to-resource --resource-type Parameter --resource-id "/arq-datos/dw/$1" \
    --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=dw
}
put_param redshift_host "$(aws redshift-serverless get-workgroup --workgroup-name "$P" --query workgroup.endpoint.address --output text)"
put_param redshift_workgroup "$P"
put_param redshift_database "$DB"
put_param redshift_copy_role_arn "$COPY_ROLE_ARN"
put_param redshift_admin_secret_arn "$(aws redshift-serverless get-namespace --namespace-name "$P" --query namespace.adminPasswordSecretArn --output text)"

echo
aws redshift-serverless get-workgroup --workgroup-name "$P" --query \
  'workgroup.{workgroup:workgroupName,estado:status,base_rpu:baseCapacity,max_rpu:maxCapacity,publico:publiclyAccessible,endpoint:endpoint.address}' \
  --output table
aws redshift-serverless list-usage-limits --resource-arn "$WG_ARN" \
  --query 'usageLimits[].{tipo:usageType,rpu_horas:amount,periodo:period,accion:breachAction}' --output table
