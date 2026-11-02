#!/usr/bin/env bash
# QuickSight Enterprise conectado al Redshift privado por una conexion VPC. Idempotente. Corre en el Mac (admin).
#   1. Suscripcion Enterprise por CLI: solo un autor normal, sin usuarios Pro ni Q&A, que son los que activan la
#      tarifa de infraestructura. La prueba gratuita es de 30 dias; al entregar, se cancela con infra/dw/90_quicksight_baja.sh.
#   2. La identidad IAM que corre esto queda registrada como autor ADMIN.
#   3. Red: QuickSight y Redshift solo se hablan por el 5439, entre sus security groups.
#   4. Rol que QuickSight usa para crear sus interfaces de red en la VPC, y la conexion VPC.
#   5. Permiso del rol de pipeline para refrescar el dataset SPICE al final de cada lote.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero}"
: "${AWS_REGION:?Corre arq primero}"

P=arq-datos
QS_ACCOUNT_NAME=${QS_ACCOUNT_NAME:-eve-arq-datos-$ACCOUNT_ID}
VPC_CONN_ID=$P-dw-vpc
DATASET_ID=$P-ventas
ME_ARN=$(aws sts get-caller-identity --query Arn --output text)
ME_NAME=${ME_ARN##*/}
QS_EMAIL=${QS_EMAIL:-}
correo() {
  if [ -z "$QS_EMAIL" ]; then read -r -p "Correo para QuickSight (notificaciones y tu usuario autor): " QS_EMAIL </dev/tty; fi
}
qs() { aws quicksight "$@" --aws-account-id "$ACCOUNT_ID"; }

# 1. Suscripcion
if ! qs describe-account-subscription >/dev/null 2>&1; then
  correo
  qs create-account-subscription --edition ENTERPRISE --authentication-method IAM_AND_QUICKSIGHT \
    --account-name "$QS_ACCOUNT_NAME" --notification-email "$QS_EMAIL" >/dev/null
  echo "Suscripcion Enterprise solicitada: $QS_ACCOUNT_NAME"
fi
for i in $(seq 1 60); do
  ESTADO=$(qs describe-account-subscription --query AccountInfo.AccountSubscriptionStatus --output text)
  [ "$ESTADO" = ACCOUNT_CREATED ] && break
  [ $((i % 6)) = 1 ] && echo "$(date +%H:%M:%S) suscripcion $ESTADO"
  sleep 10
done
[ "$ESTADO" = ACCOUNT_CREATED ] || { echo "La suscripcion no quedo creada: $ESTADO"; exit 1; }
qs describe-account-subscription --query 'AccountInfo.{cuenta:AccountName,edicion:Edition,autenticacion:AuthenticationType,estado:AccountSubscriptionStatus}' --output table

# 2. Autor ADMIN para la identidad IAM que corre el script
if ! qs describe-user --namespace default --user-name "$ME_NAME" >/dev/null 2>&1; then
  correo
  qs register-user --namespace default --identity-type IAM --iam-arn "$ME_ARN" --user-role ADMIN --email "$QS_EMAIL" >/dev/null
  echo "Usuario autor registrado: $ME_NAME"
fi
QS_USER_ARN=$(qs describe-user --namespace default --user-name "$ME_NAME" --query User.Arn --output text)

# 3. Red. QuickSight no es stateful con sus interfaces: necesita la entrada de vuelta desde Redshift.
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
RS_SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=$P-dw-redshift Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
QS_SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=$P-quicksight Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ "$QS_SG" = "None" ]; then
  QS_SG=$(aws ec2 create-security-group --group-name $P-quicksight --vpc-id "$VPC" \
    --description "Interfaces de QuickSight hacia Redshift" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$P-quicksight},{Key=Project,Value=arquitecturas-datos},{Key=Component,Value=dw}]" \
    --query GroupId --output text)
  # Sin la salida por defecto a todo internet: solo el 5439 hacia Redshift.
  aws ec2 revoke-security-group-egress --group-id "$QS_SG" \
    --ip-permissions '[{"IpProtocol":"-1","IpRanges":[{"CidrIp":"0.0.0.0/0"}]}]' >/dev/null
  aws ec2 authorize-security-group-egress --group-id "$QS_SG" \
    --ip-permissions "[{\"IpProtocol\":\"tcp\",\"FromPort\":5439,\"ToPort\":5439,\"UserIdGroupPairs\":[{\"GroupId\":\"$RS_SG\"}]}]" >/dev/null
  aws ec2 authorize-security-group-ingress --group-id "$QS_SG" --protocol tcp --port 0-65535 --source-group "$RS_SG" >/dev/null
  aws ec2 authorize-security-group-ingress --group-id "$RS_SG" --protocol tcp --port 5439 --source-group "$QS_SG" >/dev/null
  echo "Security group creado: $QS_SG"
fi

# 4. Rol de la conexion VPC y la conexion
ROLE=$P-quicksight-vpc
if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam create-role --role-name "$ROLE" --description "QuickSight crea sus interfaces de red en la VPC del DW" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"quicksight.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=dw >/dev/null
  echo "Rol creado: $ROLE"
fi
aws iam put-role-policy --role-name "$ROLE" --policy-name interfaces-vpc --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ec2:CreateNetworkInterface","ec2:ModifyNetworkInterfaceAttribute","ec2:DeleteNetworkInterface","ec2:DescribeSubnets","ec2:DescribeSecurityGroups"],"Resource":"*"}]}'
read -r -a SUBNETS <<< "$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true \
  --query 'Subnets[].SubnetId' --output text)"
if ! qs describe-vpc-connection --vpc-connection-id "$VPC_CONN_ID" >/dev/null 2>&1; then
  for intento in 1 2 3 4 5 6; do  # un rol recien creado tarda unos segundos en poder asumirse
    if qs create-vpc-connection --vpc-connection-id "$VPC_CONN_ID" --name "DW Redshift privado" \
        --subnet-ids "${SUBNETS[@]}" --security-group-ids "$QS_SG" --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/$ROLE" \
        --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=dw >/dev/null; then break; fi
    [ "$intento" -lt 6 ] || exit 1
    sleep 10
  done
fi
for i in $(seq 1 60); do
  read -r ESTADO DISP <<< "$(qs describe-vpc-connection --vpc-connection-id "$VPC_CONN_ID" \
    --query 'VPCConnection.[Status, AvailabilityStatus]' --output text)"
  [ "$DISP" = AVAILABLE ] && break
  case "$ESTADO" in *FAILED) echo "La conexion VPC fallo: $ESTADO"; exit 1 ;; esac
  [ $((i % 6)) = 1 ] && echo "$(date +%H:%M:%S) conexion VPC $ESTADO $DISP"
  sleep 10
done
[ "$DISP" = AVAILABLE ] || { echo "La conexion VPC no quedo disponible"; exit 1; }
VPC_CONN_ARN=$(qs describe-vpc-connection --vpc-connection-id "$VPC_CONN_ID" --query VPCConnection.Arn --output text)

# 5. El pipeline solo puede crear y consultar refrescos de este dataset.
DS_ARN="arn:aws:quicksight:${AWS_REGION}:${ACCOUNT_ID}:dataset/${DATASET_ID}"
aws iam put-role-policy --role-name arq-datos-pipeline --policy-name dw-quicksight --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"RefrescarSpice\",\"Effect\":\"Allow\",\"Action\":[\"quicksight:CreateIngestion\",\"quicksight:DescribeIngestion\"],\"Resource\":[\"${DS_ARN}\",\"${DS_ARN}/ingestion/*\"]}]}"

put_param() {
  aws ssm put-parameter --name "/arq-datos/dw/$1" --value "$2" --type String --overwrite >/dev/null
  aws ssm add-tags-to-resource --resource-type Parameter --resource-id "/arq-datos/dw/$1" \
    --tags Key=Project,Value=arquitecturas-datos Key=Component,Value=dw
}
put_param quicksight_user_arn "$QS_USER_ARN"
put_param quicksight_vpc_connection_arn "$VPC_CONN_ARN"
echo
echo "Autor: $QS_USER_ARN"
echo "Conexion VPC: $VPC_CONN_ARN ($DISP)"
echo "Security groups: QuickSight $QS_SG, Redshift $RS_SG"
