#!/usr/bin/env bash
# Instancia EC2 para Airflow. Sin puertos de entrada: se administra solo por Session Manager.
set -euo pipefail
: "${AWS_REGION:?Corre source infra/env.sh primero}"
cd "$(dirname "$0")/../.."

NAME=arq-datos-airflow
TYPE="${INSTANCE_TYPE:-t3.medium}"
TAGS="{Key=Project,Value=arquitecturas-datos},{Key=Component,Value=shared}"

VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
[ "$VPC" != "None" ] || { echo "No hay VPC por defecto en $AWS_REGION"; exit 1; }

SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$NAME" Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws ec2 create-security-group --group-name "$NAME" --vpc-id "$VPC" \
    --description "Airflow sin entradas, acceso solo por Session Manager" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$NAME},$TAGS]" \
    --query GroupId --output text)
  echo "Security group creado: $SG"
fi

IID=$(aws ec2 describe-instances \
  --filters Name=tag:Name,Values="$NAME" Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId | [0]' --output text)
if [ "$IID" = "None" ]; then
  IID=$(aws ec2 run-instances \
    --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
    --instance-type "$TYPE" \
    --iam-instance-profile Name=arq-datos-ec2 \
    --security-group-ids "$SG" \
    --metadata-options HttpEndpoint=enabled,HttpTokens=required,HttpPutResponseHopLimit=2 \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":30,"VolumeType":"gp3","Encrypted":true,"DeleteOnTermination":true}}]' \
    --user-data file://infra/ec2/user_data.sh \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},$TAGS]" \
                         "ResourceType=volume,Tags=[{Key=Name,Value=$NAME},$TAGS]" \
    --query 'Instances[0].InstanceId' --output text)
  echo "Instancia creada: $IID"
else
  echo "Instancia ya existe: $IID"
fi

aws ec2 wait instance-running --instance-ids "$IID"
aws ec2 describe-instances --instance-ids "$IID" --query 'Reservations[0].Instances[0].{
  id: InstanceId, tipo: InstanceType, estado: State.Name, zona: Placement.AvailabilityZone,
  perfil: IamInstanceProfile.Arn, imdsv2: MetadataOptions.HttpTokens, saltos: MetadataOptions.HttpPutResponseHopLimit}' \
  --output table
echo "Reglas de entrada del security group $SG (debe ser una lista vacia)"
aws ec2 describe-security-groups --group-ids "$SG" --query 'SecurityGroups[0].IpPermissions' --output json
echo
echo "Agrega esta linea a tu sesion:  export AIRFLOW_INSTANCE=$IID"
