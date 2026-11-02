#!/usr/bin/env bash
# Rol de la instancia de Airflow. Solo puede registrarse en Session Manager y asumir el rol de pipeline.
# Los permisos sobre los datos viven en arq-datos-pipeline, igual que cuando corres desde el Mac.
set -euo pipefail
: "${ACCOUNT_ID:?Corre source infra/env.sh primero}"
cd "$(dirname "$0")/../.."

ROLE=arq-datos-ec2
PIPELINE_ROLE=arq-datos-pipeline
TAGS=(Key=Project,Value=arquitecturas-datos Key=Component,Value=shared)
ME=$(aws sts get-caller-identity --query Arn --output text)
[[ "$ME" == *":user/"* ]] || { echo "Corre esto como usuario admin, no como $ME"; exit 1; }

TRUST='{"Version":"2012-10-17","Statement":[{"Sid":"ServicioEC2","Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  echo "Rol ya existe: $ROLE"
else
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document "$TRUST" \
    --description "Instancia de Airflow, solo SSM y asumir el rol de pipeline" --tags "${TAGS[@]}" >/dev/null
  echo "Rol creado: $ROLE"
fi

aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
# La instancia solo puede asumir los dos roles de trabajo: el pipeline y el sistema de origen simulado.
aws iam put-role-policy --role-name "$ROLE" --policy-name asumir-roles --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"AsumirRolesDeTrabajo\",\"Effect\":\"Allow\",\"Action\":\"sts:AssumeRole\",\"Resource\":[\"arn:aws:iam::${ACCOUNT_ID}:role/${PIPELINE_ROLE}\",\"arn:aws:iam::${ACCOUNT_ID}:role/arq-datos-fuentes\"]}]}"
aws iam delete-role-policy --role-name "$ROLE" --policy-name asumir-pipeline 2>/dev/null || true

if aws iam get-instance-profile --instance-profile-name "$ROLE" >/dev/null 2>&1; then
  echo "Perfil de instancia ya existe: $ROLE"
else
  aws iam create-instance-profile --instance-profile-name "$ROLE" --tags "${TAGS[@]}" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "$ROLE" --role-name "$ROLE"
  echo "Perfil de instancia creado: $ROLE"
fi

# IAM tarda unos segundos en reconocer un rol nuevo como principal valido de otra politica.
APLICADA=0
for intento in 1 2 3 4 5 6; do
  if ./infra/iam/01_pipeline_role.sh; then
    APLICADA=1
    break
  fi
  echo "IAM aun no reconoce $ROLE como principal, reintento en 10 s ($intento de 6)"
  sleep 10
done
[ "$APLICADA" = 1 ] || { echo "No se pudo actualizar la confianza de $PIPELINE_ROLE"; exit 1; }

echo
echo "Confianza de $PIPELINE_ROLE"
aws iam get-role --role-name "$PIPELINE_ROLE" \
  --query 'Role.AssumeRolePolicyDocument.Statement[].[Sid, Principal.AWS || Principal.Service]' --output table
echo "Permisos de $ROLE"
aws iam list-attached-role-policies --role-name "$ROLE" --query 'AttachedPolicies[].PolicyName' --output text
aws iam list-role-policies --role-name "$ROLE" --query PolicyNames --output text
