#!/usr/bin/env bash
# Baja de QuickSight al terminar la entrega: borra dashboard, analisis, dataset, fuente y conexion VPC, y cancela
# la suscripcion para que no cobre al vencer la prueba de 30 dias. Cancelar la suscripcion es IRREVERSIBLE:
# borra todos los recursos de QuickSight de la cuenta. Pide confirmacion escrita antes de hacerlo.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero}"
qs() { aws quicksight "$@" --aws-account-id "$ACCOUNT_ID"; }
echo "Esto borra el dashboard y cancela la suscripcion de QuickSight de la cuenta $ACCOUNT_ID."
read -r -p "Escribe CANCELAR QUICKSIGHT para continuar: " OK </dev/tty
[ "$OK" = "CANCELAR QUICKSIGHT" ] || { echo "No se hizo nada"; exit 1; }
qs delete-dashboard --dashboard-id arq-datos-ventas >/dev/null 2>&1 && echo "dashboard borrado" || true
qs delete-analysis --analysis-id arq-datos-ventas-analisis --force-delete-without-recovery >/dev/null 2>&1 && echo "analisis borrado" || true
qs delete-data-set --data-set-id arq-datos-ventas >/dev/null 2>&1 && echo "dataset borrado" || true
qs delete-data-source --data-source-id arq-datos-dw-redshift >/dev/null 2>&1 && echo "fuente borrada" || true
qs delete-vpc-connection --vpc-connection-id arq-datos-dw-vpc >/dev/null 2>&1 && echo "conexion VPC borrada" || true
aws ssm delete-parameters --names /arq-datos/dw/quicksight_dataset_id /arq-datos/dw/quicksight_user_arn \
  /arq-datos/dw/quicksight_vpc_connection_arn >/dev/null
qs update-account-settings --default-namespace default --no-termination-protection-enabled >/dev/null
qs delete-account-subscription >/dev/null
echo "Suscripcion cancelada. Estado:"
qs describe-account-subscription --query AccountInfo.AccountSubscriptionStatus --output text || true
