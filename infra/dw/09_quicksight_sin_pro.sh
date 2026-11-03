#!/usr/bin/env bash
# Quita el rol Pro a todos los usuarios de QuickSight. Un solo usuario Pro activa la tarifa de infraestructura de
# 250 USD al mes por cuenta, y el autor Pro cuesta 40 USD frente a 24 del autor normal. El proyecto no usa las
# funciones Pro (Q, temas ni BI generativa). La suscripcion registro al usuario IAM como ADMIN_PRO aunque
# 07_quicksight.sh lo pidio como ADMIN. Idempotente y reversible con update-user. Corre en el Mac despues de arq.
set -euo pipefail
: "${ACCOUNT_ID:?Corre arq primero, sola en su propia linea}"
qs() { aws quicksight "$@" --aws-account-id "$ACCOUNT_ID" --namespace default; }
listar() { qs list-users --query 'UserList[].[UserName, Role, IdentityType, Active]' --output table; }

echo "Usuarios antes"
listar
PRO=$(qs list-users --query 'UserList[?ends_with(Role, `_PRO`)].[UserName, Role, Email]' --output text)
if [ -z "$PRO" ]; then
  echo "Ningun usuario tiene rol Pro"
  exit 0
fi
while IFS=$'\t' read -r nombre rol correo; do
  nuevo=${rol%_PRO}
  echo "$nombre: $rol -> $nuevo"
  qs update-user --user-name "$nombre" --role "$nuevo" --email "$correo" >/dev/null
done <<< "$PRO"
echo "Usuarios despues"
listar
