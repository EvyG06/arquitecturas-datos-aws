#!/usr/bin/env bash
# Ejecuta un comando en la instancia de Airflow por Session Manager, sin SSH ni puertos abiertos,
# y muestra su salida. Por defecto corre como ec2-user con shell de login (grupo docker incluido).
#   ./infra/ec2/remoto.sh 'docker compose -f arquitecturas-datos-aws/orquestacion/docker-compose.yml ps'
#   ./infra/ec2/remoto.sh --root 'tail -5 /var/log/cloud-init-output.log'
set -euo pipefail
: "${AIRFLOW_INSTANCE:?Abre la terminal con arq primero}"
AS_ROOT=0
if [ "${1:-}" = "--root" ]; then AS_ROOT=1; shift; fi
[ $# -gt 0 ] || { echo "Uso: $0 [--root] 'comando'"; exit 2; }

# El comando viaja en base64 para que ninguna comilla o simbolo se reinterprete en el camino.
B64=$(printf '%s' "$*" | base64 | tr -d '\n')
if [ "$AS_ROOT" = 1 ]; then
  RUN='bash "$f"'
else
  RUN='runuser -l ec2-user -c "bash $f"'
fi
SCRIPT="f=\$(mktemp); echo $B64 | base64 -d > \"\$f\"; chmod 644 \"\$f\"; $RUN; rc=\$?; rm -f \"\$f\"; exit \$rc"
PARAMS=$(python3 -c 'import json, sys; print(json.dumps({"commands": [sys.argv[1]], "executionTimeout": ["3600"]}))' "$SCRIPT")

ID=$(aws ssm send-command --instance-ids "$AIRFLOW_INSTANCE" --document-name AWS-RunShellScript \
  --comment "arq-datos remoto" --parameters "$PARAMS" --query Command.CommandId --output text)

while true; do
  STATUS=$(aws ssm get-command-invocation --command-id "$ID" --instance-id "$AIRFLOW_INSTANCE" \
    --query Status --output text 2>/dev/null || echo Pending)
  case "$STATUS" in
    Pending|InProgress|Delayed) sleep 5 ;;
    *) break ;;
  esac
done

aws ssm get-command-invocation --command-id "$ID" --instance-id "$AIRFLOW_INSTANCE" \
  --query '[StandardOutputContent, StandardErrorContent]' --output text
CODE=$(aws ssm get-command-invocation --command-id "$ID" --instance-id "$AIRFLOW_INSTANCE" \
  --query ResponseCode --output text)
echo "[ssm $ID | estado $STATUS | codigo $CODE]"
# Devuelve el codigo real del comando remoto, asi quien llama puede distinguir un 99 (omitido) de un error.
[ "$STATUS" = "Success" ] && exit 0
case "$CODE" in ''|*[!0-9]*|0) exit 1 ;; *) exit "$CODE" ;; esac
