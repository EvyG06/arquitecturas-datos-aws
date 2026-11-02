#!/usr/bin/env bash
# Corre EN LA EC2 como ec2-user, enviado con:  ./infra/ec2/remoto.sh "$(cat infra/ec2/preparar_github.sh)"
# Verifica el arranque y prepara una llave de despliegue de solo lectura para clonar el repo.
set -euo pipefail

if ! sudo grep -q "ARQ-DATOS BOOTSTRAP OK" /var/log/cloud-init-output.log; then
  echo "El arranque de la instancia no termino bien. Ultimas lineas del log"
  sudo tail -30 /var/log/cloud-init-output.log
  exit 1
fi
echo "Arranque OK  $(docker compose version)  $(docker buildx version | cut -d' ' -f1-2)"

mkdir -p ~/.ssh && chmod 700 ~/.ssh
[ -f ~/.ssh/github_deploy ] || ssh-keygen -q -t ed25519 -N "" -C "arq-datos-airflow deploy key" -f ~/.ssh/github_deploy

# Las llaves de host de GitHub se obtienen de su API por HTTPS, autenticada con TLS,
# en lugar de confiar en lo primero que responda un ssh-keyscan.
curl -fsSL https://api.github.com/meta |
  python3 -c 'import json, sys; [print("github.com", k) for k in json.load(sys.stdin)["ssh_keys"]]' \
  > ~/.ssh/known_hosts_github
cat > ~/.ssh/config <<'CFG'
Host github.com
  User git
  IdentityFile ~/.ssh/github_deploy
  IdentitiesOnly yes
  UserKnownHostsFile ~/.ssh/known_hosts_github
  StrictHostKeyChecking yes
CFG
chmod 600 ~/.ssh/config

echo
echo "Huellas de host de GitHub registradas"
ssh-keygen -lf ~/.ssh/known_hosts_github
echo
echo "Llave publica para agregar en GitHub como deploy key de solo lectura"
cat ~/.ssh/github_deploy.pub
