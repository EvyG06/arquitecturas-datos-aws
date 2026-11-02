#!/bin/bash
# Arranque de la instancia de Airflow. cloud-init lo ejecuta una vez como root en el primer inicio.
# La salida queda en /var/log/cloud-init-output.log.
set -euxo pipefail

COMPOSE_VERSION=v2.29.7
BUILDX_VERSION=v0.17.1
PLUGINS=/usr/local/lib/docker/cli-plugins

dnf -y install docker git

mkdir -p /etc/docker
cat > /etc/docker/daemon.json <<'JSON'
{"log-driver": "json-file", "log-opts": {"max-size": "10m", "max-file": "3"}}
JSON
systemctl enable --now docker
usermod -aG docker ec2-user

mkdir -p "$PLUGINS"
curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
  -o "$PLUGINS/docker-compose"
curl -fsSL "https://github.com/docker/buildx/releases/download/${BUILDX_VERSION}/buildx-${BUILDX_VERSION}.linux-amd64" \
  -o "$PLUGINS/docker-buildx"
chmod 755 "$PLUGINS"/docker-*

if [ ! -f /swapfile ]; then
  fallocate -l 4G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

docker compose version
docker buildx version
echo "ARQ-DATOS BOOTSTRAP OK"
