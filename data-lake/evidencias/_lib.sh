# Funciones compartidas por los scripts de evidencias. Se carga con source desde la raiz del repo.

# Si la terminal no se abrio con arq, carga las variables del proyecto por su cuenta.
if [ -z "${DL_BUCKET:-}" ] || [ -z "${PYTHONPATH:-}" ]; then
  source infra/env.sh
fi
: "${DL_BUCKET:?No se pudo cargar infra/env.sh}"
: "${ACCOUNT_ID:?No se pudo obtener la cuenta, revisa las credenciales del perfil admin}"

# El entorno conda no se puede activar desde un script, solo verificar que este activo.
if ! python -c "import boto3, pyarrow, defusedxml" 2>/dev/null; then
  echo "El python activo ($(command -v python)) no tiene las dependencias del proyecto."
  echo "Corre primero  conda activate arq-aws  (o el alias arq) y vuelve a intentar."
  exit 1
fi
export AWS_PAGER=""
mkdir -p "$OUT"
ONLY="${1:-}"

# Todo el codigo del pipeline corre con el rol de minimo privilegio, igual que en la EC2.
py() { AWS_PROFILE=pipeline python "$@"; }

# paso <id> <slug> <titulo> <funcion> [argumentos]
# Limpia la pantalla, imprime encabezado con fecha y cuenta, ejecuta la funcion,
# guarda la salida en $OUT/<id>_<slug>.txt y espera Enter para que tomes la captura.
paso() {
  local id="$1" slug="$2" titulo="$3" fn="$4"
  shift 4
  if [ -n "$ONLY" ] && [ "$ONLY" != "$id" ]; then
    return 0
  fi
  clear
  {
    printf '== %s. %s ==\n' "$id" "$titulo"
    printf '%s  (%s UTC)  cuenta %s  region %s\n\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" \
      "$(date -u '+%H:%M:%S')" "$ACCOUNT_ID" "$AWS_REGION"
    "$fn" "$@"
  } 2>&1 | tee "$OUT/${id}_${slug}.txt"
  if [ -z "${SIN_PAUSA:-}" ]; then
    printf '\n'
    read -r -p '[Toma la captura y presiona Enter] ' _ </dev/tty
  fi
}
