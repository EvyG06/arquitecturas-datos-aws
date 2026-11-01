# Funciones compartidas por los scripts de evidencias. Se carga con source desde la raiz del repo.
: "${DL_BUCKET:?Abre la terminal con arq antes de correr este script}"
: "${ACCOUNT_ID:?Abre la terminal con arq antes de correr este script}"
: "${PYTHONPATH:?Abre la terminal con arq antes de correr este script}"
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
