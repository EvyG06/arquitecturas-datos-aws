#!/usr/bin/env bash
# Demostracion del DW (seccion 4 del enunciado): un lote de negocio nuevo desde las fuentes hasta el dashboard.
# Con --fallo, la carga falla en el primer intento y se muestra como se identifica y como se recupera.
#   ./infra/dw/demo.sh 9            lote de negocio 9 (los lotes 1 a 8 ya estan cargados)
#   ./infra/dw/demo.sh 10 --fallo   lote 10 con fallo controlado en cargar_redshift y reintento automatico
# Corre en el Mac despues de arq. Hace una pausa antes de cada paso para explicarlo. No hace commit.
# La salida queda en .logs/. Al terminar deja el DAG en pausa y la infraestructura encendida.
set -euo pipefail
: "${DW_BUCKET:?Corre arq primero, sola en su propia linea}"
: "${AIRFLOW_INSTANCE:?Corre arq primero, sola en su propia linea}"
NEGOCIO="${1:-}"
FALLO="${2:-}"
case "$NEGOCIO" in ''|*[!0-9]*) echo "Uso: $0 <lote de negocio de 9 a 365> [--fallo]"; exit 2 ;; esac
[ "$NEGOCIO" -ge 1 ] && [ "$NEGOCIO" -le 365 ] || { echo "El lote de negocio va de 1 a 365"; exit 2; }
[ -z "$FALLO" ] || [ "$FALLO" = "--fallo" ] || { echo "Opcion desconocida: $FALLO"; exit 2; }
cd "$(dirname "$0")/../.."
mkdir -p .logs
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG=".logs/demo_dw_$STAMP.log"
TMP=$(mktemp -d)
exec > >(tee -a "$LOG") 2>&1

DAG=dw_ecommerce
RUN=manual__demo_$STAMP
ORQ=arquitecturas-datos-aws/orquestacion
remoto() { ./infra/ec2/remoto.sh "$@"; }
dw() { remoto "$ORQ/dw.sh $*"; }
dag() { remoto "$ORQ/dag.sh $*"; }
paso() {
  printf '\n==== %s | %s\n' "$(date +%H:%M:%S)" "$*"
  if [ -t 0 ] && [ -z "${SIN_PAUSA:-}" ]; then read -r -p "[Enter para continuar] " _ </dev/tty; fi
}
lineas_total() { awk '$1 == "Todos" && $2 == "los" {print $4; exit}' "$1"; }
pausar_al_salir() { dag pausar "$DAG" >/dev/null 2>&1 || true; rm -rf "$TMP"; }
trap pausar_al_salir EXIT

paso "1. Encender EC2, RDS y DocumentDB (Redshift Serverless y QuickSight no se apagan)"
./infra/encender.sh

paso "2. Indicador antes del lote, la misma consulta que valida el KPI del dashboard"
dw loading.consultas indicador | tee "$TMP/antes.txt"
ANTES=$(lineas_total "$TMP/antes.txt")

paso "3. DAG activo. Si el programador crea la corrida del ultimo intervalo, se espera a que termine"
PREVIA=$(dag ultima "$DAG" | grep -E '^[a-z_]+__' | head -1 || true)
dag activar "$DAG"
sleep 45
ULTIMA=$(dag ultima "$DAG" | grep -E '^[a-z_]+__' | head -1 || true)
if [ -n "$ULTIMA" ] && [ "$ULTIMA" != "$PREVIA" ]; then
  echo "Corrida programada $ULTIMA, se espera antes de agregar el lote"
  dag esperar "$DAG" "$ULTIMA" || true
  dw loading.consultas indicador | tee "$TMP/antes.txt"
  ANTES=$(lineas_total "$TMP/antes.txt")
fi

paso "4. Lote de negocio $NEGOCIO en las fuentes: 3 lineas de venta en MySQL, 3 resenas y 3 eventos en DocumentDB"
dw sources.append_batch --batch-id "$NEGOCIO"

if [ "$FALLO" = "--fallo" ]; then
  paso "5. DAG $RUN con fallo controlado: cargar_redshift falla en el intento 1 dentro de su transaccion"
  remoto "$ORQ/dag.sh disparar $DAG $RUN '{\"fallo_controlado\": \"carga_a_mitad\"}'"
else
  paso "5. DAG $RUN: extraccion, S3, carga a Redshift, dbt, pruebas, conciliacion y refresco de SPICE"
  remoto "$ORQ/dag.sh disparar $DAG $RUN"
fi
rc=0; dag esperar "$DAG" "$RUN" || rc=$?

paso "6. Estado, intentos y duracion de cada tarea"
dag tareas "$DAG" "$RUN"

if [ "$FALLO" = "--fallo" ]; then
  paso "7. Identificar el fallo: cada linea dice lote, tarea e intento, y el error muestra la sentencia que fallo"
  remoto "$ORQ/dag.sh log $DAG $RUN cargar_redshift 'tarea=|division by zero|sentencia que fallo|lote cargado|Marking task as'"
  echo
  echo "Recuperacion: la carga es una transaccion por lote, asi que el intento 1 no dejo filas. El intento 2"
  echo "reemplaza las filas del lote (DELETE e INSERT por _batch_id). La marca de agua avanzo una sola vez, en"
  echo "registrar_lote_s3, y la carga no la toca."
  echo "Si se agotan los reintentos: Clear de la tarea en Airflow, o un DAG nuevo con reprocesar_batch_id=$RUN."
fi

paso "8. Registro del lote en S3, con el resultado y el estado de cada tarea"
aws s3 cp "s3://$DW_BUCKET/_control/pipeline/batch_id=$RUN/$RUN.json" -

paso "9. Refresco del dataset SPICE desde el DAG"
remoto "$ORQ/dag.sh log $DAG $RUN refrescar_quicksight 'batch_id=|SPICE|Marking task as'"

paso "10. Indicador despues del lote"
dw loading.consultas indicador | tee "$TMP/despues.txt"
DESPUES=$(lineas_total "$TMP/despues.txt")
if [ "$rc" = 0 ] && [ "$((DESPUES - ANTES))" = 3 ]; then
  echo "OK: el indicador paso de $ANTES a $DESPUES lineas, las 3 del lote de negocio $NEGOCIO"
elif [ "$rc" = 0 ] && [ "$DESPUES" = "$ANTES" ]; then
  echo "El indicador no cambio: el lote de negocio $NEGOCIO ya existia. Repite con un numero nuevo."
else
  echo "REVISAR: DAG con codigo $rc, lineas antes $ANTES y despues $DESPUES"
fi

echo
echo "Dashboard: https://${AWS_REGION}.quicksight.aws.amazon.com/sn/dashboards/arq-datos-ventas"
echo "El KPI debe coincidir con la fila 'Todos los canales' y la tabla por canal con actualizado_en."
echo "Log: $LOG"
echo "Al terminar la sesion: ./infra/detener.sh"
exit "$rc"
