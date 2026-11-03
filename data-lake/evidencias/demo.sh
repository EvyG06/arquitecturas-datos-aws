#!/usr/bin/env bash
# Demostracion del data lake (seccion 5.4 del enunciado), con el DAG dl_noticias_rss en la EC2:
#   d1 ingesta completa programada   d2 indicadores en Athena          d3 reproceso de un lote sin duplicar
#   d4 fallo controlado y recuperacion   d5 limitacion del enriquecimiento con Comprehend
#   ./data-lake/evidencias/demo.sh        todos los pasos, con pausa para explicar cada uno
#   ./data-lake/evidencias/demo.sh d3     solo un paso (los ids de lote quedan en evidencias/dl/demo/run_ids.env)
# Corre en el Mac despues de arq. Solo enciende la EC2. Al terminar deja el DAG en pausa.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
OUT=evidencias/dl/demo
source data-lake/evidencias/_lib.sh

DAG=dl_noticias_rss
ORQ=arquitecturas-datos-aws/orquestacion
remoto() { ./infra/ec2/remoto.sh "$@"; }
dag() { remoto "$ORQ/dag.sh $*"; }
seguro() { printf '%s' "$1" | sed 's/[^A-Za-z0-9_.-]/_/g'; }

IDS="$OUT/run_ids.env"
if [ ! -f "$IDS" ]; then
  t=$(date -u +%Y%m%dT%H%M%SZ)
  printf 'RUN_REPRO=manual__demo_repro_%s\nRUN_FALLO=manual__demo_fallo_%s\n' "$t" "$t" > "$IDS"
fi
source "$IDS"

UNICIDAD="SELECT count(*) AS filas, count(DISTINCT news_id) AS noticias_distintas,
       (SELECT sum(n_noticias) FROM gold_news_daily) AS noticias_en_gold FROM silver_news"

ec2_lista() {
  local estado
  estado=$(aws ec2 describe-instances --instance-ids "$AIRFLOW_INSTANCE" \
    --query 'Reservations[0].Instances[0].State.Name' --output text)
  if [ "$estado" = stopped ]; then aws ec2 start-instances --instance-ids "$AIRFLOW_INSTANCE" >/dev/null; fi
  for _ in $(seq 1 60); do
    [ "$(aws ssm describe-instance-information --filters Key=InstanceIds,Values="$AIRFLOW_INSTANCE" \
      --query 'InstanceInformationList[0].PingStatus' --output text)" = Online ] && return 0
    sleep 10
  done
  echo "La EC2 no quedo en linea en 10 minutos"; return 1
}

d1() {
  ec2_lista || return 1
  local previa ultima
  previa=$(dag ultima "$DAG" | grep -E '^[a-z_]+__' | head -1)
  dag activar "$DAG" >/dev/null
  echo "DAG activo, programado cada 30 minutos. Esperando la corrida del ultimo intervalo..."
  for _ in $(seq 1 12); do
    ultima=$(dag ultima "$DAG" | grep -E '^[a-z_]+__' | head -1)
    [ -n "$ultima" ] && [ "$ultima" != "$previa" ] && break
    sleep 15
  done
  if [ -z "$ultima" ] || [ "$ultima" = "$previa" ]; then
    ultima=manual__demo_ingesta_$(date -u +%Y%m%dT%H%M%SZ)
    echo "El programador no creo una corrida, se dispara $ultima"
    remoto "$ORQ/dag.sh disparar $DAG $ultima"
  fi
  printf 'RUN_INGESTA=%s\n' "$ultima" >> "$IDS"
  dag esperar "$DAG" "$ultima"
  dag tareas "$DAG" "$ultima"
  echo
  py -m dl.evidence lote "$(seguro "$ultima")"
}

d2() {
  py -m dl.athena data-lake/sql/01_noticias_por_fecha_y_seccion.sql data-lake/sql/02_top10_organizaciones.sql \
    data-lake/sql/04_frases_clave_por_seccion.sql data-lake/sql/05_conciliacion_gold_vs_silver.sql
}

d3() {
  source "$IDS"  # cada paso corre en una subshell: el lote de d1 se lee del archivo
  : "${RUN_INGESTA:?Corre primero el paso d1}"
  echo "Antes del reproceso"; py -m dl.athena -e "$UNICIDAD"
  remoto "$ORQ/dag.sh disparar $DAG $RUN_REPRO '{\"reprocesar_run_id\": \"$RUN_INGESTA\"}'"
  dag esperar "$DAG" "$RUN_REPRO"
  dag tareas "$DAG" "$RUN_REPRO"
  echo; echo "Despues del reproceso: mismas filas y una sola fila por noticia"
  py -m dl.athena -e "$UNICIDAD"
  echo; py -m dl.evidence lote "$(seguro "$RUN_INGESTA")"
}

d4() {
  remoto "$ORQ/dag.sh disparar $DAG $RUN_FALLO '{\"fallo_controlado\": \"silver_antes_de_publicar\"}'"
  dag esperar "$DAG" "$RUN_FALLO"
  dag tareas "$DAG" "$RUN_FALLO"
  echo; echo "Intento 1 fallido antes de publicar e intento 2 exitoso"
  remoto "$ORQ/dag.sh log $DAG $RUN_FALLO publicar_silver 'controlad|publica|Marking task as'"
  echo; echo "Sin perdida ni duplicacion"
  py -m dl.athena -e "$UNICIDAD" data-lake/sql/06_validaciones_calidad.sql
}

d5() {
  echo "Comprehend clasifica cada mencion sin contexto y sin resolver alias, sobre un RSS de titulo y descripcion corta."
  py -m dl.athena -e "SELECT entity_type, entity_text, round(score, 2) AS score, news_id
FROM silver_news_entities WHERE score < 0.7 ORDER BY score LIMIT 10" -e "SELECT entity_text, entity_type, sum(n_noticias) AS noticias
FROM gold_entity_daily WHERE entity_norm IN ('china', 'estados unidos', 'el', 'petro', 'gustavo petro')
GROUP BY entity_text, entity_type ORDER BY noticias DESC"
}

paso d1 ingesta "Ingesta completa programada por Airflow, de los RSS a Gold" d1
paso d2 athena "Indicadores en Athena" d2
paso d3 reproceso "Reproceso de un lote desde Bronze sin consultar los RSS ni duplicar" d3
paso d4 fallo "Fallo controlado en Silver y recuperacion en el reintento" d4
paso d5 limitacion "Limitacion del enriquecimiento de texto" d5
dag pausar "$DAG" >/dev/null 2>&1 || true
echo "DAG en pausa. Para apagar la EC2: ./infra/detener.sh"
