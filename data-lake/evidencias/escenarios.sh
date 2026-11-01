#!/usr/bin/env bash
# Escenarios de calidad, incrementalidad y recuperacion (seccion 4.5 del enunciado).
# ESTE SCRIPT SI MODIFICA EL ESTADO EN AWS. Crea tres lotes nuevos y reprocesa Silver y Gold.
#   ./data-lake/evidencias/escenarios.sh             todos los pasos en orden, con pausa para cada captura
#   ./data-lake/evidencias/escenarios.sh 3b          solo el paso 3b
# Los run_id quedan en evidencias/dl/escenarios/run_ids.env. Borra ese archivo para repetir con lotes nuevos.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
OUT=evidencias/dl/escenarios
source data-lake/evidencias/_lib.sh

IDS="$OUT/run_ids.env"
if [ ! -f "$IDS" ]; then
  t=$(date -u +%Y%m%dT%H%M%SZ)
  printf 'RUN_INC=manual__inc_%s\nRUN_CTL=prueba__controlada_%s\nRUN_FAIL=manual__fallo_%s\nHOY=%s\n' \
    "$t" "$t" "$t" "$(TZ=America/Bogota date +%F)" > "$IDS"
fi
source "$IDS"

VALIDACIONES="data-lake/sql/05_conciliacion_gold_vs_silver.sql data-lake/sql/06_validaciones_calidad.sql"
CONTEO="SELECT 'silver_news' AS tabla, count(*) AS filas, count(DISTINCT news_id) AS noticias FROM silver_news
UNION ALL SELECT 'silver_news_entities', count(*), count(DISTINCT news_id) FROM silver_news_entities
UNION ALL SELECT 'silver_news_key_phrases', count(*), count(DISTINCT news_id) FROM silver_news_key_phrases
UNION ALL SELECT 'gold_news_daily', count(*), sum(n_noticias) FROM gold_news_daily
UNION ALL SELECT 'gold_entity_daily', count(*), NULL FROM gold_entity_daily
UNION ALL SELECT 'gold_key_phrase_daily', count(*), NULL FROM gold_key_phrase_daily"
RESUMEN="SELECT (SELECT count(*) FROM silver_news) AS noticias,
       (SELECT count(DISTINCT news_id) FROM silver_news) AS distintas,
       (SELECT count(*) FROM silver_news WHERE version > 1) AS con_cambios,
       (SELECT count(*) FROM silver_news_entities) AS entidades,
       (SELECT count(*) FROM silver_news_key_phrases) AS frases,
       (SELECT sum(n_noticias) FROM gold_news_daily) AS gold_noticias"

publicar() {
  py -m dl.gold 2>&1 >/dev/null
  py -m dl.catalog --sync 2>&1 >/dev/null
}

e1a() { echo "La carga inicial fue el lote manual__primera"; echo; py -m dl.evidence lote manual__primera; }
e1b() { py -m dl.athena -e "$CONTEO"; }

e2a() { py -m dl.ingest --run-id "$RUN_INC"; echo "exit=$?"; }
e2b() { py -m dl.silver --run-id "$RUN_INC"; }
e2c() { py -m dl.gold; }

e3a() {
  echo "Particiones del manifiesto que Glue todavia no conoce"
  py -m dl.evidence particiones
  py -m dl.athena -e "SELECT count(*) AS noticias FROM silver_news WHERE pub_date = DATE '$HOY'"
}
e3b() { py -m dl.catalog --sync 2>&1 >/dev/null; }
e3c() {
  py -m dl.evidence particiones
  py -m dl.athena -e "SELECT seccion, count(*) AS noticias FROM silver_news WHERE pub_date = DATE '$HOY' GROUP BY seccion ORDER BY seccion"
}

e4a() { py -m dl.ingest --run-id "$RUN_INC"; echo "exit=$?"; }
e4b() {
  echo "Lotes pendientes en Bronze"
  py -m dl.silver --pending 2>&1 >/dev/null
  echo
  echo "Reproceso forzado del mismo lote $RUN_INC"
  py -m dl.silver --run-id "$RUN_INC" --force 2>&1 >/dev/null
}
e4c() {
  echo "Gold"; py -m dl.gold 2>&1 >/dev/null
  echo; echo "Glue Data Catalog"; py -m dl.catalog --sync 2>&1 >/dev/null
}
e4d() { py -m dl.athena $VALIDACIONES; }

e5a() { py -m dl.controlled --from-run "$RUN_INC" --run-id "$RUN_CTL"; }
e5b() { py -m dl.silver --run-id "$RUN_CTL"; }
e5c() { py -m dl.evidence cuarentena "$RUN_CTL"; }

e6a() { py -m dl.evidence versiones; }
e6b() {
  publicar
  py -m dl.athena -e "SELECT news_id, version, first_seen_run_id, content_run_id, nlp_status, title FROM silver_news WHERE version > 1"
}

e7a() { py -m dl.ingest --config data-lake/evidencias/sources_mundo_caido.json --run-id "$RUN_FAIL"; echo "exit=$?"; }
e7b() {
  py -m dl.ingest --run-id "$RUN_FAIL" --attempt 2; echo "exit=$?"
  echo; echo "Originales del lote en Bronze, uno por fuente"
  aws s3 ls "s3://$DL_BUCKET/bronze/" --recursive --profile pipeline | grep "run_id=$RUN_FAIL/"
}
e7c() {
  DL_FAULT=silver_antes_de_publicar AWS_PROFILE=pipeline python -m dl.silver --run-id "$RUN_FAIL" 2>&1 >/dev/null
  echo "exit=$?"; echo
  py -m dl.evidence lote "$RUN_FAIL"
  echo; echo "Archivos escritos que ningun manifiesto referencia, invisibles para Athena"
  aws s3 ls "s3://$DL_BUCKET/silver/" --recursive --profile pipeline | grep -- "-$RUN_FAIL/" || echo "  (ninguno)"
}
e7d() {
  py -m dl.silver --run-id "$RUN_FAIL" 2>&1 >/dev/null; echo "exit=$?"; echo
  py -m dl.evidence lote "$RUN_FAIL"
  echo; echo "Archivos del lote despues de reejecutar, mismas rutas y sin copias nuevas"
  aws s3 ls "s3://$DL_BUCKET/silver/" --recursive --profile pipeline | grep -- "-$RUN_FAIL/" || echo "  (ninguno)"
}
e7e() { publicar; py -m dl.athena $VALIDACIONES; }

e8a() { echo "Estado publicado antes del reproceso"; py -m dl.athena -e "$RESUMEN"; }
e8b() {
  py -m dl.silver --rebuild 2>&1 >/dev/null
  echo; echo "Athena sigue leyendo la version anterior completa hasta actualizar el catalogo"
  py -m dl.evidence particiones
}
e8c() { publicar; }
e8d() {
  echo "Estado publicado despues del reproceso, debe coincidir con 8a"
  py -m dl.athena -e "$RESUMEN"
  py -m dl.evidence particiones
}

paso 1a carga_inicial_lote "Carga inicial, lote manual__primera en Bronze y Silver" e1a
paso 1b carga_inicial_athena "Carga inicial, datos consultables en Athena" e1b
paso 2a incremental_ingesta "Nuevo lote incremental, ingesta $RUN_INC" e2a
paso 2b incremental_silver "Nuevo lote incremental, Silver solo agrega lo nuevo" e2b
paso 2c incremental_gold "Nuevo lote incremental, Gold recalcula solo las fechas afectadas" e2c
paso 3a particion_antes "Nueva particion $HOY, antes de actualizar los metadatos" e3a
paso 3b particion_sync "Nueva particion, actualizacion del Glue Data Catalog" e3b
paso 3c particion_despues "Nueva particion, disponible en Athena" e3c
paso 4a repeticion_ingesta "Repeticion de un lote exitoso, ingesta" e4a
paso 4b repeticion_silver "Repeticion de un lote exitoso, Silver" e4b
paso 4c repeticion_gold "Repeticion de un lote exitoso, Gold y catalogo" e4c
paso 4d repeticion_validaciones "Repeticion de un lote exitoso, sin duplicados ni doble conteo" e4d
paso 5a defectuosos_copia "Registros defectuosos, copia controlada del RSS" e5a
paso 5b defectuosos_silver "Registros defectuosos y contenido modificado, procesamiento del lote" e5b
paso 5c defectuosos_cuarentena "Registros defectuosos, cuarentena con trazabilidad a Bronze" e5c
paso 6a modificada_version "Noticia con contenido modificado, version vigente" e6a
paso 6b modificada_athena "Noticia con contenido modificado, publicada en Athena" e6b
paso 7a fallo_fuente "Fallo controlado, fuente caida" e7a
paso 7b fallo_reintento "Fallo controlado, reejecucion de la ingesta" e7b
paso 7c fallo_silver "Fallo controlado en Silver antes de publicar" e7c
paso 7d fallo_recuperacion "Fallo controlado, reejecucion de Silver" e7d
paso 7e fallo_validaciones "Fallo controlado, resultados sin perdida ni duplicacion" e7e
paso 8a reproceso_antes "Reprocesamiento desde Bronze, estado antes" e8a
paso 8b reproceso_rebuild "Reprocesamiento desde Bronze sin consultar los RSS" e8b
paso 8c reproceso_publicacion "Reprocesamiento desde Bronze, publicacion" e8c
paso 8d reproceso_despues "Reprocesamiento desde Bronze, mismo resultado" e8d
