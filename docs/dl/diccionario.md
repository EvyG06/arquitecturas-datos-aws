# Diccionario de datos del data lake

Todas las tablas están en Parquet con compresión Snappy, registradas en la base `arq_dl` de Glue y particionadas por `pub_date` (tipo `date`), el día de publicación en hora de Bogotá. Los esquemas salen del código ([silver.py](../../data-lake/src/dl/silver.py) y [gold.py](../../data-lake/src/dl/gold.py)), y [catalog.py](../../data-lake/src/dl/catalog.py) los traduce a tipos de Glue. Los timestamps se guardan en UTC sin zona.

Una noticia sin fecha de publicación válida no tiene `pub_date` y por eso no llega a ninguna tabla. Queda en la cuarentena con `fecha_invalida` o `fecha_fuera_de_rango` (ver [README](README.md#limpieza-y-validación)).

## Silver

### `silver_news`

**Granularidad.** Una fila por noticia vigente (`news_id` único en toda la tabla). Si la noticia cambia de texto, la fila se reemplaza por la versión nueva.

| Columna | Tipo | Descripción |
|---|---|---|
| `news_id` | string | Id estable. Número de la URL de El Tiempo, o `h_` + 16 caracteres del SHA-256 del enlace |
| `id_strategy` | string | `url_numeric_id` o `link_hash` |
| `source_id` | string | Feed primario, el primero donde se vio la noticia. No cambia en las actualizaciones |
| `medio` | string | Medio del feed primario |
| `seccion` | string | Sección del feed primario. Es la que usa Gold |
| `sources_seen` | array&lt;string&gt; | Todos los feeds donde apareció la noticia |
| `link` | string | Enlace normalizado, sin parámetros, fragmento ni barra final |
| `guid` | string | GUID original del ítem |
| `title` | string | Título sin HTML |
| `description` | string | Descripción sin HTML |
| `category` | string | Categoría del ítem, sin HTML |
| `pub_date_raw` | string | `pubDate` original, tal como venía en el RSS |
| `published_at_utc` | timestamp | Fecha de publicación convertida a UTC |
| `nlp_text` | string | Texto analizado por Comprehend, `título. descripción` |
| `text_hash` | string | SHA-256 de `nlp_text`. Define si la noticia cambió |
| `version` | int | 1 al aparecer, más 1 por cada cambio de texto |
| `first_seen_run_id` | string | Lote de Bronze donde apareció por primera vez |
| `content_run_id` | string | Lote de Bronze que trajo el contenido vigente |
| `content_ingested_at_utc` | timestamp | Hora de ingesta del contenido vigente |
| `bronze_uri` | string | Ruta S3 del `feed.xml` del que salió el contenido vigente |
| `item_position` | int | Posición del ítem dentro de ese feed |
| `nlp_status` | string | `analizado`, `texto_vacio`, `texto_demasiado_largo`, `idioma_no_soportado` o `error_api` |
| `nlp_language` | string | Idioma detectado |
| `nlp_language_score` | double | Confianza de la detección de idioma |
| `nlp_analyzed_at_utc` | timestamp | Fecha del análisis de Comprehend |
| `pub_date` | date (partición) | Día de publicación en hora de Bogotá |

### `silver_news_entities`

**Granularidad.** Una fila por entidad distinta detectada en cada noticia vigente. La entidad es el par (`entity_type`, `entity_norm`).

| Columna | Tipo | Descripción |
|---|---|---|
| `news_id` | string | Noticia vigente a la que pertenece |
| `text_hash` | string | Texto analizado, el mismo de la noticia |
| `entity_type` | string | `persona`, `organizacion` o `lugar` (PERSON, ORGANIZATION y LOCATION de Comprehend) |
| `entity_text` | string | Texto de la primera mención, con espacios normalizados |
| `entity_norm` | string | Texto en minúsculas con `casefold` y espacios normalizados. Los acentos se conservan |
| `score` | double | Mayor confianza de Comprehend entre las menciones |
| `mentions` | int | Veces que Comprehend detectó la entidad en el texto |
| `pub_date` | date (partición) | Igual a la de la noticia |

### `silver_news_key_phrases`

**Granularidad.** Una fila por frase clave distinta (`phrase_norm`) detectada en cada noticia vigente.

| Columna | Tipo | Descripción |
|---|---|---|
| `news_id` | string | Noticia vigente a la que pertenece |
| `text_hash` | string | Texto analizado |
| `phrase_text` | string | Texto de la primera aparición |
| `phrase_norm` | string | Texto normalizado con `casefold` y espacios |
| `score` | double | Mayor confianza entre las apariciones |
| `mentions` | int | Veces que aparece en el texto |
| `pub_date` | date (partición) | Igual a la de la noticia |

## Gold

Cada noticia vigente tiene una sola `pub_date` y una sola sección primaria, así que sumar `n_noticias` entre fechas o secciones no cuenta dos veces la misma noticia.

### `gold_news_daily`

**Granularidad.** Una fila por día, medio y sección.

| Columna | Tipo | Definición |
|---|---|---|
| `medio` | string | Medio |
| `seccion` | string | Sección primaria |
| `n_noticias` | int | Noticias vigentes distintas publicadas ese día en esa sección |
| `pub_date` | date (partición) | Día de publicación |

### `gold_entity_daily`

**Granularidad.** Una fila por día, sección y entidad (`entity_type`, `entity_norm`).

| Columna | Tipo | Definición |
|---|---|---|
| `seccion` | string | Sección primaria de las noticias |
| `entity_type` | string | `persona`, `organizacion` o `lugar` |
| `entity_norm` | string | Llave de la entidad |
| `entity_text` | string | Forma de escribir la entidad que aparece en más noticias de ese día y sección |
| `n_noticias` | int | Noticias distintas que mencionan la entidad. Es la métrica de presencia |
| `menciones` | int | Suma de `mentions`. Una noticia que repite el nombre suma varias veces aquí, pero una sola en `n_noticias` |
| `score_promedio` | double | Promedio de `score` entre esas noticias, con 4 decimales |
| `pub_date` | date (partición) | Día de publicación |

### `gold_key_phrase_daily`

**Granularidad.** Una fila por día, sección y frase clave (`phrase_norm`).

| Columna | Tipo | Definición |
|---|---|---|
| `seccion` | string | Sección primaria |
| `phrase_norm` | string | Llave de la frase |
| `phrase_text` | string | Forma más frecuente de la frase |
| `n_noticias` | int | Noticias distintas que contienen la frase |
| `menciones` | int | Suma de apariciones |
| `pub_date` | date (partición) | Día de publicación |

### Conciliación entre Silver y Gold

| Métrica de Gold | Equivalente en Silver |
|---|---|
| `sum(n_noticias)` de `gold_news_daily` | `count(distinct news_id)` de `silver_news` |
| `sum(n_noticias)` de `gold_entity_daily` | `count(distinct (news_id, entity_type, entity_norm))` de `silver_news_entities` |
| `sum(n_noticias)` de `gold_key_phrase_daily` | `count(distinct (news_id, phrase_norm))` de `silver_news_key_phrases` |

`construir_gold` lo verifica por día antes de publicar y [05_conciliacion_gold_vs_silver.sql](../../data-lake/sql/05_conciliacion_gold_vs_silver.sql) lo repite en Athena sobre lo publicado.

## Cuarentena

`quarantine/silver/run_id=<id>/rechazos.jsonl`, una línea JSON por ítem rechazado.

| Campo | Descripción |
|---|---|
| `rejection_reason` | `titulo_vacio`, `enlace_ausente`, `enlace_invalido`, `fecha_invalida` o `fecha_fuera_de_rango` |
| `source_id`, `medio`, `seccion` | Feed de donde vino |
| `bronze_run_id` | Lote de Bronze |
| `bronze_uri` | Ruta S3 del `feed.xml` original |
| `item_position` | Posición del ítem en ese feed |
| `raw` | `title`, `link`, `guid`, `description`, `pubDate` y `category` tal como venían |

## Archivos de control

| Archivo | Contenido |
|---|---|
| `_control/ingestion/run_id=<id>/_run.json` | Estado del lote (`complete`, `partial`, `failed`) y de cada fuente |
| `_control/ingestion/run_id=<id>/source=<fuente>.json` | Registro de la descarga de una fuente |
| `_control/silver/state.json` | Manifiesto de Silver. Archivo vigente por tabla y fecha, build actual y lotes procesados con sus conteos |
| `_control/silver/run_id=<id>/<build>__<fecha>.json` | Reporte completo de cada procesamiento del lote |
| `_control/gold/state.json` | Manifiesto de Gold. Archivo vigente por tabla y fecha, y las entradas de Silver con que se calculó |
| `_control/pipeline/run_id=<id>/<run>.json` | Resultado de la ejecución del DAG y estado de cada tarea |
| `cache/comprehend/<xx>/<text_hash>.json` | Resultado de Comprehend de un texto, escrito una sola vez |
