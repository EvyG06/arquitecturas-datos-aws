# Extracción de las fuentes a S3

Módulo: `data-warehouse/src/ecommerce_dw/extraction/`. Corre en la EC2, dentro del contenedor de Airflow, con el rol `arq-datos-pipeline` y los usuarios de solo lectura `etl_lector`.

```bash
./orquestacion/dw.sh extraction.extract --batch-id ext_01_inicial --mode inicial
./orquestacion/dw.sh extraction.extract --batch-id ext_02_incremental      # modo auto: incremental
./orquestacion/dw.sh extraction.extract --estado                           # marcas de agua e historial
```

Códigos de salida: `0` lote extraído, `99` el lote ya estaba extraído (no se consulta ni se escribe nada), `1` error.

## Organización en S3

```
s3://eve-dw-<cuenta>-us-east-2/
  raw/<fuente>/<entidad>/batch_id=<id>/part-00000.parquet     datos del lote
  _control/extraction/batch_id=<id>/plan.json                 ventana fijada del lote
  _control/extraction/batch_id=<id>/entidades/<entidad>.json  filas, bytes y ventana por entidad
  _control/extraction/batch_id=<id>/batch.json                registro del lote
  _control/copy/batch_id=<id>/<fuente>_<entidad>.manifest     manifiesto para COPY
  _control/extraction/state.json                              marcas de agua e historial de lotes
```

Fuentes: `mysql` (customers, products, orders, order_items) y `documentdb` (reviews, navigation_events).

## Por qué Parquet

- **Tipos explícitos.** Decimales, fechas y timestamps viajan con su tipo, sin conversiones de texto ni ambigüedad de zona horaria. En CSV, `10.50` o `2026-01-01 08:00` se vuelven a interpretar al cargarlos.
- **Compatible con COPY.** Redshift carga Parquet con `FORMAT AS PARQUET`. Empareja las columnas **por posición** y exige el mismo número de columnas en la tabla y en el archivo, así que cada entidad tiene un esquema fijo en `entities.py`, con las columnas técnicas al final: `_cdc_ts`, `_batch_id`, `_extracted_at`, `_source`.
- **Columnar y comprimido (snappy).** Ocupa menos en S3 y Redshift lee menos bytes.
- Cada lote tiene un manifiesto COPY con `mandatory: true` y `meta.content_length`, que es obligatorio para Parquet. COPY carga exactamente los archivos del lote, nunca otros que compartan el prefijo.

## Ventana de cada lote (CDC por marca de tiempo)

Cada fila tiene una marca de cambio `_cdc_ts`:

- **MySQL:** columna `TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)` con índice (`sources/cdc.py`). MySQL la actualiza sola en cada INSERT o UPDATE. No se usa `updated_at` porque es una fecha de negocio: `simulate_changes` la escribe con valores del pasado, y `orders` y `order_items` no la tienen.
- **DocumentDB:** no tiene un equivalente a `ON UPDATE`, así que el sistema de origen pone `_cdc_ts` al escribir (`aws_conn.cdc_stamp`).

Un lote lee `desde < _cdc_ts <= hasta`:

- `hasta` es la hora de la fuente al planificar el lote: `UTC_TIMESTAMP(6)` en MySQL y el reloj de la EC2 en DocumentDB, que es el mismo reloj del generador.
- `desde` es la marca de agua del lote anterior **menos un solape de 5 minutos** (`DW_CDC_SOLAPE_SEGUNDOS`). Una transacción que empezó antes del corte y confirmó después tiene un `_cdc_ts` anterior a `hasta`, pero no era visible cuando se leyó. Sin solape, esa fila se perdería para siempre. Con solape la trae el lote siguiente, y queda contada en `repetidas_por_solape`. Raw es de solo inserción y el staging de dbt se queda con la última versión por llave, así que una fila repetida no duplica nada.
- La extracción inicial no tiene `desde` y además toma los documentos anteriores a la marca CDC.
- Las cuatro tablas MySQL se leen dentro de un mismo `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY`. Así un pedido y sus ítems nunca quedan partidos entre dos lotes por haberse leído en momentos distintos.
- DocumentDB se lee del primario, porque un secundario con retraso podría no tener todavía documentos de la ventana.

## Cómo se identifica un lote y cuándo avanza la marca de agua

1. **`plan.json`** fija la ventana por entidad y el `base_seq`, que es la versión del estado sobre la que se planificó. Se escribe una sola vez (`If-None-Match: *`), así todos los reintentos del lote leen la misma ventana aunque el reloj haya avanzado.
2. **Un Parquet por entidad**, también con `If-None-Match`. Si un intento falla a mitad, el reintento conserva los archivos que ya estaban completos (S3 escribe cada objeto de forma atómica) y extrae solo los que faltan.
3. **`batch.json` y los manifiestos** registran la fecha de extracción, el identificador del lote, las filas por entidad, los bytes y la ventana.
4. **`state.json`** se actualiza al final con compare-and-swap sobre el ETag (`If-Match`). En ese momento avanzan las marcas de agua, y el lote entra al historial con un `seq` consecutivo.

**Un lote está extraído solo si aparece en el historial de `state.json`.** Sus archivos y su registro no bastan. La carga a Redshift solo toma lotes de ese historial.

## Escenarios

| Escenario | Comportamiento |
| --- | --- |
| Repetir un lote confirmado | Sale con 99 sin consultar las fuentes ni escribir en S3 |
| Fallo a mitad (`DW_FAULT=extraccion_a_mitad`) | La marca no avanza. El reintento reutiliza la ventana y los archivos completos, y lee solo lo que falta |
| Fallo después del registro (`DW_FAULT=extraccion_antes_de_confirmar`) | El reintento solo confirma el estado |
| Lote obsoleto: falló, otro lote avanzó la marca y luego se reintenta | Se rechaza (`StaleBatch`). Sus archivos nunca se cargan porque no están en el historial |
| Dos procesos confirman a la vez | El compare-and-swap deja pasar a uno y el otro reintenta o falla, nunca hay una escritura silenciosa |

## Seguridad

- La extracción usa `etl_lector`, con `GRANT SELECT` en MySQL y el rol `read` en DocumentDB, y TLS obligatorio. Las credenciales maestras son del rol `arq-datos-fuentes`, que el pipeline no puede asumir.
- El rol de pipeline puede escribir en el bucket del DW, pero solo puede borrar en `_control/`. Un error del código no puede borrar raw.
- Ningún archivo de datos se sobrescribe: cada escritura usa `If-None-Match: *`.

## Limitaciones conocidas

- La CDC por marca de tiempo no ve borrados físicos. Las fuentes del proyecto no borran filas: los cambios de estado son UPDATE.
- Una transacción que dure más que el solape podría perder filas. Si eso pasa, se sube `DW_CDC_SOLAPE_SEGUNDOS`.
- En DocumentDB, la marca depende de que el escritor la ponga.

## Diferencia con la versión local

En local, raw se recargaba completo en cada corrida (`DELETE` + `INSERT` en DuckDB). En AWS cada lote trae solo lo que cambió y raw es de solo inserción, con el lote en `_batch_id`. La deduplicación por llave pasa al staging de dbt.
