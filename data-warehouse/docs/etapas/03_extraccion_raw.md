# Etapa 3: Extracción a raw

**Objetivo:** copiar las seis fuentes a DuckDB, sin transformar, en el esquema `raw`.

**Antes de empezar:** la [etapa 2](02_fuentes_iniciales.md) completa. **Cierra DBeaver** antes de ejecutar: DuckDB admite un solo escritor a la vez.

## Comandos

Opción A, disparar solo la extracción desde la CLI de Airflow:

```bash
docker compose --project-directory . -f docker/docker-compose.yml exec -T airflow-scheduler airflow dags trigger extract_to_raw
docker compose --project-directory . -f docker/docker-compose.yml exec -T airflow-scheduler airflow dags list-runs -d extract_to_raw
```

Opción B, desde la interfaz: abre <http://localhost:8080>, activa `extract_to_raw` y pulsa **Trigger DAG**.

La corrida tarda entre 6 y 10 segundos. En `list-runs`, el estado debe ser `success`.

Para ejecutar todo el flujo de una vez, usa `./scripts/run_pipeline.sh`; la etapa 3 es la parte que dispara `extract_to_raw` dentro de él.

## Qué debes ver

En la vista **Graph** del DAG `extract_to_raw` se ven dos TaskGroups, `mysql` y `mongodb`. Cada uno tiene `extract` y `load_raw`. Las dos extracciones pueden correr en paralelo; las cargas van en serie, y al final corre `verify_raw_counts`.

Carpeta de artefactos: `warehouse/extracts/manual__<fecha>/` (o `scheduled__...`) contiene seis CSV, uno por fuente. Ninguna tarea borra esta carpeta; `reset_environment.sh` sí la elimina.

Conteos esperados en `raw` con los valores por defecto (iguales a las fuentes, porque raw copia todo, incluidas las filas defectuosas):

| Tabla raw | Filas |
| --- | --- |
| `raw.mysql_customers` | 54 |
| `raw.mysql_products` | 27 |
| `raw.mysql_orders` | 106 |
| `raw.mysql_order_items` | 259 |
| `raw.mongo_reviews` | 158 |
| `raw.mongo_navigation_events` | 508 |

## Validar en DBeaver

1. Crea una conexión DuckDB apuntando a `warehouse/warehouse.duckdb`. En WSL, la ruta está en [docs/connections.md](../connections.md). Abre la conexión en modo solo lectura.
2. Ejecuta:

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'raw'
ORDER BY table_name;
```

Deben aparecer seis tablas.

3. Compara con la fuente. Por ejemplo:

```sql
SELECT COUNT(*) AS filas, COUNT(DISTINCT order_id) AS pedidos_distintos
FROM raw.mysql_orders;
```

Con los valores por defecto debe devolver 106 filas. Si ya aplicaste el lote incremental de la [etapa 9](09_carga_incremental.md), la cifra será 109.

Las columnas de raw conservan el nombre de la fuente y no tienen transformación. En las tablas de Mongo hay columnas tipadas para los campos principales, además de `document_json` (tipo JSON, con el documento completo, incluidos los objetos anidados), `_loaded_at` (momento de la carga) y `_source` (colección de origen). Las etapas siguientes convierten y limpian estos datos.

## Archivos implicados

| Archivo | Función |
| --- | --- |
| [src/airflow/dags/extract_to_raw.py](../../src/airflow/dags/extract_to_raw.py) | Define el DAG: TaskGroups, XCom y `verify_raw_counts` |
| [src/ecommerce_dw/ingestion/airflow_tasks.py](../../src/ecommerce_dw/ingestion/airflow_tasks.py) | Funciones que llaman los operadores (`DUCKDB_PATH`, carpeta de artefactos) |
| [src/ecommerce_dw/ingestion/raw_ingestion.py](../../src/ecommerce_dw/ingestion/raw_ingestion.py) | Extrae a CSV y carga en DuckDB con `DELETE` + inserción en una transacción |
| [src/ecommerce_dw/ingestion/raw_sources.py](../../src/ecommerce_dw/ingestion/raw_sources.py) | Lista de tablas raw y sus columnas |
| [src/airflow/dags/ecommerce_pipeline.py](../../src/airflow/dags/ecommerce_pipeline.py) | DAG maestro que dispara esta extracción |

## Dónde cambiar

- Qué columnas o colecciones se copian: [raw_sources.py](../../src/ecommerce_dw/ingestion/raw_sources.py).
- Cómo se conectan las extracciones y las cargas: [extract_to_raw.py](../../src/airflow/dags/extract_to_raw.py).
- Ruta de DuckDB dentro del contenedor: variable `DUCKDB_PATH` en `airflow_tasks.py`. El archivo es `warehouse/warehouse.duckdb` en el host (montaje `./warehouse:/opt/warehouse`).

## Idempotencia

Cada carga de raw borra la tabla y la vuelve a llenar dentro de una transacción, así que repetirla no duplica filas. Esto es una carga completa, no incremental: la [etapa 9](09_carga_incremental.md) explica por qué el incremental vive en dbt.

Para comprobarlo, dispara `extract_to_raw` dos veces más y compara los conteos de la tabla de arriba antes y después. En la verificación de este proyecto, las dos corridas adicionales dejaron los mismos conteos.

## Siguiente etapa

[04_calidad_cuarentena.md](04_calidad_cuarentena.md)
