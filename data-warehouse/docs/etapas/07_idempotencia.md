# Etapa 7: Idempotencia

**Objetivo:** comprobar que volver a ejecutar el pipeline sin cambios en las fuentes deja el warehouse igual.

**Antes de empezar:** la [etapa 6](06_pruebas.md) completa. Cierra DBeaver antes de ejecutar.

## Comandos

```bash
./scripts/run_pipeline.sh --existing-sources
./scripts/run_pipeline.sh --existing-sources
```

`--existing-sources` no regenera los datos de MySQL ni de MongoDB: solo dispara `ecommerce_pipeline` con las fuentes que ya existen. Cada corrida extrae a raw, reconstruye los modelos y ejecuta las pruebas.

Este flag no acepta opciones de generación; si lo combinas con `--customer-count` u otra, el script termina con error.

## Qué debes ver

Después de cada corrida, `ecommerce_pipeline` aparece en `success` en Airflow. Los conteos de las consultas de abajo son los mismos antes y después de repetir.

Conteos de referencia con los valores por defecto (sin lotes ni cambios):

| Tabla | Filas |
| --- | --- |
| `raw.mysql_orders` | 106 |
| `raw.mongo_reviews` | 158 |
| `marts.fct_sales` | 253 |
| `marts.fct_reviews` | 150 |
| `marts.fct_navigation_events` | 500 |

Si ya aplicaste cambios de las etapas 8 o 9, los conteos son otros; lo que importa es que no cambien entre corridas.

## Validar en DBeaver

Huella rápida del hecho de ventas. Ejecútala antes y después de repetir el pipeline; debe devolver lo mismo:

```sql
SELECT COUNT(*) AS filas, COUNT(DISTINCT sale_sk) AS llaves, ROUND(SUM(subtotal), 2) AS total
FROM marts.fct_sales;
```

Esta comparación fue verificada en el proyecto: repetir `run_pipeline.sh --existing-sources` no cambió ningún conteo.

## Por qué es idempotente

- **raw:** cada carga borra la tabla y la vuelve a llenar en una transacción.
  La columna `_loaded_at` guarda la hora de cada ejecución, así que cambia en cada corrida y se propaga a quarantine. Por eso las huellas se calculan sobre columnas de negocio, no sobre `_loaded_at`.
- **intermediate y quarantine:** se recalculan completos en cada corrida.
- **Dimensiones:** se reconstruyen; las llaves subrogadas son deterministas (se generan con `generate_surrogate_key` a partir de datos estables).
- **Hechos incrementales:** usan `delete+insert` por llave subrogada, así que reprocesar una ventana de fechas reemplaza filas en lugar de duplicarlas (ver [etapa 9](09_carga_incremental.md)).

## Archivos implicados

| Archivo | Función |
| --- | --- |
| [scripts/run_pipeline.sh](../../scripts/run_pipeline.sh) | Flag `--existing-sources` |
| [src/ecommerce_dw/ingestion/raw_ingestion.py](../../src/ecommerce_dw/ingestion/raw_ingestion.py) | Carga raw en transacción |
| [src/dbt/macros/incremental.sql](../../src/dbt/macros/incremental.sql) | Estrategia incremental de los hechos |

## Dónde cambiar

- Si agregas un modelo que escriba en DuckDB, respeta la misma regla: reconstruir completo o usar `delete+insert` con una llave estable.

## Siguiente etapa

[08_cambios_scd2.md](08_cambios_scd2.md)
