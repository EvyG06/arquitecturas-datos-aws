# Etapa 9: Carga incremental por lote

**Objetivo:** agregar un lote diario de datos nuevos a las fuentes y comprobar que los tres hechos se actualizan solo con esas filas, sin duplicar nada.

**Antes de empezar:** la [etapa 8](08_cambios_scd2.md) completa. Cierra DBeaver antes de ejecutar.

## Comandos

```bash
./scripts/verify_incremental_load.sh --batch-id 2
```

El script:

1. Toma una línea base de los hechos.
2. Agrega un lote de datos nuevos a MySQL y MongoDB con `append_batch.sh`.
3. Ejecuta `run_pipeline.sh --existing-sources`.
4. Comprueba que cada hecho creció en 3 filas.
5. Repite el mismo lote y comprueba que nada cambia.

Tarda unos 3 minutos.

**Elige un número de lote que no hayas usado.** Va de 1 a 365. Si el lote ya existe, `append_batch.py` se detiene. Esta guía usa el lote 2 como ejemplo. Si ya lo aplicaste en tu warehouse, elige otro número.

Para agregar un lote sin verificación:

```bash
./scripts/append_batch.sh --batch-id 2
./scripts/run_pipeline.sh --existing-sources
```

Cada lote tiene una fecha de negocio: `2026-01-01` más `batch-id − 1` días. Por eso el lote 2 es del 2 de enero de 2026.

## Qué debes ver

Al final de `verify_incremental_load.sh`, cada hecho muestra el aumento de filas. Con un lote nuevo, `fct_sales`, `fct_reviews` y `fct_navigation_events` deben crecer en 3 cada uno. Al repetir el lote, los conteos no cambian.

Valores verificados en el proyecto con el lote 2 sobre el warehouse inicial:

| Hecho | Antes | Después |
| --- | --- | --- |
| `fct_sales` | 253 | 256 |
| `fct_reviews` | 150 | 153 |
| `fct_navigation_events` | 500 | 503 |

## Validar en DBeaver

Fechas del último lote, para confirmar que solo llegaron datos nuevos:

```sql
SELECT MIN(d.date) AS desde, MAX(d.date) AS hasta, COUNT(*) AS filas
FROM marts.fct_sales f
JOIN marts.dim_date d USING (date_sk)
WHERE d.date >= DATE '2026-01-01';
```

Filas de cada lote en raw (con el lote 2 aplicado, `raw.mysql_orders` tiene 109 filas en lugar de 106):

```sql
SELECT COUNT(*) AS filas FROM raw.mysql_orders;
```

## Cómo funciona la carga incremental

- **Qué se procesa:** `incremental_candidate_filter` selecciona tres grupos: llaves nuevas; filas cuya fecha de negocio es mayor o igual que el máximo cargado menos `incremental_lookback_days` (7 días), que permite reprocesar datos que llegaron tarde; y filas cuyas llaves de cliente, producto o geografía cambiaron por un cambio SCD2 (por ejemplo, las ventas anteriores a un cambio de ciudad se reapuntan a la versión vigente en su fecha).
- **Cómo se escribe:** `delete+insert` por llave subrogada. Las filas de la ventana se borran y se vuelven a insertar; no se duplican.
- **Huérfanos:** después de cada corrida, `delete_orphaned_facts` borra los hechos cuya llave subrogada ya no existe en la capa validada.
- **Por qué no se incremental raw:** raw siempre se recarga completo. La lógica incremental vive en dbt, para que sea fácil de explicar y probar.

## Cuándo usar full refresh

Si cambias la fórmula de una llave subrogada, los hechos ya cargados conservan llaves viejas que la ventana incremental no toca. En ese caso reconstruye todo:

```bash
docker compose --project-directory . -f docker/docker-compose.yml exec -T airflow-scheduler /home/airflow/dbt-venv/bin/dbt run --full-refresh --select marts --project-dir /opt/airflow/src/dbt --profiles-dir /opt/airflow/src/dbt
```

La sección de carga incremental de [data_model.md](../data_model.md) explica este caso con el cambio de llaves de clientes y productos.

## Archivos implicados

| Archivo | Función |
| --- | --- |
| [scripts/verify_incremental_load.sh](../../scripts/verify_incremental_load.sh) | Lote, verificación de +3 y repetición sin cambios |
| [scripts/append_batch.sh](../../scripts/append_batch.sh) | Entrada del script de lote |
| [src/ecommerce_dw/sources/append_batch.py](../../src/ecommerce_dw/sources/append_batch.py) | Genera el lote con IDs estables (`BASE_ID + batch_id × 10 + índice`) |
| [src/dbt/macros/incremental.sql](../../src/dbt/macros/incremental.sql) | Filtro de candidatos y borrado de huérfanos |
| [src/dbt/models/marts/fct_sales.sql](../../src/dbt/models/marts/fct_sales.sql) | Hecho incremental de ventas |
| [src/dbt/models/marts/fct_reviews.sql](../../src/dbt/models/marts/fct_reviews.sql) | Hecho incremental de reseñas |
| [src/dbt/models/marts/fct_navigation_events.sql](../../src/dbt/models/marts/fct_navigation_events.sql) | Hecho incremental de eventos |
| [src/dbt/dbt_project.yml](../../src/dbt/dbt_project.yml) | Variables `incremental_lookback_days` y `business_timezone` |

## Dónde cambiar

- Tamaño de la ventana de reproceso: `incremental_lookback_days` en `dbt_project.yml`.
- Fecha de negocio de cada lote: `append_batch.py`.
- Estrategia de un hecho: el bloque `config` de cada `fct_*.sql`.

## Idempotencia

- Repetir el mismo lote no cambia ningún conteo; el script lo verifica.
- `run_pipeline.sh --existing-sources` conserva los lotes ya agregados.
- Sin `--existing-sources`, `generate_data.sh` regenera las fuentes desde cero y borra los lotes.

## Siguiente etapa

Fin de la ruta. Vuelve al [README](../../README.md) para revisar la documentación de referencia de [arquitectura](../architecture.md), [modelo de datos](../data_model.md) y [Airflow](../airflow.md).
