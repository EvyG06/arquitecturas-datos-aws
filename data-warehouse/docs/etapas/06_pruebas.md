# Etapa 6: Pruebas de calidad con dbt

**Objetivo:** comprobar que el modelo estrella cumple las reglas de integridad y de negocio antes de darlo por bueno.

**Antes de empezar:** la [etapa 5](05_modelo_estrella.md) completa.

## Comandos

Ejecuta todo el flujo de dbt, que termina con las pruebas:

```bash
./scripts/run_dbt.sh
```

El script ejecuta, en orden: `deps`, `seed`, `run` de intermediate, `snapshot`, `run` completo y `test`. Termina con `dbt workflow completed successfully.` solo si todas las pruebas pasan.

Para correr solo las pruebas desde Airflow, usa la tarea `dbt_test` del DAG `transform_warehouse`.

## Qué debes ver

Al final de `dbt test` aparece un resumen del estilo `Done. PASS=92 WARN=0 ERROR=0 SKIP=0 TOTAL=92`. Con el proyecto actual, el resultado verificado es PASS=92, sin errores.

Qué prueban:

| Tipo | Ejemplos |
| --- | --- |
| Llaves únicas y no nulas | `unique` y `not_null` en las llaves subrogadas de las dimensiones y hechos |
| Integridad referencial | `relationships` de cada hecho hacia cada dimensión (`customer_sk`, `product_sk`, `date_sk`, etc.) |
| Valores permitidos | `accepted_values` donde aplica |
| Cobertura de fechas | `assert_dim_date_covers_configured_range.sql` y `assert_every_year_has_holidays.sql` |
| Zona horaria de Mongo | `assert_mongo_facts_use_bogota_business_date.sql` |
| Conciliación con la fuente | `assert_sales_totals_match_source_by_business_date.sql` y `assert_sales_dates_match_raw_order_dates.sql` |

## Validar en DBeaver

Cero hechos de reseñas huérfanos hacia clientes:

```sql
SELECT COUNT(*) AS huerfanos
FROM marts.fct_reviews r
LEFT JOIN marts.dim_customer c USING (customer_sk)
WHERE c.customer_sk IS NULL;
```

Totales de ventas por fecha de negocio, para comparar con MySQL (las pruebas de conciliación hacen la misma comparación):

```sql
SELECT d.date AS fecha, ROUND(SUM(f.subtotal), 2) AS total
FROM marts.fct_sales f
JOIN marts.dim_date d USING (date_sk)
GROUP BY 1
ORDER BY 1
LIMIT 20;
```


## Dónde cambiar

- Las pruebas genéricas se declaran en los archivos `.yml` de cada carpeta de modelos.
- Las pruebas personalizadas están en [src/dbt/tests/](../../src/dbt/tests/).

## Idempotencia

Las pruebas solo leen datos. Ejecutarlas varias veces no cambia el warehouse.

## Siguiente etapa

[07_idempotencia.md](07_idempotencia.md)
