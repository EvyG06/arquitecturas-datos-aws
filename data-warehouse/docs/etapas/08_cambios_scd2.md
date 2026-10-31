# Etapa 8: Cambios en la fuente y dimensiones SCD tipo 2

**Objetivo:** simular que un cliente cambia de ciudad y un producto cambia de categoría, y comprobar que el warehouse conserva la historia en lugar de sobrescribirla.

**Antes de empezar:** la [etapa 7](07_idempotencia.md) completa. Cierra DBeaver antes de ejecutar.

## Comandos

```bash
./scripts/simulate_changes.sh --customer-id 1 --city Medellín --department Antioquia --product-id 1 --category Hogar --changed-at 2025-07-01
./scripts/run_pipeline.sh --existing-sources
```

`simulate_changes.sh` modifica MySQL y escribe la fecha de `--changed-at` en `updated_at`. Después, el pipeline extrae el cambio y los snapshots crean una versión nueva.

Si repites el mismo cambio con la misma fecha, no se crean versiones nuevas.

## Qué debes ver

Historia de cliente 1, con los valores de la verificación del proyecto:

| customer_id | city | valid_from | valid_to | is_current |
| --- | --- | --- | --- | --- |
| 1 | Bogotá | 2024-11-10 | 2025-07-01 | false |
| 1 | Medellín | 2025-07-01 | (vacío) | true |

Historia de producto 1:

| product_id | category | valid_from | valid_to | is_current |
| --- | --- | --- | --- | --- |
| 1 | Electrónica | 2025-01-01 | 2025-07-01 | false |
| 1 | Hogar | 2025-07-01 | (vacío) | true |

Las fechas de `valid_from` de la primera versión dependen de la fecha de la fuente, así que pueden variar si cambias la semilla o el tamaño de los datos.

## Validar en DBeaver

Historia de cada entidad cambiada:

```sql
SELECT customer_id, city, department, valid_from, valid_to, is_current
FROM marts.dim_customer
WHERE customer_id = 1
ORDER BY valid_from;

SELECT product_id, category, valid_from, valid_to, is_current
FROM marts.dim_product
WHERE product_id = 1
ORDER BY valid_from;
```

Solo debe haber una fila con `is_current = true` por entidad:

```sql
SELECT customer_id, COUNT(*) AS vigentes
FROM marts.dim_customer
WHERE is_current
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

La consulta anterior debe devolver cero filas.

Para ver cómo un hecho apunta a la versión correcta según la fecha del pedido:

```sql
SELECT f.sale_sk, f.customer_sk, c.city, d.date
FROM marts.fct_sales f
JOIN marts.dim_customer c USING (customer_sk)
JOIN marts.dim_date d USING (date_sk)
WHERE c.customer_id = 1
ORDER BY d.date;
```

Las ventas anteriores a 2025-07-01 deben apuntar a la versión de Bogotá, y las posteriores a la de Medellín.

Si las ventas ya estaban cargadas antes del cambio, `fct_sales` las reapunta en la siguiente corrida incremental, aunque caigan fuera de la ventana de reproceso (ver [etapa 9](09_carga_incremental.md)).

## Por qué funciona

- El snapshot usa `strategy='timestamp'` sobre `updated_at`. Un cambio con fecha nueva genera una fila nueva y cierra la anterior.
- Las llaves subrogadas incluyen `dbt_valid_from`, así que cada versión tiene su propia llave. Sin esa corrección, la versión nueva colisionaba con la anterior.
- `fct_sales` elige la versión vigente en la fecha del pedido con la macro `scd2_point_in_time_condition`.

## Archivos implicados

| Archivo | Función |
| --- | --- |
| [scripts/simulate_changes.sh](../../scripts/simulate_changes.sh) | Entrada del script de cambios |
| [src/ecommerce_dw/sources/simulate_changes.py](../../src/ecommerce_dw/sources/simulate_changes.py) | Aplica el cambio a MySQL con `updated_at` |
| [src/dbt/snapshots/snap_dim_customer.sql](../../src/dbt/snapshots/snap_dim_customer.sql) | Snapshot de clientes |
| [src/dbt/snapshots/snap_dim_product.sql](../../src/dbt/snapshots/snap_dim_product.sql) | Snapshot de productos |
| [src/dbt/macros/scd2.sql](../../src/dbt/macros/scd2.sql) | Versión vigente por fecha |
| [src/dbt/models/marts/dim_customer.sql](../../src/dbt/models/marts/dim_customer.sql) | Llave subrogada por versión |

## Dónde cambiar

- Qué cambia en la simulación: argumentos de [simulate_changes.py](../../src/ecommerce_dw/sources/simulate_changes.py).
- Qué columnas se historizan: la lista de columnas en los snapshots.

## Idempotencia

Repetir el mismo cambio con la misma fecha no crea versiones nuevas. Para ver el efecto de otro cambio, usa una fecha distinta.

## Siguiente etapa

[09_carga_incremental.md](09_carga_incremental.md)
