# Etapa 2: Fuentes iniciales

**Objetivo:** crear las tablas de MySQL y las colecciones de MongoDB, y llenarlas con datos sintéticos que incluyen defectos a propósito.

**Antes de empezar:** la [etapa 1](01_entorno.md) completa, con MySQL y MongoDB en estado `healthy`.

## Comandos

```bash
./scripts/create_source_schema.sh
./scripts/generate_data.sh
```

Sin argumentos, el generador usa estos valores:

| Parámetro | Valor | Qué controla |
| --- | --- | --- |
| `--customer-count` | 50 | Clientes válidos |
| `--product-count` | 25 | Productos válidos |
| `--order-count` | 100 | Pedidos válidos |
| `--review-count` | 150 | Reseñas en MongoDB |
| `--event-count` | 500 | Eventos de navegación en MongoDB |
| `--seed` | 42 | Semilla; con la misma semilla los datos son idénticos |
| `--bad-records` | 2 | Cuántas unidades de defectos inyectar |

Pasa estos argumentos a `generate_data.sh` si quieres otro tamaño. Cambiar el tamaño cambia todos los conteos de las etapas siguientes.

## Qué debes ver

`create_source_schema.sh` termina con `MySQL tables and MongoDB collections are ready.`

`generate_data.sh` imprime `Injected bad rows: 34 (--bad-records=2)...`, y la última línea indica que los datos se generaron.

Conteos esperados en las fuentes con los valores por defecto:

| Fuente | Objeto | Filas |
| --- | --- | --- |
| MySQL | `customers` | 54 |
| MySQL | `products` | 27 |
| MySQL | `orders` | 106 |
| MySQL | `order_items` | 259 |
| MongoDB | `reviews` | 158 |
| MongoDB | `navigation_events` | 508 |

Los conteos incluyen las filas defectuosas, por eso son mayores que 50, 25 y 100. Las 34 filas inyectadas son 32 defectos (4 en `customers`, 2 en `products`, 4 en `orders`, 6 en `order_items`, 8 en `reviews` y 8 en `navigation_events`) más 2 cabeceras de pedido válidas.

## Validar en DBeaver y Compass

En DBeaver, conecta a `ecommerce` y ejecuta:

```sql
SELECT 'customers' AS objeto, COUNT(*) AS filas FROM customers
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items;
```

En Compass, abre la base `ecommerce`. Las colecciones `reviews` y `navigation_events` deben tener 158 y 508 documentos. Abre un documento y observa que `reviews` y `navigation_events` contienen objetos anidados.

Los valores están en español (ciudades, categorías, canales, nombres de producto, descripciones y textos de reseña), pero las tablas y las columnas están en inglés.

## Archivos implicados

| Archivo | Función |
| --- | --- |
| [src/ecommerce_dw/sources/mysql/schema.sql](../../src/ecommerce_dw/sources/mysql/schema.sql) | DDL de `customers`, `products`, `orders` y `order_items` |
| [scripts/create_source_schema.sh](../../scripts/create_source_schema.sh) | Aplica el DDL y crea las colecciones de Mongo si faltan |
| [scripts/generate_data.sh](../../scripts/generate_data.sh) | Ejecuta el generador dentro de la imagen de Airflow |
| [src/ecommerce_dw/sources/generate_data.py](../../src/ecommerce_dw/sources/generate_data.py) | Genera datos con Faker (`es_CO`) e inyecta defectos (`build_bad_records`) |

No hay DAGs en esta etapa.

## Defectos inyectados

Cada unidad de `--bad-records` genera 17 filas: 16 defectos y una cabecera de pedido válida que aísla un defecto de subtotal. Con el valor 2 se inyectan 34 filas: 32 defectos y 2 cabeceras válidas. Los defectos los rechazará la [etapa 4](04_calidad_cuarentena.md).

## Dónde cambiar

- Tamaño y semilla: argumentos de `generate_data.sh` (ver tabla).
- Tipos de defectos: `build_bad_records` en [generate_data.py](../../src/ecommerce_dw/sources/generate_data.py).
- Estructura de las fuentes: [schema.sql](../../src/ecommerce_dw/sources/mysql/schema.sql) para MySQL, y los validadores JSON Schema de `reviews` y `navigation_events` en [generate_data.py](../../src/ecommerce_dw/sources/generate_data.py). Ahí los validadores se crean con `validationLevel` `strict`.

## Idempotencia

- `create_source_schema.sh` es idempotente: usa `CREATE TABLE IF NOT EXISTS` y crea las colecciones solo si faltan.
- `generate_data.sh` es **destructivo**: borra las filas y las colecciones antes de regenerar. Vuelve a producir el mismo resultado con la misma semilla, pero también borra los lotes incrementales de la [etapa 9](09_carga_incremental.md).

## Siguiente etapa

[03_extraccion_raw.md](03_extraccion_raw.md)
