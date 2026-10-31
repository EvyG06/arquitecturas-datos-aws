# Etapa 1: Entorno

**Objetivo:** levantar los servicios con Docker Compose y confirmar que Airflow y las bases de origen responden.

**Antes de empezar:** Docker Desktop (o Docker Engine con Compose v2) en ejecución y los puertos del [README](../../README.md) libres.

## Comandos

Desde la raíz del repositorio:

```bash
./scripts/configure_environment.sh
./scripts/start_infrastructure.sh
docker compose --project-directory . -f docker/docker-compose.yml ps
```

`configure_environment.sh` crea `.env` a partir de `docker/.env.example`. Si `.env` ya existe, conserva sus valores. `start_infrastructure.sh` construye la imagen de Airflow y levanta los servicios en segundo plano.

## Qué debes ver

- `configure_environment.sh` imprime `Created .env from docker/.env.example...` la primera vez, o `Environment file already exists; keeping current values.` si ya existía.
- `ps` muestra `airflow-scheduler`, `airflow-webserver`, `mongodb`, `mysql` y `postgres` en estado `running`. Los tres servicios con healthcheck (`mysql`, `mongodb` y `postgres`) pasan a `healthy` en unos segundos.
- Airflow responde en <http://localhost:8080>. Usuario y contraseña son `AIRFLOW_ADMIN_USERNAME` y `AIRFLOW_ADMIN_PASSWORD` de `.env`.
- Los DAGs `extract_to_raw`, `transform_warehouse` y `ecommerce_pipeline` aparecen en la lista. Todavía no tienen ejecuciones.

## Validar conexiones

Usa [docs/connections.md](../connections.md) para configurar:

- **DBeaver, MySQL:** host `127.0.0.1`, puerto `3306`, base `ecommerce`, usuario `MYSQL_USER` y contraseña `MYSQL_PASSWORD` de `.env`. Al conectar debes ver la base `ecommerce`. Las tablas aparecen en la etapa 2.
- **Compass, MongoDB:** cadena `mongodb://127.0.0.1:27017/` con el usuario `MONGO_ROOT_USERNAME` y la contraseña `MONGO_ROOT_PASSWORD` de `.env`, con autenticación en `admin`.
- **DuckDB:** todavía no existe. El archivo `warehouse/warehouse.duckdb` se crea en la etapa 3.

## Archivos implicados

| Archivo | Función en esta etapa |
| --- | --- |
| [docker/docker-compose.yml](../../docker/docker-compose.yml) | Define los servicios, puertos, healthchecks y variables |
| [docker/Dockerfile](../../docker/Dockerfile) | Imagen de Airflow con Python, dbt y dbt_utils |
| [docker/.env.example](../../docker/.env.example) | Plantilla de variables. Aquí cambias puertos y valores por defecto |
| [scripts/configure_environment.sh](../../scripts/configure_environment.sh) | Crea `.env` |
| [scripts/start_infrastructure.sh](../../scripts/start_infrastructure.sh) | Ejecuta `docker compose up --build -d` |
| [docs/connections.md](../connections.md) | Parámetros de conexión para DBeaver y Compass |

No hay DAGs ni modelos dbt que ejecutar todavía.

## Dónde cambiar

- Puertos: `AIRFLOW_UI_PORT` y `POSTGRES_HOST_PORT` en `.env`. Si cambias el puerto de MySQL o MongoDB, actualiza también [docs/connections.md](../connections.md).
- Servicios y volúmenes: [docker/docker-compose.yml](../../docker/docker-compose.yml).

## Idempotencia

`configure_environment.sh` no sobrescribe `.env`. `start_infrastructure.sh` puede repetirse: Compose solo recrea lo que cambió.

## Siguiente etapa

[02_fuentes_iniciales.md](02_fuentes_iniciales.md)
