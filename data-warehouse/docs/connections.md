# Conexión a las bases de datos

Ejecuta `./scripts/configure_environment.sh` y `./scripts/run_pipeline.sh` desde la raíz antes de abrir los clientes. Consulta los usuarios y contraseñas en el archivo local `.env`; no uses los valores de la plantilla si los cambiaste. MySQL y MongoDB publican puertos en el equipo anfitrión; PostgreSQL publica un puerto solo en `127.0.0.1`. DuckDB se abre como archivo.

## MySQL en DBeaver

Crea una conexión **MySQL** con estos valores:

| Campo | Valor |
| --- | --- |
| Host | `127.0.0.1` |
| Puerto | `3306` |
| Base de datos | `MYSQL_DATABASE` de `.env` |
| Usuario | `MYSQL_USER` de `.env` |
| Contraseña | `MYSQL_PASSWORD` de `.env` |

Pulsa **Test Connection**; DBeaver puede solicitar la descarga del controlador JDBC. Luego consulta `customers`, `products`, `orders` y `order_items`. Por ejemplo: `SELECT COUNT(*) FROM orders;`. [Guía de DBeaver para MySQL](https://dbeaver.com/docs/dbeaver/Database-driver-MySQL/).

## MongoDB en Compass

Crea una conexión a `mongodb://127.0.0.1:27017/`. En **Advanced Connection Options → Authentication**, selecciona **Username / Password** y usa `MONGO_ROOT_USERNAME` y `MONGO_ROOT_PASSWORD` de `.env`. Define **Authentication Database** como `admin`; es la base donde se autentica el usuario, distinta de `MONGO_DATABASE`, que contiene `reviews` y `navigation_events`. Pulsa **Connect** y abre la base indicada por `MONGO_DATABASE`. [Guía de autenticación de Compass](https://www.mongodb.com/docs/compass/connect/advanced-connection-options/authentication-connection/).

## DuckDB en DBeaver

Crea una conexión **DuckDB** y selecciona el archivo `warehouse/warehouse.duckdb` de este repositorio. El archivo aparece después de la primera ejecución del flujo. Si DBeaver corre en Windows y el proyecto está en WSL, busca el archivo mediante `\\wsl.localhost\<distribución>\home\<usuario>\<ruta-del-repositorio>\warehouse\warehouse.duckdb` y sustituye `<distribución>` por el nombre de tu distribución, `<usuario>` por tu usuario de Linux y `<ruta-del-repositorio>` por la ruta donde clonaste este proyecto dentro de WSL. DBeaver puede solicitar la descarga del controlador JDBC. [Guía de DuckDB para DBeaver](https://duckdb.org/docs/current/guides/sql_editors/dbeaver).

Consulta, por ejemplo, `SELECT COUNT(*) FROM marts.fct_sales;`. Para inspeccionar sin escribir, configura la propiedad del controlador `duckdb.read_only=true`. Desconecta DBeaver antes de ejecutar el flujo: DuckDB no permite que el proceso de Airflow escriba mientras otro proceso mantiene abierto el archivo, incluso en modo de solo lectura. [Conexión de solo lectura](https://duckdb.org/docs/current/clients/java/connecting) y [concurrencia de DuckDB](https://duckdb.org/docs/current/connect/concurrency).

## PostgreSQL de Airflow en DBeaver

Crea una conexión **PostgreSQL** con estos valores:

| Campo | Valor |
| --- | --- |
| Host | `127.0.0.1` |
| Puerto | `POSTGRES_HOST_PORT` de `.env`, o `5433` si no está definido |
| Base de datos | `airflow` |
| Usuario | `POSTGRES_USER` de `.env` |
| Contraseña | `POSTGRES_PASSWORD` de `.env` |

Pulsa **Test Connection**; DBeaver puede solicitar el controlador JDBC. Esta base guarda DAG runs, estados de tareas y otros metadatos de Airflow, no ventas ni tablas del warehouse. Para inspeccionarla, ejecuta `SELECT COUNT(*) FROM dag_run;`. El puerto queda limitado a la máquina local; si cambias `POSTGRES_HOST_PORT`, ejecuta `./scripts/start_infrastructure.sh` para aplicar la nueva publicación.
