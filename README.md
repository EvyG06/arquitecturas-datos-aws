# Arquitecturas de datos en AWS

Actividad de Ingeniería de Datos 2026-2 (Universidad EIA). 
Integrantes: Pablo Restrepo y Evelyn Gómez

- **Data lake de noticias.** Tres feeds RSS de El Tiempo a una arquitectura medallón en S3, con Comprehend, Glue y Athena. [Documentación](docs/dl/README.md).
- **Data warehouse de e-commerce.** El DW local de DE-DW migrado a RDS for MySQL, DocumentDB, S3, Redshift Serverless, dbt y QuickSight. [Documentación](docs/dw/README.md).

| Documento | Contenido |
|---|---|
| [docs/dl/README.md](docs/dl/README.md) | Arquitectura, fuentes, reglas de calidad e identificación, incrementalidad, recuperación, Glue, Athena y escenarios del data lake |
| [docs/dl/diccionario.md](docs/dl/diccionario.md) | Diccionario de datos de Silver y Gold |
| [docs/dw/README.md](docs/dw/README.md) | Arquitectura, despliegue, ejecución, escenarios, decisiones y diferencias con la versión local del DW |
| [docs/dw/02_extraccion.md](docs/dw/02_extraccion.md) | Extracción por lote, marca de agua y recuperación |
| [docs/costos.md](docs/costos.md) | Estimación de costos y gasto real |
| [docs/seguridad.md](docs/seguridad.md) | Controles de seguridad |
| [orquestacion/.env.example](orquestacion/.env.example) | Variables requeridas, sin secretos, y parámetros de SSM |

## Estructura

| Carpeta | Contenido |
|---|---|
| `infra/` | Presupuesto, IAM, EC2, scripts del DW por fase, encendido, apagado, costos y eliminación |
| `orquestacion/` | Imagen de Airflow, Docker Compose, los dos DAGs y los comandos que corren en la EC2 |
| `data-lake/` | Código (`src/dl`), configuración de fuentes, consultas de Athena, infraestructura, evidencias y pruebas |
| `data-warehouse/` | Código del DW (`src/ecommerce_dw`), proyecto dbt, scripts de prueba con DuckDB y pruebas |
| `evidencias/` | Salida de cada escenario, generada por los scripts |
| `docs/` | Documentación |

## Requisitos

- Un usuario IAM con permisos de administrador. Los scripts de IAM se niegan a correr con root o con un rol.
- AWS CLI v2 y el plugin de Session Manager en el Mac.
- Un entorno conda `arq-aws` con Python 3.11 o superior y `pip install -r data-lake/requirements.txt -r data-warehouse/requirements-aws.txt`.
- En `~/.aws/config` dos perfiles. `admin` con el usuario IAM y `pipeline`, que asume `arq-datos-pipeline` desde `admin` (`role_arn` y `source_profile = admin`).
- El alias `arq`, que deja la terminal lista para todos los scripts. Se escribe solo, en su propia línea.

```bash
alias arq='conda activate arq-aws && cd ~/arquitecturas-datos-aws && source infra/env.sh'
```

`infra/env.sh` fija la región, el perfil `admin`, la cuenta, los nombres de los buckets, `PYTHONPATH` y el id de la EC2 de Airflow.

## Despliegue de la base común

Los scripts son idempotentes. Se corren en este orden desde la raíz del repositorio, después de `arq`.

```bash
# 1. Presupuesto mensual de 40 USD sin contar creditos. notifications.json lleva tu correo y no se versiona.
aws budgets create-budget --account-id "$ACCOUNT_ID" --budget file://infra/budget/budget.json \
  --notifications-with-subscribers file://infra/budget/notifications.json

# 2. Roles. El de la EC2 vuelve a correr el de pipeline para agregarse a su relacion de confianza.
./infra/iam/01_pipeline_role.sh
./infra/iam/02_ec2_role.sh

# 3. Data lake: bucket, workgroup de Athena y catalogo
./data-lake/infra/01_bucket.sh
./data-lake/infra/02_athena_workgroup.sh
python -m dl.catalog --setup

# 4. EC2 de Airflow sin puertos de entrada, con disco cifrado e IMDSv2 obligatorio
./infra/ec2/01_instancia.sh
source infra/env.sh                                     # toma el id de la instancia nueva
./infra/ec2/remoto.sh "$(cat infra/ec2/preparar_github.sh)"
# Registrar la llave publica que imprime el paso anterior como Deploy key de SOLO LECTURA en GitHub. Despues:
./infra/ec2/remoto.sh 'git clone git@github.com:EvyG06/arquitecturas-datos-aws.git && arquitecturas-datos-aws/orquestacion/setup_ec2.sh'
./infra/ec2/remoto.sh 'arquitecturas-datos-aws/orquestacion/verificar_ec2.sh'
```

Después, el DW se despliega con los pasos de [docs/dw/README.md](docs/dw/README.md#despliegue).

## Operación diaria

| Acción | Comando |
|---|---|
| Encender EC2, RDS y DocumentDB y esperar a que respondan | `./infra/encender.sh` |
| Apagar lo que cobra por hora | `./infra/detener.sh` |
| Correr un comando en la EC2 | `./infra/ec2/remoto.sh '<comando>'` (Session Manager, devuelve el código de salida remoto) |
| Interfaz de Airflow | `aws ssm start-session --target "$AIRFLOW_INSTANCE" --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8080` y abrir http://localhost:8080 |
| Contraseña del admin de Airflow | `aws ssm start-session --target "$AIRFLOW_INSTANCE"` y en esa sesión `grep AIRFLOW_ADMIN_PASSWORD ~/arquitecturas-datos-aws/orquestacion/.env` |
| Costos | `python infra/costos.py` |
| Pruebas | `python -m pytest data-lake/tests infra/tests -q` y `PYTHONPATH=data-warehouse/src python -m pytest data-warehouse/tests -q` |

La contraseña se lee en una sesión interactiva y no con `remoto.sh`, porque la salida de `send-command` queda guardada en SSM.

RDS y DocumentDB se encienden solos si pasan 7 días detenidos. Si el laboratorio se pausa más de una semana, hay que volver a correr `./infra/detener.sh` o eliminar todo.

## Demostración

| Proyecto | Comando | Qué muestra |
|---|---|---|
| Data lake | `./data-lake/evidencias/demo.sh` | Ingesta completa programada, indicadores en Athena, reproceso sin duplicar, fallo controlado y recuperación, limitación de Comprehend |
| Data warehouse | `./infra/dw/demo.sh 9` | Un lote de negocio nuevo desde MySQL y DocumentDB hasta el dashboard |
| Data warehouse | `./infra/dw/demo.sh 10 --fallo` | Lo mismo, con un fallo en la carga, cómo se identifica y cómo se recupera |

Cada lote de negocio del DW se usa una sola vez. Repetir un número no agrega filas, porque `append_batch` es idempotente.

## Eliminación

1. **QuickSight** antes de que termine la prueba de 30 días, si el resto se va a conservar más tiempo. `./infra/dw/90_quicksight_baja.sh` pide escribir `CANCELAR QUICKSIGHT`.
2. **Todo lo demás.** `python infra/eliminar_todo.py` hace el inventario sin modificar nada. Lista por fases todo lo que encuentra y compara contra la etiqueta `Project=arquitecturas-datos` para mostrar cualquier recurso que no cubra. `python infra/eliminar_todo.py --ejecutar` lo borra después de escribir `ELIMINAR <cuenta>`. Tarda de 15 a 30 minutos y es idempotente.

| Fase | Qué borra |
|---|---|
| 1 | Objetos y conexión VPC de QuickSight, EC2 de Airflow con su disco, RDS for MySQL sin snapshot final, instancia de DocumentDB, topes de uso y workgroup de Redshift |
| 2 | Suscripción de QuickSight, cluster de DocumentDB, namespace de Redshift con sus snapshots, grupos de subredes y de parámetros de RDS |
| 3 | Grupo de subredes de DocumentDB, los dos buckets con todas sus versiones, base de Glue, workgroup de Athena, parámetros de SSM, secretos, grupos de logs y los cuatro security groups |
| 4 | Perfil de instancia y los cinco roles de IAM con sus políticas |

3. **A mano.** La llave de despliegue en GitHub (Settings > Deploy keys). El presupuesto se conserva para que avise si algo quedó cobrando, y se borra con `--incluir-presupuesto`. Si QuickSight creó roles `aws-quicksight-*`, el inventario los lista para borrarlos a mano.
