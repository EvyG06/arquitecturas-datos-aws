# Controles de seguridad

Controles implementados en los dos proyectos, con el script o el archivo donde se configuran y la evidencia que los muestra. Las referencias son a la documentación de AWS y al NIST SP 800-53 Rev. 5 cuando el control corresponde a una familia concreta.

## Identidad y mínimo privilegio (NIST AC-6)

| Control | Dónde |
|---|---|
| Ninguna llave de acceso en la EC2. La instancia usa credenciales temporales de su rol vía IMDSv2 | `infra/ec2/01_instancia.sh`, `orquestacion/setup_ec2.sh` |
| El rol de la instancia solo puede registrarse en Session Manager y asumir dos roles de trabajo | `infra/iam/02_ec2_role.sh` |
| `arq-datos-pipeline` confía solo en el usuario IAM y en el rol de la instancia, no en el servicio EC2 en general | `infra/iam/01_pipeline_role.sh` |
| El pipeline no puede borrar Bronze en el data lake ni `raw/` en el DW. Solo borra Silver, Gold, cuarentena y archivos de control | `infra/iam/01_pipeline_role.sh`, `03_dw_access.sh`, [simulación](../evidencias/dw/fuentes/03_simulacion_iam.txt) |
| El pipeline maneja particiones de Glue, pero no puede crear ni borrar tablas. Athena solo en el workgroup `arq-dl` | `infra/iam/01_pipeline_role.sh` |
| Las credenciales maestras de las fuentes son del rol `arq-datos-fuentes`. El pipeline solo lee los secretos de `etl_lector` | `infra/iam/03_dw_access.sh`, `04_rol_fuentes.sh`, [simulación](../evidencias/dw/fuentes/03_simulacion_iam.txt) |
| `etl_lector` solo tiene `SELECT` en MySQL y el rol `read` en DocumentDB | `sources/create_readers.py`, [evidencia](../evidencias/dw/fuentes/02_cdc_y_lectores.txt) |
| En Redshift el pipeline entra como `IAMR:arq-datos-pipeline` con credenciales temporales y `PASSWORD DISABLE` | `infra/dw/05_redshift_bootstrap.py` |
| `ASSUMEROLE` revocado a `PUBLIC` y otorgado solo al usuario del pipeline, para que nadie más use el rol de COPY | `infra/dw/05_redshift_bootstrap.py` |
| El rol de COPY solo lee `raw/` y `_control/copy/` del bucket del DW | `infra/dw/04_redshift.sh` |
| La Data API solo deja ver las sentencias que lanzó el mismo rol (`redshift-data:statement-owner-iam-userid`) | `infra/iam/05_redshift_access.sh` |
| `bi_lector`, el usuario de QuickSight, solo tiene `USAGE` en `marts` y `SELECT` en `ventas_dashboard`, con límite de 10 conexiones | `infra/dw/06_bi_lector.py`, `models/marts/ventas_dashboard.sql` |
| El pipeline solo puede refrescar el dataset SPICE del dashboard (`CreateIngestion`, `DescribeIngestion`) | `infra/dw/07_quicksight.sh` |
| Los scripts de IAM se niegan a correr con la cuenta root o con un rol | `infra/iam/*.sh` |

## Red (NIST SC-7)

| Control | Dónde |
|---|---|
| La EC2 no tiene reglas de entrada. Se administra solo por Session Manager | `infra/ec2/01_instancia.sh` |
| La interfaz de Airflow escucha en `127.0.0.1` de la instancia y se abre con port forwarding de Session Manager | `orquestacion/docker-compose.yml` |
| RDS, DocumentDB y Redshift no son públicos. Sus security groups solo aceptan el puerto de la base desde el grupo de Airflow | `infra/dw/01_fuentes.sh`, `04_redshift.sh` |
| QuickSight llega al Redshift privado por una conexión VPC. Su grupo solo tiene salida al 5439 del grupo de Redshift, y Redshift solo acepta de ese grupo y del de Airflow | `infra/dw/07_quicksight.sh` |
| Las llaves de host de GitHub se toman de su API por HTTPS en lugar de confiar en `ssh-keyscan`, y la llave de despliegue es de solo lectura | `infra/ec2/preparar_github.sh` |

## Cifrado en tránsito y en reposo (NIST SC-8 y SC-28)

| Control | Dónde |
|---|---|
| Los dos buckets rechazan cualquier solicitud sin TLS (`aws:SecureTransport`) | `data-lake/infra/01_bucket.sh`, `infra/dw/02_bucket.sh` |
| MySQL exige TLS en el servidor (`require_secure_transport=1`). DocumentDB tiene TLS activo. El cliente verifica el certificado con el bundle de CA de RDS | `infra/dw/01_fuentes.sh`, `aws_conn.py`, [evidencia](../evidencias/dw/fuentes/01_inspeccion_fuentes.txt) |
| Redshift exige SSL (`require_ssl`) y dbt verifica el certificado y el nombre del host (`sslmode: verify-full`) | `infra/dw/04_redshift.sh`, `profiles.yml` |
| S3 con cifrado del lado del servidor (SSE-S3), EBS, RDS y DocumentDB con almacenamiento cifrado, Redshift cifrado siempre | Scripts de cada recurso |
| Resultados de Athena cifrados (SSE_S3) y configuración del workgroup forzada | `data-lake/infra/02_athena_workgroup.sh` |
| Bloqueo de acceso público en los dos buckets | `data-lake/infra/01_bucket.sh`, `infra/dw/02_bucket.sh` |

## Secretos (NIST IA-5)

- Las contraseñas maestras de RDS, DocumentDB y Redshift las genera cada servicio y las guarda en Secrets Manager (`--manage-master-user-password`). No aparecen en ningún script ni log.
- Las contraseñas de `etl_lector` y `bi_lector` se generan en Python y van directo a Secrets Manager. Nunca pasan por la terminal, el historial de la shell ni los argumentos de un proceso. A Redshift llega solo el hash md5 de la de `bi_lector`.
- `orquestacion/.env` se crea en la EC2 con `umask 077` y secretos aleatorios de `openssl rand`. Está en `.gitignore` y el repositorio solo tiene [.env.example](../orquestacion/.env.example).
- La contraseña de Airflow se lee en una sesión interactiva de Session Manager y no con `remoto.sh`, porque la salida de `send-command` queda guardada en SSM.
- Endpoints y ARNs van en SSM Parameter Store, no en el código.

## Integridad de los datos y de la ejecución

| Control | Dónde |
|---|---|
| Bronze y los Parquet del DW se escriben con `If-None-Match: *`. Un lote existente no se puede sobrescribir, ni siquiera por error | `s3util.py` de los dos proyectos, [evidencia](../evidencias/dw/extraccion/08_sin_sobrescritura.txt) |
| Versionado de S3, con expiración de versiones viejas a los 7 días | Scripts de los buckets |
| Manifiestos y marca de agua con compare-and-swap por ETag. Dos procesos no pueden publicar a la vez | `silver.py`, `gold.py`, `extract.py` |
| El XML se lee con `defusedxml` y DTD prohibidas, y con un tamaño máximo de respuesta. Así no hay expansión de entidades ni XXE (OWASP XXE Prevention Cheat Sheet) | `data-lake/src/dl/ingest.py`, `parse.py` |
| Solo se aceptan enlaces `https` de `eltiempo.com` | `data-lake/src/dl/parse.py` |
| Los parámetros de los DAGs viajan como variables de entorno y nunca se interpolan en el comando de bash. Además tienen patrón o lista cerrada (OWASP OS Command Injection Defense) | `orquestacion/dags/*.py` |
| Los ids de lote se validan con `^[A-Za-z0-9_.-]{1,128}$` antes de usarlos en rutas de S3 o en SQL | `extraction/extract.py` |

## Registro y auditoría (NIST AU-2 y AU-12)

- Redshift exporta `connectionlog` y `userlog` a CloudWatch Logs.
- Cada ejecución de los DAGs deja un registro en `_control/pipeline/` con el estado de cada tarea, y los logs de Airflow llevan lote, tarea e intento en cada línea.
- Cada comando de `remoto.sh` queda en el historial de Run Command de SSM, y CloudTrail guarda 90 días de eventos de administración sin configuración adicional.

## Costo como control

Presupuesto de 40 USD sin créditos, tope diario de 10 RPU-horas en Redshift con desactivación del workgroup, corte de 100 MB por consulta en Athena y encendido y apagado explícitos de lo que cobra por hora. Detalle en [costos.md](costos.md).

## Riesgos aceptados

- La EC2 está en una subred pública con IP pública, sin reglas de entrada, para salir a internet sin pagar un NAT Gateway, que costaría más que el resto del laboratorio.
- S3 usa SSE-S3 y no llaves KMS propias. Con KMS se ganaría control de acceso a la llave y registro de cada uso, a cambio de costo por solicitud.
- QuickSight registró también a la cuenta root como autora al entrar desde la consola. Lo recomendable es abrir el dashboard solo con el usuario IAM, borrar el usuario de root en QuickSight y activar MFA en los dos.
