# Estimación de costos

Precios de lista bajo demanda en us-east-2, antes de capas gratuitas y créditos. `python infra/costos.py` vuelve a consultar los precios en la AWS Price List API, mide el uso real de la cuenta y trae el gasto de Cost Explorer. Su última salida está en [evidencias/costos/01_costos.txt](../evidencias/costos/01_costos.txt).

## Precios unitarios

| Recurso | Uso en el proyecto | Precio | Fuente |
|---|---|---|---|
| EC2 t3.medium Linux | Airflow con los dos DAGs | 0.0416 USD por hora | [EC2 On-Demand](https://aws.amazon.com/ec2/pricing/on-demand/) |
| EBS gp3 | Disco de 30 GB de la EC2 | 0.08 USD por GB-mes | [EBS](https://aws.amazon.com/ebs/pricing/) |
| IPv4 pública | IP de la EC2 mientras está encendida | 0.005 USD por hora | [VPC](https://aws.amazon.com/vpc/pricing/) |
| RDS for MySQL db.t4g.micro Single-AZ | Fuente relacional | 0.016 USD por hora | [RDS for MySQL](https://aws.amazon.com/rds/mysql/pricing/) |
| RDS gp3 | 20 GB | 0.115 USD por GB-mes | Misma página |
| DocumentDB db.t3.medium | Fuente documental | 0.078 USD por hora | [DocumentDB](https://aws.amazon.com/documentdb/pricing/) |
| DocumentDB almacenamiento e I/O | Menos de 1 GB | 0.10 USD por GB-mes y 0.20 USD por millón de I/O | Misma página |
| Redshift Serverless | 4 RPU de base y de máximo | 0.375 USD por RPU-hora, por segundo con mínimo de 60 s | [Redshift](https://aws.amazon.com/redshift/pricing/) |
| Redshift Managed Storage | Menos de 1 GB | 0.024 USD por GB-mes | Misma página |
| QuickSight Enterprise | Autores del dashboard | 24 USD por autor al mes, con 10 GB de SPICE incluidos | [QuickSight](https://aws.amazon.com/quicksuite/quicksight/pricing) |
| Comprehend | Idioma, entidades y frases clave | 0.0001 USD por unidad de 100 caracteres, mínimo 3 unidades por documento | [Comprehend](https://aws.amazon.com/comprehend/pricing/) |
| Athena | Consultas y validaciones del data lake | 5 USD por TB escaneado, mínimo 10 MB por consulta | [Athena](https://aws.amazon.com/athena/pricing/) |
| S3 Standard | Los dos buckets, menos de 100 MB | 0.023 USD por GB-mes | [S3](https://aws.amazon.com/s3/pricing/) |
| Secrets Manager | 6 secretos (3 de servicios y 3 propios) | 0.40 USD por secreto al mes | [Secrets Manager](https://aws.amazon.com/secrets-manager/pricing/) |

Sin costo en este volumen. Glue Data Catalog (el primer millón de objetos y de solicitudes es gratis), SSM Parameter Store estándar, Session Manager, la conexión VPC de QuickSight y la transferencia entre servicios de la misma región.

## Escenarios

| Escenario | USD | Composición |
|---|---|---|
| Una hora con EC2, RDS y DocumentDB encendidos | 0.141 | 0.0416 + 0.005 + 0.016 + 0.078 |
| Un lote del DW en Redshift | 0.19 | Supuesto de 0.5 RPU-hora, 4 RPU durante unos 7 minutos de consultas de carga, dbt y pruebas |
| Tope diario de Redshift | 3.75 | 10 RPU-horas, al llegar el límite de uso desactiva el workgroup |
| Un mes con todo apagado | 7.23 | Discos (2.40 + 2.30), secretos (2.40), almacenamiento de DocumentDB, Redshift y S3 (0.13) |
| Mes de laboratorio | 12.44 | 20 horas encendido (2.81), 10 lotes del DW (1.88), 40 lotes del data lake con Comprehend (0.52) y Athena (0.02), más el mes apagado |
| QuickSight después de la prueba | 24 por autor | La suscripción tiene dos autores, el usuario IAM y root, así que serían 48 |
| Un mes encendido sin parar | 109.86 | 730 horas encendido más el mes apagado, sin Redshift ni QuickSight |

El costo lo deciden las horas encendidas. Por eso `./infra/detener.sh` se corre al final de cada sesión, y el último escenario muestra lo que costaría olvidarlo.

### Capas gratuitas y créditos que aplican

| Servicio | Beneficio | Efecto en este proyecto |
|---|---|---|
| Redshift Serverless | Crédito de 300 USD por 90 días para cuentas que nunca lo usaron | Cubre todo el cómputo y el almacenamiento del DW |
| DocumentDB | Prueba de un mes con 750 horas de db.t3.medium, 30 millones de I/O y 5 GB | Cubre la fuente documental el primer mes |
| QuickSight | Prueba de 30 días para 25 usuarios en la primera suscripción | Hay que cancelar antes del 2 de diciembre de 2026 con `./infra/dw/90_quicksight_baja.sh` |
| Comprehend | 50,000 unidades por API al mes durante 12 meses desde la primera solicitud | Cubre todo el enriquecimiento |
| RDS y EC2 | Capa gratuita o créditos según la fecha de creación de la cuenta | Puede cubrir parte de las horas encendidas |

El presupuesto de 40 USD mide el costo sin créditos (`IncludeCredit: false`), así que avisa por el consumo real aunque los créditos lo paguen.

## Controles de costo

| Control | Dónde |
|---|---|
| Presupuesto mensual de 40 USD con alertas por correo | `infra/budget/budget.json` |
| Encendido y apagado explícitos de EC2, RDS y DocumentDB | `infra/encender.sh`, `infra/detener.sh` |
| Redshift con 4 RPU de base y de máximo, el mínimo posible | `infra/dw/04_redshift.sh` |
| Tope de 10 RPU-horas al día con desactivación del workgroup | `infra/dw/04_redshift.sh` |
| Consultas de Redshift cortadas a los 30 minutos (`max_query_execution_time`) | `infra/dw/04_redshift.sh` |
| Corte de 100 MB por consulta en Athena | `data-lake/infra/02_athena_workgroup.sh` |
| Caché de Comprehend por hash del texto, así un texto se paga una sola vez | `data-lake/src/dl/enrich.py` |
| Gold recalcula solo las fechas que cambiaron y Silver solo reescribe las fechas tocadas | `gold.py`, `silver.py` |
| Particiones por fecha para que Athena lea solo los días consultados | `data-lake/src/dl/catalog.py` |
| Ciclo de vida que borra versiones viejas de S3 a los 7 días y los resultados de Athena a los 7 días | Scripts de los buckets |
| QuickSight Enterprise sin usuarios Pro ni Q, que activarían la tarifa de 250 USD al mes por cuenta | `infra/dw/07_quicksight.sh` |
| Un NAT Gateway costaría unos 33 USD al mes, más que todo el laboratorio. La EC2 sale por su IP pública sin reglas de entrada | `infra/ec2/01_instancia.sh` |

RDS y DocumentDB se vuelven a encender solos a los 7 días de detenidos. Si el laboratorio se pausa más tiempo, hay que detenerlos otra vez o eliminar todo con `python infra/eliminar_todo.py --ejecutar`.

## Gasto real

Se completa con la salida de `python infra/costos.py`.
