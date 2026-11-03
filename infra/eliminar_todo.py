#!/usr/bin/env python3
"""Elimina los recursos de AWS de los dos proyectos: data lake, data warehouse y la base comun.

  python infra/eliminar_todo.py                                  inventario, no modifica nada
  python infra/eliminar_todo.py --ejecutar                       borra todo despues de escribir ELIMINAR <cuenta>
  python infra/eliminar_todo.py --ejecutar --incluir-presupuesto tambien borra el presupuesto

Se corre en el Mac despues de arq, con el perfil admin. Tarda de 15 a 30 minutos, casi todo esperando a que
RDS, DocumentDB, Redshift y las interfaces de red de QuickSight terminen de borrarse. Es idempotente: si se
corta, se vuelve a correr y sigue con lo que quede.

El presupuesto se conserva por defecto para que siga avisando si algo quedo cobrando. La llave de despliegue
de GitHub se borra a mano en Settings > Deploy keys del repositorio.
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

import boto3
from botocore.exceptions import ClientError, WaiterError

PROYECTO = "arquitecturas-datos"
EC2_NOMBRE = "arq-datos-airflow"
GRUPOS = ("arq-datos-quicksight", "arq-datos-airflow", "arq-datos-dw-fuentes", "arq-datos-dw-redshift")
RDS = "arq-datos-dw-mysql"
RDS_SUBREDES = "arq-datos-dw-mysql"
RDS_PARAMETROS = "arq-datos-dw-mysql84"
DOCDB = "arq-datos-dw-docdb"
DOCDB_INSTANCIA = "arq-datos-dw-docdb-1"
DOCDB_SUBREDES = "arq-datos-dw-docdb"
REDSHIFT = "arq-datos-dw"  # mismo nombre para el workgroup y el namespace
PERFIL_EC2 = "arq-datos-ec2"
ROLES = ("arq-datos-quicksight-vpc", "arq-datos-redshift-s3", "arq-datos-fuentes", "arq-datos-pipeline",
         "arq-datos-ec2")
PREFIJO_SECRETOS = "arq-datos/"
RUTA_SSM = "/arq-datos/"
GLUE_DB = "arq_dl"
ATHENA_WG = "arq-dl"
PRESUPUESTO = "arquitecturas-datos-mensual"
QS_DASHBOARD = "arq-datos-ventas"
QS_ANALISIS = "arq-datos-ventas-analisis"
QS_DATASET = "arq-datos-ventas"
QS_FUENTE = "arq-datos-dw-redshift"
QS_VPC = "arq-datos-dw-vpc"
PREFIJOS_LOGS = ("/aws/redshift/", "/aws/rds/", "/aws/docdb/")

ESPERA = 1.0  # multiplica todas las pausas; las pruebas lo ponen en 0


def log(msg: str) -> None:
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def codigo(err: ClientError) -> str:
    return err.response.get("Error", {}).get("Code", "")


def si_existe(fn: Callable, *ausente: str):
    """Devuelve el resultado de fn, o None si la API responde con uno de los codigos de recurso ausente."""
    try:
        return fn()
    except ClientError as err:
        if codigo(err) in ausente:
            return None
        raise


def reintentar(fn: Callable, codigos: tuple[str, ...], timeout: int = 900, cada: int = 30):
    """Repite fn mientras falle con un codigo transitorio (recurso ocupado o con dependencias)."""
    inicio = time.monotonic()
    while True:
        try:
            return fn()
        except ClientError as err:
            if codigo(err) not in codigos or time.monotonic() - inicio > timeout:
                raise
            log(f"  {codigo(err)}, se reintenta en {cada} s")
            time.sleep(cada * ESPERA)


def esperar_hasta(que: str, listo: Callable[[], bool], timeout: int = 1800, cada: int = 20) -> None:
    inicio = time.monotonic()
    while not listo():
        if time.monotonic() - inicio > timeout:
            raise TimeoutError(f"{que}: no termino en {timeout // 60} minutos")
        time.sleep(cada * ESPERA)


def esperar_waiter(cliente, nombre: str, **kwargs) -> None:
    try:
        cliente.get_waiter(nombre).wait(WaiterConfig={"Delay": max(1, int(20 * ESPERA)), "MaxAttempts": 90},
                                        **kwargs)
    except WaiterError as err:
        raise TimeoutError(f"{nombre}: {err}") from err


def nada() -> None:
    return None


@dataclass
class Recurso:
    fase: int
    tipo: str
    nombre: str
    detalle: str
    borrar: Callable[[], None]
    esperar: Callable[[], None] = nada
    claves: list[str] = field(default_factory=list)  # ids que aparecen en el ARN, para la verificacion por etiqueta


# ---------------------------------------------------------------- inventario por servicio

def quicksight(ses, cuenta: str) -> list[Recurso]:
    qs = ses.client("quicksight")
    info = si_existe(lambda: qs.describe_account_subscription(AwsAccountId=cuenta)["AccountInfo"],
                     "ResourceNotFoundException")
    estado = (info or {}).get("AccountSubscriptionStatus", "")
    if not info or "UNSUBSCRIBED" in estado or "DELET" in estado:
        return []
    out = []
    objetos = (
        ("dashboard", QS_DASHBOARD,
         lambda: qs.describe_dashboard(AwsAccountId=cuenta, DashboardId=QS_DASHBOARD)["Dashboard"],
         lambda: qs.delete_dashboard(AwsAccountId=cuenta, DashboardId=QS_DASHBOARD)),
        ("analisis", QS_ANALISIS,
         lambda: qs.describe_analysis(AwsAccountId=cuenta, AnalysisId=QS_ANALISIS)["Analysis"],
         lambda: qs.delete_analysis(AwsAccountId=cuenta, AnalysisId=QS_ANALISIS, ForceDeleteWithoutRecovery=True)),
        ("dataset SPICE", QS_DATASET,
         lambda: qs.describe_data_set(AwsAccountId=cuenta, DataSetId=QS_DATASET)["DataSet"],
         lambda: qs.delete_data_set(AwsAccountId=cuenta, DataSetId=QS_DATASET)),
        ("fuente de datos", QS_FUENTE,
         lambda: qs.describe_data_source(AwsAccountId=cuenta, DataSourceId=QS_FUENTE)["DataSource"],
         lambda: qs.delete_data_source(AwsAccountId=cuenta, DataSourceId=QS_FUENTE)),
    )
    for tipo, nombre, describir, borrar in objetos:
        obj = si_existe(describir, "ResourceNotFoundException")
        if obj and obj.get("Status") != "DELETED":
            out.append(Recurso(1, f"QuickSight {tipo}", nombre, "", borrar, claves=[nombre]))

    vpc = si_existe(lambda: qs.describe_vpc_connection(AwsAccountId=cuenta, VPCConnectionId=QS_VPC)["VPCConnection"],
                    "ResourceNotFoundException")
    if vpc and vpc.get("Status") != "DELETED":
        def vpc_borrada() -> bool:
            v = si_existe(lambda: qs.describe_vpc_connection(AwsAccountId=cuenta, VPCConnectionId=QS_VPC)
                          ["VPCConnection"], "ResourceNotFoundException")
            return v is None or v.get("Status") == "DELETED"
        out.append(Recurso(
            1, "QuickSight conexion VPC", QS_VPC, vpc.get("Status", ""),
            lambda: qs.delete_vpc_connection(AwsAccountId=cuenta, VPCConnectionId=QS_VPC),
            lambda: esperar_hasta("conexion VPC de QuickSight", vpc_borrada, timeout=1200), [QS_VPC]))

    def cancelar() -> None:
        qs.update_account_settings(AwsAccountId=cuenta, DefaultNamespace="default", TerminationProtectionEnabled=False)
        qs.delete_account_subscription(AwsAccountId=cuenta)
    out.append(Recurso(2, "QuickSight suscripcion", info.get("AccountName", cuenta),
                       f"{info.get('Edition', '')} {estado}".strip(), cancelar))
    return out


def ec2_instancia(ses) -> list[Recurso]:
    ec2 = ses.client("ec2")
    vivas = ["pending", "running", "stopping", "stopped", "shutting-down"]
    res = ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": [EC2_NOMBRE]},
                                          {"Name": "instance-state-name", "Values": vivas}])
    inst = [i for r in res["Reservations"] for i in r["Instances"]]
    if not inst:
        return []
    ids = [i["InstanceId"] for i in inst]
    discos = [b["Ebs"]["VolumeId"] for i in inst for b in i.get("BlockDeviceMappings", []) if "Ebs" in b]
    estados = ", ".join(f"{i['InstanceId']} {i['State']['Name']}" for i in inst)
    return [Recurso(1, "EC2 de Airflow (el disco se borra con ella)", EC2_NOMBRE, estados,
                    lambda: ec2.terminate_instances(InstanceIds=ids),
                    lambda: esperar_waiter(ec2, "instance_terminated", InstanceIds=ids), ids + discos)]


def rds_mysql(ses) -> list[Recurso]:
    rds = ses.client("rds")
    out = []
    inst = si_existe(lambda: rds.describe_db_instances(DBInstanceIdentifier=RDS)["DBInstances"][0],
                     "DBInstanceNotFound", "DBInstanceNotFoundFault")
    if inst:
        def borrar() -> None:
            if inst["DBInstanceStatus"] != "deleting":
                reintentar(lambda: rds.delete_db_instance(DBInstanceIdentifier=RDS, SkipFinalSnapshot=True,
                                                          DeleteAutomatedBackups=True),
                           ("InvalidDBInstanceState", "InvalidDBInstanceStateFault"))
        out.append(Recurso(1, "RDS for MySQL", RDS, inst["DBInstanceStatus"], borrar,
                           lambda: esperar_waiter(rds, "db_instance_deleted", DBInstanceIdentifier=RDS), [RDS]))
    if si_existe(lambda: rds.describe_db_subnet_groups(DBSubnetGroupName=RDS_SUBREDES)["DBSubnetGroups"],
                 "DBSubnetGroupNotFoundFault"):
        out.append(Recurso(2, "RDS grupo de subredes", RDS_SUBREDES, "",
                           lambda: rds.delete_db_subnet_group(DBSubnetGroupName=RDS_SUBREDES), claves=[RDS_SUBREDES]))
    if si_existe(lambda: rds.describe_db_parameter_groups(DBParameterGroupName=RDS_PARAMETROS)["DBParameterGroups"],
                 "DBParameterGroupNotFound", "DBParameterGroupNotFoundFault"):
        out.append(Recurso(2, "RDS grupo de parametros", RDS_PARAMETROS, "",
                           lambda: rds.delete_db_parameter_group(DBParameterGroupName=RDS_PARAMETROS),
                           claves=[RDS_PARAMETROS]))
    return out


def documentdb(ses) -> list[Recurso]:
    doc = ses.client("docdb")
    out = []

    def cluster():
        return si_existe(lambda: doc.describe_db_clusters(DBClusterIdentifier=DOCDB)["DBClusters"][0],
                         "DBClusterNotFoundFault")

    def arrancar_si_detenido() -> None:
        # Con el cluster detenido no se puede borrar la instancia: se enciende primero.
        c = cluster()
        if c and c["Status"] in ("stopped", "stopping"):
            if c["Status"] == "stopping":
                esperar_hasta("DocumentDB deteniendose", lambda: cluster()["Status"] == "stopped", timeout=1200)
            log(f"  {DOCDB} esta detenido, se enciende para poder borrarlo")
            doc.start_db_cluster(DBClusterIdentifier=DOCDB)
            esperar_hasta("DocumentDB encendiendo", lambda: cluster()["Status"] == "available", timeout=1800)

    cl = cluster()
    inst = si_existe(lambda: doc.describe_db_instances(DBInstanceIdentifier=DOCDB_INSTANCIA)["DBInstances"][0],
                     "DBInstanceNotFound", "DBInstanceNotFoundFault")
    estados = ("InvalidDBInstanceState", "InvalidDBInstanceStateFault", "InvalidDBClusterStateFault")
    if inst:
        def borrar_instancia() -> None:
            if inst["DBInstanceStatus"] != "deleting":
                arrancar_si_detenido()
                reintentar(lambda: doc.delete_db_instance(DBInstanceIdentifier=DOCDB_INSTANCIA), estados)
        out.append(Recurso(1, "DocumentDB instancia", DOCDB_INSTANCIA, inst["DBInstanceStatus"], borrar_instancia,
                           lambda: esperar_waiter(doc, "db_instance_deleted", DBInstanceIdentifier=DOCDB_INSTANCIA),
                           [DOCDB_INSTANCIA]))
    if cl:
        def borrar_cluster() -> None:
            c = cluster()
            if c and c["Status"] != "deleting":
                arrancar_si_detenido()
                reintentar(lambda: doc.delete_db_cluster(DBClusterIdentifier=DOCDB, SkipFinalSnapshot=True), estados)
        out.append(Recurso(2, "DocumentDB cluster", DOCDB, cl["Status"], borrar_cluster,
                           lambda: esperar_hasta("cluster DocumentDB", lambda: cluster() is None), [DOCDB]))
    if si_existe(lambda: doc.describe_db_subnet_groups(DBSubnetGroupName=DOCDB_SUBREDES)["DBSubnetGroups"],
                 "DBSubnetGroupNotFoundFault"):
        out.append(Recurso(3, "DocumentDB grupo de subredes", DOCDB_SUBREDES, "",
                           lambda: doc.delete_db_subnet_group(DBSubnetGroupName=DOCDB_SUBREDES),
                           claves=[DOCDB_SUBREDES]))
    return out


def redshift(ses) -> list[Recurso]:
    rs = ses.client("redshift-serverless")
    out = []

    def workgroup():
        return si_existe(lambda: rs.get_workgroup(workgroupName=REDSHIFT)["workgroup"], "ResourceNotFoundException")

    def namespace():
        return si_existe(lambda: rs.get_namespace(namespaceName=REDSHIFT)["namespace"], "ResourceNotFoundException")

    wg = workgroup()
    if wg:
        limites = rs.list_usage_limits(resourceArn=wg["workgroupArn"]).get("usageLimits", [])

        def borrar_wg() -> None:
            for lim in limites:
                si_existe(lambda i=lim["usageLimitId"]: rs.delete_usage_limit(usageLimitId=i),
                          "ResourceNotFoundException")
            if wg.get("status") != "DELETING":
                reintentar(lambda: rs.delete_workgroup(workgroupName=REDSHIFT), ("ConflictException",))
        out.append(Recurso(1, "Redshift Serverless workgroup", REDSHIFT,
                           f"{wg.get('status')}, {wg.get('baseCapacity')} RPU, {len(limites)} tope(s) de uso",
                           borrar_wg, lambda: esperar_hasta("workgroup de Redshift", lambda: workgroup() is None),
                           [f"workgroup/{wg.get('workgroupId', REDSHIFT)}"]))
    ns = namespace()
    if ns:
        fotos = [s["snapshotName"] for s in rs.list_snapshots(namespaceName=REDSHIFT).get("snapshots", [])]

        def borrar_ns() -> None:
            for nombre in fotos:
                si_existe(lambda n=nombre: rs.delete_snapshot(snapshotName=n), "ResourceNotFoundException")
            if ns.get("status") != "DELETING":
                reintentar(lambda: rs.delete_namespace(namespaceName=REDSHIFT), ("ConflictException",))
        out.append(Recurso(2, "Redshift Serverless namespace (datos y secreto del admin)", REDSHIFT,
                           f"{ns.get('status')}, {len(fotos)} snapshot(s)", borrar_ns,
                           lambda: esperar_hasta("namespace de Redshift", lambda: namespace() is None),
                           [f"namespace/{ns.get('namespaceId', REDSHIFT)}"]))
    return out


def vaciar_y_borrar_bucket(s3, nombre: str) -> None:
    for pagina in s3.get_paginator("list_multipart_uploads").paginate(Bucket=nombre):
        for up in pagina.get("Uploads", []):
            s3.abort_multipart_upload(Bucket=nombre, Key=up["Key"], UploadId=up["UploadId"])
    borrados = 0
    for pagina in s3.get_paginator("list_object_versions").paginate(Bucket=nombre):
        objs = [{"Key": o["Key"], "VersionId": o["VersionId"]}
                for o in pagina.get("Versions", []) + pagina.get("DeleteMarkers", [])]
        for i in range(0, len(objs), 1000):
            resp = s3.delete_objects(Bucket=nombre, Delete={"Objects": objs[i:i + 1000], "Quiet": True})
            if resp.get("Errors"):
                raise RuntimeError(f"{nombre}: {resp['Errors'][:3]}")
            borrados += len(objs[i:i + 1000])
    log(f"  {nombre}: {borrados} versiones de objetos borradas")
    s3.delete_bucket(Bucket=nombre)


def buckets(ses, cuenta: str, region: str) -> list[Recurso]:
    s3 = ses.client("s3")
    out = []
    for capa, nombre in (("data lake", f"eve-dl-{cuenta}-{region}"), ("data warehouse", f"eve-dw-{cuenta}-{region}")):
        try:
            s3.head_bucket(Bucket=nombre)
        except ClientError as err:
            if codigo(err) in ("404", "NoSuchBucket", "NotFound"):
                continue
            raise
        versiones = sum(len(p.get("Versions", [])) + len(p.get("DeleteMarkers", []))
                        for p in s3.get_paginator("list_object_versions").paginate(Bucket=nombre))
        out.append(Recurso(3, f"S3 bucket del {capa}, con todas sus versiones", nombre, f"{versiones} versiones",
                           lambda n=nombre: vaciar_y_borrar_bucket(s3, n), claves=[nombre]))
    return out


def catalogo(ses) -> list[Recurso]:
    out = []
    glue = ses.client("glue")
    if si_existe(lambda: glue.get_database(Name=GLUE_DB).get("Database"), "EntityNotFoundException"):
        tablas = [t["Name"] for p in glue.get_paginator("get_tables").paginate(DatabaseName=GLUE_DB)
                  for t in p["TableList"]]
        out.append(Recurso(3, "Glue Data Catalog base (tablas y particiones)", GLUE_DB, f"{len(tablas)} tablas",
                           lambda: glue.delete_database(Name=GLUE_DB), claves=[f"database/{GLUE_DB}"]))
    athena = ses.client("athena")
    if si_existe(lambda: athena.get_work_group(WorkGroup=ATHENA_WG).get("WorkGroup", {}).get("Name"),
                 "InvalidRequestException", "ResourceNotFoundException"):
        out.append(Recurso(3, "Athena workgroup", ATHENA_WG, "",
                           lambda: athena.delete_work_group(WorkGroup=ATHENA_WG, RecursiveDeleteOption=True),
                           claves=[f"workgroup/{ATHENA_WG}"]))
    return out


def configuracion(ses, incluir_presupuesto: bool, cuenta: str) -> list[Recurso]:
    out = []
    ssm = ses.client("ssm")
    nombres = [p["Name"] for pag in ssm.get_paginator("get_parameters_by_path").paginate(Path=RUTA_SSM, Recursive=True)
               for p in pag["Parameters"]]
    if nombres:
        def borrar_ssm() -> None:
            for i in range(0, len(nombres), 10):
                ssm.delete_parameters(Names=nombres[i:i + 10])
        out.append(Recurso(3, "Parametros de SSM", RUTA_SSM + "*", f"{len(nombres)} parametros", borrar_ssm,
                           claves=[f"parameter{RUTA_SSM}"]))

    sm = ses.client("secretsmanager")
    secretos = [s for pag in sm.get_paginator("list_secrets").paginate(
        Filters=[{"Key": "name", "Values": [PREFIJO_SECRETOS]}]) for s in pag["SecretList"]]
    for s in secretos:
        out.append(Recurso(3, "Secreto (sin ventana de recuperacion)", s["Name"], "",
                           lambda arn=s["ARN"]: sm.delete_secret(SecretId=arn, ForceDeleteWithoutRecovery=True),
                           claves=[s["ARN"]]))

    logs = ses.client("logs")
    for prefijo in PREFIJOS_LOGS:
        for pag in logs.get_paginator("describe_log_groups").paginate(logGroupNamePrefix=prefijo):
            for g in pag["logGroups"]:
                if "arq-datos" in g["logGroupName"]:
                    out.append(Recurso(3, "CloudWatch grupo de logs", g["logGroupName"],
                                       f"{g.get('storedBytes', 0)} bytes",
                                       lambda n=g["logGroupName"]: logs.delete_log_group(logGroupName=n),
                                       claves=[g["logGroupName"]]))

    if incluir_presupuesto:
        budgets = ses.client("budgets", region_name="us-east-1")
        if si_existe(lambda: budgets.describe_budget(AccountId=cuenta, BudgetName=PRESUPUESTO).get("Budget"),
                     "NotFoundException"):
            out.append(Recurso(3, "Presupuesto", PRESUPUESTO, "",
                               lambda: budgets.delete_budget(AccountId=cuenta, BudgetName=PRESUPUESTO),
                               claves=[PRESUPUESTO]))
    return out


def borrar_grupos(ec2, grupos: list[dict]) -> None:
    # Primero se quitan todas las reglas: los grupos se referencian entre si y AWS no borra un grupo referenciado.
    for g in grupos:
        if g.get("IpPermissions"):
            si_existe(lambda g=g: ec2.revoke_security_group_ingress(GroupId=g["GroupId"],
                                                                    IpPermissions=g["IpPermissions"]),
                      "InvalidGroup.NotFound", "InvalidPermission.NotFound")
        if g.get("IpPermissionsEgress"):
            si_existe(lambda g=g: ec2.revoke_security_group_egress(GroupId=g["GroupId"],
                                                                   IpPermissions=g["IpPermissionsEgress"]),
                      "InvalidGroup.NotFound", "InvalidPermission.NotFound")
    pendientes = {g["GroupId"]: g["GroupName"] for g in grupos}
    inicio = time.monotonic()
    while pendientes:
        for gid, nombre in list(pendientes.items()):
            try:
                ec2.delete_security_group(GroupId=gid)
                log(f"  grupo {nombre} borrado")
                pendientes.pop(gid)
            except ClientError as err:
                if codigo(err) == "InvalidGroup.NotFound":
                    pendientes.pop(gid)
                elif codigo(err) != "DependencyViolation":
                    raise
        if not pendientes:
            break
        espera = time.monotonic() - inicio
        if espera > 1800:
            raise TimeoutError(f"grupos con interfaces de red todavia en uso: {sorted(pendientes.values())}")
        for gid, nombre in pendientes.items():
            enis = ec2.describe_network_interfaces(Filters=[{"Name": "group-id", "Values": [gid]}])["NetworkInterfaces"]
            for eni in enis:
                log(f"  {nombre} sigue en uso por {eni['NetworkInterfaceId']} ({eni.get('Status')}): "
                    f"{eni.get('Description', '')[:80]}")
                # Una interfaz suelta (available) que ya no usa ningun servicio se puede borrar directamente.
                if eni.get("Status") == "available" and espera > 300:
                    si_existe(lambda e=eni: ec2.delete_network_interface(NetworkInterfaceId=e["NetworkInterfaceId"]),
                              "InvalidNetworkInterfaceID.NotFound", "InvalidParameterValue",
                              "AuthFailure", "OperationNotPermitted")
        time.sleep(30 * ESPERA)


def grupos_seguridad(ses) -> list[Recurso]:
    ec2 = ses.client("ec2")
    grupos = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": list(GRUPOS)}])["SecurityGroups"]
    if not grupos:
        return []
    detalle = ", ".join(f"{g['GroupName']} ({g['GroupId']})" for g in grupos)
    return [Recurso(3, "Security groups", f"{len(grupos)} grupos", detalle, lambda: borrar_grupos(ec2, grupos),
                    claves=[g["GroupId"] for g in grupos])]


def borrar_rol(iam, nombre: str) -> None:
    for p in iam.list_instance_profiles_for_role(RoleName=nombre)["InstanceProfiles"]:
        iam.remove_role_from_instance_profile(InstanceProfileName=p["InstanceProfileName"], RoleName=nombre)
    for pag in iam.get_paginator("list_attached_role_policies").paginate(RoleName=nombre):
        for p in pag["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=nombre, PolicyArn=p["PolicyArn"])
    for pag in iam.get_paginator("list_role_policies").paginate(RoleName=nombre):
        for p in pag["PolicyNames"]:
            iam.delete_role_policy(RoleName=nombre, PolicyName=p)
    iam.delete_role(RoleName=nombre)


def identidades(ses) -> list[Recurso]:
    iam = ses.client("iam")
    out = []
    perfil = si_existe(lambda: iam.get_instance_profile(InstanceProfileName=PERFIL_EC2)["InstanceProfile"],
                       "NoSuchEntity")
    if perfil:
        def borrar_perfil() -> None:
            for r in perfil["Roles"]:
                si_existe(lambda r=r: iam.remove_role_from_instance_profile(InstanceProfileName=PERFIL_EC2,
                                                                            RoleName=r["RoleName"]), "NoSuchEntity")
            iam.delete_instance_profile(InstanceProfileName=PERFIL_EC2)
        out.append(Recurso(4, "IAM perfil de instancia", PERFIL_EC2, "", borrar_perfil, claves=[PERFIL_EC2]))
    for nombre in ROLES:
        rol = si_existe(lambda n=nombre: iam.get_role(RoleName=n)["Role"], "NoSuchEntity")
        if rol:
            out.append(Recurso(4, "IAM rol (con sus politicas en linea)", nombre, "",
                               lambda n=nombre: borrar_rol(iam, n), claves=[f"role/{nombre}"]))
    return out


def inventario(ses, cuenta: str, region: str, incluir_presupuesto: bool) -> list[Recurso]:
    recursos: list[Recurso] = []
    for paso in (lambda: quicksight(ses, cuenta), lambda: ec2_instancia(ses), lambda: rds_mysql(ses),
                 lambda: documentdb(ses), lambda: redshift(ses), lambda: buckets(ses, cuenta, region),
                 lambda: catalogo(ses), lambda: configuracion(ses, incluir_presupuesto, cuenta),
                 lambda: grupos_seguridad(ses), lambda: identidades(ses)):
        recursos += paso()
    return sorted(recursos, key=lambda r: r.fase)


def etiquetados(ses) -> list[str]:
    tag = ses.client("resourcegroupstaggingapi")
    return [r["ResourceARN"] for pag in tag.get_paginator("get_resources").paginate(
        TagFilters=[{"Key": "Project", "Values": [PROYECTO]}]) for r in pag["ResourceTagMappingList"]]


def informativos(ses) -> list[str]:
    """Cosas que no borra el script pero conviene revisar a mano."""
    notas = []
    iam = ses.client("iam")
    for pag in iam.get_paginator("list_roles").paginate():
        for r in pag["Roles"]:
            if r["RoleName"].startswith("aws-quicksight-"):
                notas.append(f"rol {r['RoleName']}: lo creo QuickSight, borralo si no lo vas a volver a usar")
    sm = ses.client("secretsmanager")
    for pag in sm.get_paginator("list_secrets").paginate():
        for s in pag["SecretList"]:
            if s.get("OwningService") in ("rds", "redshift", "docdb"):
                notas.append(f"secreto {s['Name']} administrado por {s['OwningService']}: el servicio lo borra con "
                             "su recurso, si sigue aqui despues de borrar todo revisalo")
    notas.append("llave de despliegue de GitHub: borrala a mano en Settings > Deploy keys del repositorio")
    return notas


def imprimir(recursos: list[Recurso]) -> None:
    if not recursos:
        print("No queda ningun recurso del proyecto.")
        return
    actual = None
    for r in recursos:
        if r.fase != actual:
            actual = r.fase
            print(f"\nFase {r.fase}")
        detalle = f" | {r.detalle}" if r.detalle else ""
        print(f"  {r.tipo:<58} {r.nombre}{detalle}")


def no_cubiertos(arns: list[str], recursos: list[Recurso]) -> list[str]:
    claves = [c for r in recursos for c in (r.claves or [r.nombre]) if c] + [f"budget/{PRESUPUESTO}"]
    return [a for a in arns if not any(c in a for c in claves)]


def ejecutar(recursos: list[Recurso]) -> list[str]:
    fallos = []
    for fase in sorted({r.fase for r in recursos}):
        log(f"==== fase {fase}")
        iniciados = []
        for r in (x for x in recursos if x.fase == fase):
            log(f"borrando {r.tipo}: {r.nombre}")
            try:
                r.borrar()
                iniciados.append(r)
            except Exception as err:  # se sigue con lo demas y se reporta al final
                fallos.append(f"{r.tipo} {r.nombre}: {err}")
                log(f"  ERROR {err}")
        for r in iniciados:
            if r.esperar is nada:
                continue
            log(f"esperando {r.tipo}: {r.nombre}")
            try:
                r.esperar()
                log(f"  {r.nombre} eliminado")
            except Exception as err:
                fallos.append(f"{r.tipo} {r.nombre}: {err}")
                log(f"  ERROR {err}")
    return fallos


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Elimina todos los recursos de AWS del proyecto")
    p.add_argument("--ejecutar", action="store_true", help="borra de verdad, despues de una confirmacion escrita")
    p.add_argument("--incluir-presupuesto", action="store_true", help="tambien borra el presupuesto de costos")
    args = p.parse_args(argv)

    ses = boto3.Session()
    region = ses.region_name or "us-east-2"
    ident = ses.client("sts").get_caller_identity()
    cuenta = ident["Account"]
    print(f"Cuenta {cuenta} | region {region} | identidad {ident['Arn']}")
    print("Modo:", "EJECUCION" if args.ejecutar else "inventario, no se modifica nada")

    recursos = inventario(ses, cuenta, region, args.incluir_presupuesto)
    imprimir(recursos)
    arns = etiquetados(ses)
    sueltos = no_cubiertos(arns, recursos)
    print(f"\nRecursos con la etiqueta Project={PROYECTO}: {len(arns)}. No cubiertos por el inventario: {len(sueltos)}")
    for a in sueltos:
        print(f"  {a}")
    print("\nPara revisar a mano:")
    for nota in informativos(ses):
        print(f"  {nota}")
    if not args.incluir_presupuesto:
        print(f"  presupuesto {PRESUPUESTO}: se conserva, con --incluir-presupuesto tambien se borra")

    if not args.ejecutar or not recursos:
        return 0

    print("\nEsto borra de forma IRREVERSIBLE los datos de S3, Redshift, RDS y DocumentDB, y cancela QuickSight.")
    try:
        texto = input(f"Escribe ELIMINAR {cuenta} para continuar: ")
    except EOFError:
        texto = ""
    if texto.strip() != f"ELIMINAR {cuenta}":
        print("No se hizo nada")
        return 1

    fallos = ejecutar(recursos)
    log("==== verificacion")
    quedan = inventario(ses, cuenta, region, args.incluir_presupuesto)
    imprimir(quedan)
    arns = etiquetados(ses)
    print(f"\nRecursos que aun aparecen con la etiqueta Project={PROYECTO}: {len(arns)}")
    print("(la API de etiquetas puede tardar unos minutos en dejar de listar recursos recien borrados)")
    for a in arns:
        print(f"  {a}")
    if fallos:
        print("\nFallos:")
        for f in fallos:
            print(f"  {f}")
        print("Vuelve a correr el script: es idempotente y sigue con lo que quede.")
    return 1 if fallos or quedan else 0


if __name__ == "__main__":
    sys.exit(main())
