#!/usr/bin/env python3
"""Estimacion de costos de los dos proyectos y gasto real de la cuenta. Solo lee, no modifica nada.

  python infra/costos.py

Secciones:
  1. Precios unitarios. Se consultan en la AWS Price List API para us-east-2. Si la API no devuelve un precio,
     se usa el valor documentado en docs/costos.md y se marca como tal.
  2. Uso medido: recursos encendidos, autores de QuickSight, secretos, bytes en S3 y RPU-horas de Redshift.
  3. Escenarios: hora encendida, mes apagado, mes de laboratorio y mes encendido sin parar.
  4. Gasto real por servicio en Cost Explorer (antes de creditos) y estado del presupuesto.

Cost Explorer cobra 0.01 USD por solicitud a la API y este script hace tres.
Se corre en el Mac despues de arq, con el perfil admin.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

REGION = "us-east-2"
PREFIJO_USO = "USE2"
REDSHIFT_WG = "arq-datos-dw"
PRESUPUESTO = "arquitecturas-datos-mensual"
EC2_NOMBRE = "arq-datos-airflow"

# Valores de respaldo, tomados de las paginas de precios de AWS (ver docs/costos.md). La API los reemplaza.
DOCUMENTADOS = {
    "ec2_t3_medium_hora": 0.0416,
    "ebs_gp3_gb_mes": 0.08,
    "ipv4_publica_hora": 0.005,
    "rds_t4g_micro_hora": 0.016,
    "rds_gp3_gb_mes": 0.115,
    "docdb_t3_medium_hora": 0.078,
    "docdb_almacenamiento_gb_mes": 0.10,
    "redshift_rpu_hora": 0.375,
    "redshift_rms_gb_mes": 0.024,
    "s3_standard_gb_mes": 0.023,
    "secreto_mes": 0.40,
    "quicksight_autor_mes": 24.0,
    "comprehend_unidad": 0.0001,  # 100 caracteres, minimo 3 unidades por documento y por API
    "athena_tb": 5.0,  # minimo 10 MB por consulta
}

# Busquedas en la Price List API: (servicio, filtros exactos, condicion sobre la dimension de precio)
BUSQUEDAS = {
    "ec2_t3_medium_hora": ("AmazonEC2", {"usagetype": f"{PREFIJO_USO}-BoxUsage:t3.medium", "operation": "RunInstances",
                                         "capacitystatus": "Used", "preInstalledSw": "NA"}, None),
    "ebs_gp3_gb_mes": ("AmazonEC2", {"usagetype": f"{PREFIJO_USO}-EBS:VolumeUsage.gp3"}, None),
    "ipv4_publica_hora": ("AmazonVPC", {"usagetype": f"{PREFIJO_USO}-PublicIPv4:InUseAddress"}, None),
    "rds_t4g_micro_hora": ("AmazonRDS", {"usagetype": f"{PREFIJO_USO}-InstanceUsage:db.t4g.micro",
                                         "databaseEngine": "MySQL", "deploymentOption": "Single-AZ"}, None),
    "rds_gp3_gb_mes": ("AmazonRDS", {"usagetype": f"{PREFIJO_USO}-RDS:GP3-Storage", "databaseEngine": "MySQL",
                                     "deploymentOption": "Single-AZ"}, None),
    "docdb_t3_medium_hora": ("AmazonDocDB", {"usagetype": f"{PREFIJO_USO}-InstanceUsage:db.t3.medium"}, None),
    "docdb_almacenamiento_gb_mes": ("AmazonDocDB", {"usagetype": f"{PREFIJO_USO}-StorageUsage"}, None),
    "redshift_rpu_hora": ("AmazonRedshift", {"regionCode": REGION},
                          lambda attrs, dim: "serverless" in attrs.get("usagetype", "").lower()
                          and "rpu" in dim.get("unit", "").lower()),
    "s3_standard_gb_mes": ("AmazonS3", {"usagetype": f"{PREFIJO_USO}-TimedStorage-ByteHrs"}, None),
    "secreto_mes": ("AWSSecretsManager", {"regionCode": REGION},
                    lambda attrs, dim: "api" not in attrs.get("usagetype", "").lower()
                    and "request" not in dim.get("unit", "").lower()),
}


def seccion(titulo: str) -> None:
    print(f"\n## {titulo}")


def tabla(filas: list[tuple], encabezado: tuple) -> None:
    filas = [tuple(str(c) for c in f) for f in filas]
    anchos = [max(len(str(x)) for x in col) for col in zip(encabezado, *filas, strict=True)]
    for f in [encabezado, *filas]:
        print("  " + "  ".join(str(c).ljust(a) for c, a in zip(f, anchos, strict=True)))


def precio_api(pricing, servicio: str, filtros: dict, cond) -> tuple[float, str] | None:
    """Mayor precio bajo demanda que cumple los filtros (en S3 es el primer tramo). None si no hay ninguno."""
    kwargs = {"ServiceCode": servicio, "MaxResults": 100,
              "Filters": [{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in filtros.items()]}
    mejor = None
    for _ in range(10):
        resp = pricing.get_products(**kwargs)
        for crudo in resp.get("PriceList", []):
            prod = json.loads(crudo)
            attrs = prod.get("product", {}).get("attributes", {})
            for termino in prod.get("terms", {}).get("OnDemand", {}).values():
                for dim in termino.get("priceDimensions", {}).values():
                    usd = float(dim.get("pricePerUnit", {}).get("USD", "0") or 0)
                    if usd > 0 and (cond is None or cond(attrs, dim)) and (mejor is None or usd > mejor[0]):
                        mejor = (usd, f"{dim.get('unit', '')} | {dim.get('description', '')[:70]}")
        if not resp.get("NextToken"):
            break
        kwargs["NextToken"] = resp["NextToken"]
    return mejor


def precios(ses) -> dict[str, float]:
    seccion("1. Precios unitarios en us-east-2 (USD)")
    pricing = ses.client("pricing", region_name="us-east-1")
    usados, filas = {}, []
    for clave, respaldo in DOCUMENTADOS.items():
        origen, detalle, valor = "documentado", "", respaldo
        if clave in BUSQUEDAS:
            servicio, filtros, cond = BUSQUEDAS[clave]
            try:
                encontrado = precio_api(pricing, servicio, filtros, cond)
            except (ClientError, BotoCoreError) as err:
                encontrado, detalle = None, f"API: {err}"[:70]
            if encontrado and abs(encontrado[0] - respaldo) <= 0.5 * respaldo:
                valor, detalle = encontrado
                origen = "Price List API"
            elif encontrado:
                # un filtro que trae otro producto no debe mover la estimacion: se avisa y se usa el documentado
                detalle = f"la API dio {encontrado[0]:.4f} ({encontrado[1][:40]}), fuera de rango"
        usados[clave] = valor
        filas.append((clave, f"{valor:.4f}", origen, detalle))
    tabla(filas, ("concepto", "USD", "origen", "unidad y descripcion de AWS"))
    return usados


def uso(ses) -> dict:
    seccion("2. Uso medido en la cuenta")
    datos: dict = {}
    ec2 = ses.client("ec2")
    inst = [i for r in ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": [EC2_NOMBRE]}])["Reservations"]
            for i in r["Instances"] if i["State"]["Name"] != "terminated"]
    datos["ec2"] = inst[0]["State"]["Name"] if inst else "no existe"
    rds = ses.client("rds")
    try:
        datos["rds"] = rds.describe_db_instances(DBInstanceIdentifier="arq-datos-dw-mysql")["DBInstances"][0][
            "DBInstanceStatus"]
    except ClientError:
        datos["rds"] = "no existe"
    try:
        datos["docdb"] = ses.client("docdb").describe_db_clusters(DBClusterIdentifier="arq-datos-dw-docdb")[
            "DBClusters"][0]["Status"]
    except ClientError:
        datos["docdb"] = "no existe"

    cuenta = ses.client("sts").get_caller_identity()["Account"]
    try:
        usuarios = ses.client("quicksight").list_users(AwsAccountId=cuenta, Namespace="default")["UserList"]
        datos["autores_qs"] = sum(u.get("Role") in ("ADMIN", "AUTHOR", "ADMIN_PRO", "AUTHOR_PRO") for u in usuarios)
        datos["usuarios_qs"] = ", ".join(f"{u['UserName']} ({u.get('Role')})" for u in usuarios)
    except ClientError as err:
        datos["autores_qs"], datos["usuarios_qs"] = 0, f"sin QuickSight ({err.response['Error']['Code']})"

    sm = ses.client("secretsmanager")
    secretos = [s["Name"] for p in sm.get_paginator("list_secrets").paginate() for s in p["SecretList"]]
    datos["secretos"] = len(secretos)

    s3 = ses.client("s3")
    total = 0
    for nombre in (f"eve-dl-{cuenta}-{REGION}", f"eve-dw-{cuenta}-{REGION}"):
        try:
            for p in s3.get_paginator("list_object_versions").paginate(Bucket=nombre):
                total += sum(v["Size"] for v in p.get("Versions", []))
        except ClientError:
            pass
    datos["s3_gb"] = total / 1024 ** 3

    cw = ses.client("cloudwatch")
    hoy = datetime.now(timezone.utc)
    inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    puntos = cw.get_metric_statistics(Namespace="AWS/Redshift-Serverless", MetricName="ComputeSeconds",
                                      Dimensions=[{"Name": "Workgroup", "Value": REDSHIFT_WG}],
                                      StartTime=inicio_mes - timedelta(days=31), EndTime=hoy, Period=86400,
                                      Statistics=["Sum"])["Datapoints"]
    por_dia = sorted((p["Timestamp"].date(), p["Sum"] / 3600) for p in puntos if p["Sum"] > 0)
    datos["rpu_horas_mes"] = sum(h for d, h in por_dia if d >= inicio_mes.date())
    datos["rpu_por_dia"] = por_dia

    tabla([("EC2 de Airflow", datos["ec2"]), ("RDS for MySQL", datos["rds"]), ("DocumentDB", datos["docdb"]),
           ("Usuarios de QuickSight", datos["usuarios_qs"]), ("Autores de QuickSight", datos["autores_qs"]),
           ("Secretos en Secrets Manager", datos["secretos"]), ("Datos en S3 (todas las versiones)",
                                                               f"{datos['s3_gb'] * 1024:.1f} MB"),
           ("Redshift RPU-horas en el mes", f"{datos['rpu_horas_mes']:.2f}")], ("recurso", "estado"))
    if por_dia:
        print("\n  Redshift Serverless por dia (metrica ComputeSeconds de CloudWatch):")
        tabla([(d.isoformat(), f"{h:.2f}") for d, h in por_dia], ("dia UTC", "RPU-horas"))
    return datos


def escenarios(p: dict[str, float], u: dict) -> None:
    seccion("3. Escenarios (USD, precios de lista antes de capas gratuitas y creditos)")
    hora = p["ec2_t3_medium_hora"] + p["ipv4_publica_hora"] + p["rds_t4g_micro_hora"] + p["docdb_t3_medium_hora"]
    autores = max(u.get("autores_qs", 0), 1)
    fijo = (30 * p["ebs_gp3_gb_mes"] + 20 * p["rds_gp3_gb_mes"] + 1 * p["docdb_almacenamiento_gb_mes"]
            + u.get("secretos", 6) * p["secreto_mes"] + max(u.get("s3_gb", 0), 0.1) * p["s3_standard_gb_mes"]
            + 1 * p["redshift_rms_gb_mes"])
    qs = autores * p["quicksight_autor_mes"]
    rpu_lote = 0.5  # supuesto por lote del DW, comparar con la medicion de la seccion 2
    textos = 30 + 40 * 10  # carga inicial y 40 lotes del data lake con 10 textos nuevos cada uno
    comprehend = textos * 3 * 4 * p["comprehend_unidad"]  # 3 APIs, 4 unidades promedio por texto
    athena = 40 * 8 * 10 / 1024 ** 2 * p["athena_tb"]  # 8 consultas de validacion por lote, 10 MB minimo
    lab_horas, lab_lotes_dw = 20, 10
    lab = lab_horas * hora + lab_lotes_dw * rpu_lote * p["redshift_rpu_hora"] + comprehend + athena
    filas = [
        ("Hora con EC2, RDS y DocumentDB encendidos", f"{hora:.3f}", "EC2 + IPv4 publica + RDS + DocumentDB"),
        ("Lote del DW en Redshift (supuesto 0.5 RPU-h)", f"{rpu_lote * p['redshift_rpu_hora']:.3f}",
         "4 RPU durante unos 7 minutos de consultas"),
        ("Tope diario de Redshift (10 RPU-h)", f"{10 * p['redshift_rpu_hora']:.2f}",
         "el limite de uso desactiva el workgroup al llegar"),
        ("Mes con todo apagado", f"{fijo:.2f}", "discos, almacenamiento de bases, secretos y S3"),
        (f"QuickSight, {autores} autor(es)", f"{qs:.2f}", "0 durante la prueba de 30 dias"),
        (f"Mes de laboratorio ({lab_horas} h encendido, {lab_lotes_dw} lotes DW, 40 lotes DL)",
         f"{lab + fijo:.2f}", f"sin QuickSight; Comprehend {comprehend:.2f}, Athena {athena:.4f}"),
        ("Mes encendido sin parar (730 h)", f"{730 * hora + fijo:.2f}", "sin Redshift ni QuickSight"),
    ]
    tabla(filas, ("escenario", "USD", "composicion"))


def gasto_real(ses) -> None:
    seccion("4. Gasto real en Cost Explorer (USD, costo sin mezclar, antes de creditos)")
    ce = ses.client("ce", region_name="us-east-1")
    hoy = date.today()
    inicio = (hoy.replace(day=1) - timedelta(days=1)).replace(day=1)
    periodo = {"Start": inicio.isoformat(), "End": (hoy + timedelta(days=1)).isoformat()}
    try:
        resp = ce.get_cost_and_usage(TimePeriod=periodo, Granularity="MONTHLY", Metrics=["UnblendedCost"],
                                     GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
                                     Filter={"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}})
        filas = []
        for r in resp["ResultsByTime"]:
            mes = r["TimePeriod"]["Start"][:7]
            for g in sorted(r["Groups"], key=lambda g: -float(g["Metrics"]["UnblendedCost"]["Amount"])):
                monto = float(g["Metrics"]["UnblendedCost"]["Amount"])
                if monto >= 0.005:
                    filas.append((mes, g["Keys"][0], f"{monto:.2f}"))
        tabla(filas or [("-", "sin uso facturable", "0.00")], ("mes", "servicio", "USD"))

        resp = ce.get_cost_and_usage(TimePeriod=periodo, Granularity="MONTHLY", Metrics=["UnblendedCost"],
                                     GroupBy=[{"Type": "DIMENSION", "Key": "RECORD_TYPE"}])
        print("\n  Por tipo de registro (Credit son los creditos aplicados, en negativo):")
        tabla([(r["TimePeriod"]["Start"][:7], g["Keys"][0], f"{float(g['Metrics']['UnblendedCost']['Amount']):.2f}")
               for r in resp["ResultsByTime"] for g in r["Groups"]], ("mes", "tipo", "USD"))
    except ClientError as err:
        print(f"  Cost Explorer no respondio: {err.response['Error']['Code']} {err.response['Error']['Message']}")

    try:
        cuenta = ses.client("sts").get_caller_identity()["Account"]
        b = ses.client("budgets", region_name="us-east-1").describe_budget(AccountId=cuenta,
                                                                           BudgetName=PRESUPUESTO)["Budget"]
        gasto = b.get("CalculatedSpend", {})
        print(f"\n  Presupuesto {PRESUPUESTO}: limite {b['BudgetLimit']['Amount']} USD, "
              f"gasto real {gasto.get('ActualSpend', {}).get('Amount', '?')} USD, "
              f"pronostico {gasto.get('ForecastedSpend', {}).get('Amount', 'sin pronostico')} USD "
              "(sin creditos)")
    except ClientError as err:
        print(f"  Presupuesto no disponible: {err.response['Error']['Code']}")


def main() -> int:
    ses = boto3.Session()
    print(f"Costos del proyecto | {datetime.now():%Y-%m-%d %H:%M} | region {ses.region_name}")
    p = precios(ses)
    u = uso(ses)
    escenarios(p, u)
    gasto_real(ses)
    return 0


if __name__ == "__main__":
    sys.exit(main())
