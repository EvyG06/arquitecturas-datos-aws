#!/usr/bin/env python3
"""Fuente de datos, dataset SPICE, analisis y dashboard de QuickSight. Corre en el Mac (admin). Idempotente.

La contrasena de bi_lector se lee de Secrets Manager y va directo a la API: no pasa por la terminal ni por los
argumentos de ningun proceso. El dashboard lee solo marts.ventas_dashboard, la unica tabla que bi_lector puede leer.
Con --validar solo arma las peticiones y las valida contra el modelo de la API, sin llamar a AWS.
Con --compartir USUARIO da a otro usuario de QuickSight los mismos permisos de dueno sobre los cuatro recursos.
"""
from __future__ import annotations

import argparse
import json
import time

import boto3

DATA_SOURCE_ID = "arq-datos-dw-redshift"
DATASET_ID = "arq-datos-ventas"
ANALYSIS_ID = "arq-datos-ventas-analisis"
DASHBOARD_ID = "arq-datos-ventas"
DS = "ventas"  # identificador del dataset dentro de la definicion
TAGS = [{"Key": "Project", "Value": "arquitecturas-datos"}, {"Key": "Component", "Value": "dw"}]

COLUMNS = [("sale_sk", "STRING"), ("fecha", "DATETIME"), ("anio", "INTEGER"), ("mes", "INTEGER"),
           ("categoria", "STRING"), ("producto", "STRING"), ("canal", "STRING"), ("departamento", "STRING"),
           ("ciudad", "STRING"), ("cantidad", "INTEGER"), ("subtotal", "DECIMAL"), ("actualizado_en", "DATETIME")]

ACTIONS = {
    "data_source": ["quicksight:DescribeDataSource", "quicksight:DescribeDataSourcePermissions",
                    "quicksight:PassDataSource", "quicksight:UpdateDataSource", "quicksight:DeleteDataSource",
                    "quicksight:UpdateDataSourcePermissions"],
    "dataset": ["quicksight:DescribeDataSet", "quicksight:DescribeDataSetPermissions", "quicksight:PassDataSet",
                "quicksight:DescribeIngestion", "quicksight:ListIngestions", "quicksight:UpdateDataSet",
                "quicksight:DeleteDataSet", "quicksight:CreateIngestion", "quicksight:CancelIngestion",
                "quicksight:UpdateDataSetPermissions"],
    "analysis": ["quicksight:RestoreAnalysis", "quicksight:UpdateAnalysisPermissions", "quicksight:DeleteAnalysis",
                 "quicksight:DescribeAnalysisPermissions", "quicksight:QueryAnalysis", "quicksight:DescribeAnalysis",
                 "quicksight:UpdateAnalysis"],
    "dashboard": ["quicksight:DescribeDashboard", "quicksight:ListDashboardVersions",
                  "quicksight:UpdateDashboardPermissions", "quicksight:QueryDashboard", "quicksight:UpdateDashboard",
                  "quicksight:DeleteDashboard", "quicksight:DescribeDashboardPermissions",
                  "quicksight:UpdateDashboardPublishedVersion"],
}


def col(name: str) -> dict:
    return {"DataSetIdentifier": DS, "ColumnName": name}


def title(text: str) -> dict:
    return {"Visibility": "VISIBLE", "FormatText": {"PlainText": text}}


def total(field_id: str) -> dict:
    return {"NumericalMeasureField": {"FieldId": field_id, "Column": col("subtotal"),
                                      "AggregationFunction": {"SimpleNumericalAggregation": "SUM"}}}


def element(element_id: str, kind: str, column: int, row: int, width: int, height: int) -> dict:
    return {"ElementId": element_id, "ElementType": kind, "ColumnIndex": column, "RowIndex": row,
            "ColumnSpan": width, "RowSpan": height}


def category_filter(group: str, filter_id: str, column: str) -> dict:
    return {
        "FilterGroupId": group,
        "Filters": [{"CategoryFilter": {"FilterId": filter_id, "Column": col(column), "Configuration": {
            "FilterListConfiguration": {"MatchOperator": "CONTAINS", "SelectAllOptions": "FILTER_ALL_VALUES"}}}}],
        "ScopeConfiguration": {"SelectedSheets": {"SheetVisualScopingConfigurations": [
            {"SheetId": "ventas", "Scope": "ALL_VISUALS"}]}},
        "CrossDataset": "SINGLE_DATASET",
        "Status": "ENABLED",
    }


def definition(dataset_arn: str) -> dict:
    visuals = [
        {"KPIVisual": {"VisualId": "kpi_ventas", "Title": title("Ventas totales (subtotal de lineas validas)"),
                       "ChartConfiguration": {"FieldWells": {"Values": [total("kpi_subtotal")]}}}},
        {"TableVisual": {"VisualId": "tabla_canal", "Title": title("Ventas por canal y fecha de actualizacion"),
                         "ChartConfiguration": {"FieldWells": {"TableAggregatedFieldWells": {
                             "GroupBy": [{"CategoricalDimensionField": {"FieldId": "tabla_canal_dim", "Column": col("canal")}}],
                             "Values": [total("tabla_subtotal"),
                                        {"DateMeasureField": {"FieldId": "tabla_actualizado", "Column": col("actualizado_en"),
                                                              "AggregationFunction": "MAX"}}]}}}}},
        {"LineChartVisual": {"VisualId": "linea_fecha", "Title": title("Ventas por mes"),
                             "ChartConfiguration": {"FieldWells": {"LineChartAggregatedFieldWells": {
                                 "Category": [{"DateDimensionField": {"FieldId": "linea_mes", "Column": col("fecha"),
                                                                      "DateGranularity": "MONTH"}}],
                                 "Values": [total("linea_subtotal")]}}}}},
        {"BarChartVisual": {"VisualId": "barras_categoria", "Title": title("Ventas por categoria"),
                            "ChartConfiguration": {"Orientation": "HORIZONTAL", "FieldWells": {"BarChartAggregatedFieldWells": {
                                "Category": [{"CategoricalDimensionField": {"FieldId": "barras_cat", "Column": col("categoria")}}],
                                "Values": [total("barras_subtotal")]}}}}},
    ]
    return {
        "DataSetIdentifierDeclarations": [{"Identifier": DS, "DataSetArn": dataset_arn}],
        "Sheets": [{
            "SheetId": "ventas",
            "Name": "Ventas",
            "Visuals": visuals,
            "FilterControls": [
                {"Dropdown": {"FilterControlId": "control_canal", "Title": "Canal de venta",
                              "SourceFilterId": "filtro_canal", "Type": "MULTI_SELECT"}},
                {"Dropdown": {"FilterControlId": "control_categoria", "Title": "Categoria",
                              "SourceFilterId": "filtro_categoria", "Type": "MULTI_SELECT"}},
            ],
            "SheetControlLayouts": [{"Configuration": {"GridLayout": {"Elements": [
                element("control_canal", "FILTER_CONTROL", 0, 0, 6, 1),
                element("control_categoria", "FILTER_CONTROL", 6, 0, 6, 1),
            ]}}}],
            "Layouts": [{"Configuration": {"GridLayout": {"Elements": [
                element("kpi_ventas", "VISUAL", 0, 0, 12, 6),
                element("tabla_canal", "VISUAL", 12, 0, 24, 6),
                element("linea_fecha", "VISUAL", 0, 6, 18, 12),
                element("barras_categoria", "VISUAL", 18, 6, 18, 12),
            ]}}}],
        }],
        "FilterGroups": [category_filter("fg_canal", "filtro_canal", "canal"),
                         category_filter("fg_categoria", "filtro_categoria", "categoria")],
    }


def requests(account: str, region: str, p: dict, password: str | None) -> dict:
    """Todas las peticiones, para crearlas o validarlas sin enviarlas."""
    user = [{"Principal": p["quicksight_user_arn"], "Actions": None}]
    perms = lambda kind: [{**u, "Actions": ACTIONS[kind]} for u in user]  # noqa: E731
    ds_arn = f"arn:aws:quicksight:{region}:{account}:datasource/{DATA_SOURCE_ID}"
    dataset_arn = f"arn:aws:quicksight:{region}:{account}:dataset/{DATASET_ID}"
    base = {"AwsAccountId": account}
    data_source = {**base, "DataSourceId": DATA_SOURCE_ID, "Name": "DW Redshift (ecommerce_dw)",
                   "DataSourceParameters": {"RedshiftParameters": {"Host": p["redshift_host"], "Port": 5439,
                                                                    "Database": p["redshift_database"]}},
                   "Credentials": {"CredentialPair": {"Username": "bi_lector", "Password": password or "x" * 32}},
                   "VpcConnectionProperties": {"VpcConnectionArn": p["quicksight_vpc_connection_arn"]},
                   "SslProperties": {"DisableSsl": False}}
    dataset = {**base, "DataSetId": DATASET_ID, "Name": "Ventas (marts.ventas_dashboard)", "ImportMode": "SPICE",
               "PhysicalTableMap": {"ventas": {"RelationalTable": {
                   "DataSourceArn": ds_arn, "Schema": "marts", "Name": "ventas_dashboard",
                   "InputColumns": [{"Name": n, "Type": t} for n, t in COLUMNS]}}}}
    d = definition(dataset_arn)
    return {
        "data_source": (data_source, perms("data_source")),
        "dataset": (dataset, perms("dataset")),
        "analysis": ({**base, "AnalysisId": ANALYSIS_ID, "Name": "Ventas del DW", "Definition": d}, perms("analysis")),
        "dashboard": ({**base, "DashboardId": DASHBOARD_ID, "Name": "Ventas del DW", "Definition": d,
                       "DashboardPublishOptions": {"AdHocFilteringOption": {"AvailabilityStatus": "ENABLED"},
                                                   "ExportToCSVOption": {"AvailabilityStatus": "ENABLED"}}},
                      perms("dashboard")),
    }


OPS = {"data_source": ("CreateDataSource", "UpdateDataSource"), "dataset": ("CreateDataSet", "UpdateDataSet"),
       "analysis": ("CreateAnalysis", "UpdateAnalysis"), "dashboard": ("CreateDashboard", "UpdateDashboard")}


def create_extra(kind: str) -> dict:
    """Campos que solo lleva la creacion: el tipo de fuente no se puede cambiar despues."""
    return {"Type": "REDSHIFT"} if kind == "data_source" else {}


def validate(reqs: dict) -> None:
    import botocore.session
    from botocore.validate import validate_parameters

    model = botocore.session.get_session().get_service_model("quicksight")
    for kind, (req, perms) in reqs.items():
        create, update = OPS[kind]
        validate_parameters({**req, **create_extra(kind), "Permissions": perms, "Tags": TAGS},
                            model.operation_model(create).input_shape)
        validate_parameters(req, model.operation_model(update).input_shape)
        print(f"ok  {create} y {update}")


SHARE = {  # recurso: (metodo de boto3, operacion de la API, parametro de id, id)
    "data_source": ("update_data_source_permissions", "UpdateDataSourcePermissions", "DataSourceId", DATA_SOURCE_ID),
    "dataset": ("update_data_set_permissions", "UpdateDataSetPermissions", "DataSetId", DATASET_ID),
    "analysis": ("update_analysis_permissions", "UpdateAnalysisPermissions", "AnalysisId", ANALYSIS_ID),
    "dashboard": ("update_dashboard_permissions", "UpdateDashboardPermissions", "DashboardId", DASHBOARD_ID),
}


def share(qs, account: str, user_name: str, validate_only: bool = False) -> None:
    """Agrega permisos de dueno para otro usuario de QuickSight, sin quitar los que ya existen."""
    if validate_only:
        import botocore.session
        from botocore.validate import validate_parameters

        model = botocore.session.get_session().get_service_model("quicksight")
        arn = f"arn:aws:quicksight:us-east-2:{account}:user/default/{user_name}"
    else:
        arn = qs.describe_user(AwsAccountId=account, Namespace="default", UserName=user_name)["User"]["Arn"]
    for kind, (method, operation, key, ident) in SHARE.items():
        req = {"AwsAccountId": account, key: ident, "GrantPermissions": [{"Principal": arn, "Actions": ACTIONS[kind]}]}
        if validate_only:
            validate_parameters(req, model.operation_model(operation).input_shape)
            print(f"ok  {operation}")
        else:
            getattr(qs, method)(**req)
            print(f"{kind}: permisos de dueno para {arn}")


def wait(fn, done, failed, what: str, timeout: int = 600):
    start = time.monotonic()
    while True:
        status, detail = fn()
        if status in done:
            return status
        if status in failed:
            raise RuntimeError(f"{what}: {status} {json.dumps(detail, default=str)[:1500]}")
        if time.monotonic() - start > timeout:
            raise RuntimeError(f"{what}: sigue en {status} despues de {timeout} s")
        time.sleep(5)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--validar", action="store_true", help="Valida las peticiones contra el modelo, sin AWS")
    parser.add_argument("--compartir", metavar="USUARIO", help="Usuario de QuickSight que recibe permisos de dueno")
    args = parser.parse_args()
    if args.validar:
        p = {"quicksight_user_arn": "arn:aws:quicksight:us-east-2:1:user/default/x", "redshift_host": "h",
             "redshift_database": "db", "quicksight_vpc_connection_arn": "arn:aws:quicksight:us-east-2:1:vpcConnection/v"}
        validate(requests("123456789012", "us-east-2", p, None))
        share(None, "123456789012", "726168540074", validate_only=True)
        return 0
    if args.compartir:
        share(boto3.client("quicksight"), boto3.client("sts").get_caller_identity()["Account"], args.compartir)
        print(f"\nAbre https://{boto3.session.Session().region_name}.quicksight.aws.amazon.com/sn/dashboards/{DASHBOARD_ID}")
        return 0

    session = boto3.session.Session()
    region, account = session.region_name, boto3.client("sts").get_caller_identity()["Account"]
    ssm, sm, qs = boto3.client("ssm"), boto3.client("secretsmanager"), boto3.client("quicksight")
    p = {x["Name"].rsplit("/", 1)[1]: x["Value"] for page in ssm.get_paginator("get_parameters_by_path").paginate(
        Path="/arq-datos/dw/") for x in page["Parameters"]}
    password = json.loads(sm.get_secret_value(SecretId=p["redshift_bi_secret_arn"])["SecretString"])["password"]
    reqs = requests(account, region, p, password)

    # Fuente de datos: si existe se actualiza, asi la contrasena queda sincronizada con el secreto.
    req, perms = reqs["data_source"]
    try:
        qs.create_data_source(**req, **create_extra("data_source"), Permissions=perms, Tags=TAGS)
        print("fuente de datos creada")
    except qs.exceptions.ResourceExistsException:
        qs.update_data_source(**req)
        print("fuente de datos actualizada")
    wait(lambda: (lambda d: (d["Status"], d.get("ErrorInfo")))(
        qs.describe_data_source(AwsAccountId=account, DataSourceId=DATA_SOURCE_ID)["DataSource"]),
        ("CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL"), ("CREATION_FAILED", "UPDATE_FAILED"), "fuente de datos")

    req, perms = reqs["dataset"]
    try:
        qs.create_data_set(**req, Permissions=perms, Tags=TAGS)
        print("dataset SPICE creado")
    except qs.exceptions.ResourceExistsException:
        qs.update_data_set(**req)
        print("dataset SPICE actualizado")
    ssm.put_parameter(Name="/arq-datos/dw/quicksight_dataset_id", Value=DATASET_ID, Type="String", Overwrite=True)

    ingestion = f"inicial-{int(time.time())}"
    qs.create_ingestion(AwsAccountId=account, DataSetId=DATASET_ID, IngestionId=ingestion, IngestionType="FULL_REFRESH")
    status = wait(lambda: (lambda i: (i["IngestionStatus"], i.get("ErrorInfo")))(
        qs.describe_ingestion(AwsAccountId=account, DataSetId=DATASET_ID, IngestionId=ingestion)["Ingestion"]),
        ("COMPLETED",), ("FAILED", "CANCELLED"), "carga SPICE")
    rows = qs.describe_ingestion(AwsAccountId=account, DataSetId=DATASET_ID, IngestionId=ingestion)["Ingestion"].get("RowInfo", {})
    print(f"SPICE {status}: {rows.get('RowsIngested')} filas, {rows.get('RowsDropped', 0)} descartadas")

    for kind, describe, key, ident in (("analysis", qs.describe_analysis, "Analysis", "AnalysisId"),
                                       ("dashboard", qs.describe_dashboard, "Dashboard", "DashboardId")):
        req, perms = reqs[kind]
        create = qs.create_analysis if kind == "analysis" else qs.create_dashboard
        update = qs.update_analysis if kind == "analysis" else qs.update_dashboard
        try:
            create(**req, Permissions=perms, Tags=TAGS)
            print(f"{kind} creado")
        except qs.exceptions.ResourceExistsException:
            update(**req)
            print(f"{kind} actualizado")

        def state():
            obj = describe(AwsAccountId=account, **{ident: req[ident]})[key]
            if kind == "dashboard":
                v = obj["Version"]
                return v["Status"], v.get("Errors")
            return obj["Status"], obj.get("Errors")
        wait(state, ("CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL"), ("CREATION_FAILED", "UPDATE_FAILED"), kind)
    version = qs.describe_dashboard(AwsAccountId=account, DashboardId=DASHBOARD_ID)["Dashboard"]["Version"]["VersionNumber"]
    qs.update_dashboard_published_version(AwsAccountId=account, DashboardId=DASHBOARD_ID, VersionNumber=version)
    print(f"\ndashboard publicado (version {version}):")
    print(f"  https://{region}.quicksight.aws.amazon.com/sn/dashboards/{DASHBOARD_ID}")
    print(f"analisis para editarlo:\n  https://{region}.quicksight.aws.amazon.com/sn/analyses/{ANALYSIS_ID}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
