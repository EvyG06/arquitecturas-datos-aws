"""Pruebas de infra/eliminar_todo.py: con moto para los servicios que emula y con Stubber para Redshift
Serverless y QuickSight, que moto no cubre. Se corren con: python -m pytest infra/tests -q"""
import importlib.util
import json
import sys
from pathlib import Path

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_aws

RUTA = Path(__file__).resolve().parents[1] / "eliminar_todo.py"
spec = importlib.util.spec_from_file_location("eliminar_todo", RUTA)
et = importlib.util.module_from_spec(spec)
sys.modules["eliminar_todo"] = et  # dataclass necesita el modulo registrado
spec.loader.exec_module(et)
et.ESPERA = 0

CUENTA = "123456789012"
REGION = "us-east-2"
CONFIANZA = json.dumps({"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]})


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    # moto no emula estos dos servicios: se prueban aparte con Stubber
    monkeypatch.setattr(et, "redshift", lambda ses: [])
    monkeypatch.setattr(et, "quicksight", lambda ses, cuenta: [])
    with mock_aws():
        yield boto3.Session(region_name=REGION)


def crear_todo(ses):
    ec2 = ses.client("ec2")
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    ids = {n: ec2.create_security_group(GroupName=n, Description=n, VpcId=vpc)["GroupId"] for n in et.GRUPOS}
    # referencias cruzadas como las reales: fuentes y redshift aceptan a airflow, quicksight sale hacia redshift
    for destino in ("arq-datos-dw-fuentes", "arq-datos-dw-redshift"):
        ec2.authorize_security_group_ingress(GroupId=ids[destino], IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 5439, "ToPort": 5439,
            "UserIdGroupPairs": [{"GroupId": ids["arq-datos-airflow"]}]}])
    ec2.authorize_security_group_egress(GroupId=ids["arq-datos-quicksight"], IpPermissions=[{
        "IpProtocol": "tcp", "FromPort": 5439, "ToPort": 5439,
        "UserIdGroupPairs": [{"GroupId": ids["arq-datos-dw-redshift"]}]}])
    ami = ec2.describe_images(Owners=["amazon"])["Images"][0]["ImageId"]
    ec2.run_instances(ImageId=ami, MinCount=1, MaxCount=1, InstanceType="t3.medium",
                      SecurityGroupIds=[ids["arq-datos-airflow"]],
                      TagSpecifications=[{"ResourceType": "instance", "Tags": [
                          {"Key": "Name", "Value": et.EC2_NOMBRE}, {"Key": "Project", "Value": et.PROYECTO}]}])

    subredes = [s["SubnetId"] for s in ec2.describe_subnets()["Subnets"]][:2]
    rds = ses.client("rds")
    rds.create_db_subnet_group(DBSubnetGroupName=et.RDS_SUBREDES, DBSubnetGroupDescription="x", SubnetIds=subredes)
    rds.create_db_parameter_group(DBParameterGroupName=et.RDS_PARAMETROS, DBParameterGroupFamily="mysql8.4",
                                  Description="x")
    rds.create_db_instance(DBInstanceIdentifier=et.RDS, Engine="mysql", DBInstanceClass="db.t4g.micro",
                           AllocatedStorage=20, MasterUsername="dw_admin", MasterUserPassword="x" * 12,
                           DBSubnetGroupName=et.RDS_SUBREDES, DBParameterGroupName=et.RDS_PARAMETROS)
    doc = ses.client("docdb")  # moto emula DocumentDB con el backend de RDS, que no acepta engine docdb
    doc.create_db_subnet_group(DBSubnetGroupName=et.DOCDB_SUBREDES, DBSubnetGroupDescription="x", SubnetIds=subredes)
    doc.create_db_cluster(DBClusterIdentifier=et.DOCDB, Engine="aurora-postgresql", MasterUsername="dw_admin",
                          MasterUserPassword="x" * 12, DBSubnetGroupName=et.DOCDB_SUBREDES)
    doc.create_db_instance(DBInstanceIdentifier=et.DOCDB_INSTANCIA, DBClusterIdentifier=et.DOCDB,
                           Engine="aurora-postgresql", DBInstanceClass="db.t3.medium")

    s3 = ses.client("s3")
    for nombre in (f"eve-dl-{CUENTA}-{REGION}", f"eve-dw-{CUENTA}-{REGION}"):
        s3.create_bucket(Bucket=nombre, CreateBucketConfiguration={"LocationConstraint": REGION})
        s3.put_bucket_versioning(Bucket=nombre, VersioningConfiguration={"Status": "Enabled"})
        for i in range(3):
            s3.put_object(Bucket=nombre, Key="raw/a.parquet", Body=f"v{i}".encode())
        s3.delete_object(Bucket=nombre, Key="raw/a.parquet")  # deja un delete marker

    glue = ses.client("glue")
    glue.create_database(DatabaseInput={"Name": et.GLUE_DB})
    glue.create_table(DatabaseName=et.GLUE_DB, TableInput={"Name": "silver_news"})
    ses.client("athena").create_work_group(Name=et.ATHENA_WG)
    ssm = ses.client("ssm")
    for i in range(12):  # mas de 10 obliga a borrar en tandas
        ssm.put_parameter(Name=f"/arq-datos/dw/p{i}", Value="v", Type="String")
    sm = ses.client("secretsmanager")
    for nombre in ("arq-datos/dw/mysql-lector", "arq-datos/dw/docdb-lector", "arq-datos/dw/redshift-bi-lector"):
        sm.create_secret(Name=nombre, SecretString="{}")
    sm.create_secret(Name="otro-proyecto/secreto", SecretString="{}")
    ses.client("logs").create_log_group(logGroupName="/aws/redshift/arq-datos-dw/connectionlog")
    ses.client("logs").create_log_group(logGroupName="/aws/redshift/otro/connectionlog")
    ses.client("budgets", region_name="us-east-1").create_budget(AccountId=CUENTA, Budget={
        "BudgetName": et.PRESUPUESTO, "BudgetLimit": {"Amount": "40", "Unit": "USD"},
        "TimeUnit": "MONTHLY", "BudgetType": "COST"})

    iam = ses.client("iam")
    for rol in et.ROLES:
        iam.create_role(RoleName=rol, AssumeRolePolicyDocument=CONFIANZA)
        iam.put_role_policy(RoleName=rol, PolicyName="en-linea", PolicyDocument=json.dumps({
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "*"}]}))
    gestionada = iam.create_policy(PolicyName="ssm-core", PolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "ssm:*", "Resource": "*"}]}))
    iam.attach_role_policy(RoleName="arq-datos-ec2", PolicyArn=gestionada["Policy"]["Arn"])
    iam.create_instance_profile(InstanceProfileName=et.PERFIL_EC2)
    iam.add_role_to_instance_profile(InstanceProfileName=et.PERFIL_EC2, RoleName="arq-datos-ec2")


def test_inventario_no_modifica_nada(aws, capsys):
    crear_todo(aws)
    assert et.main([]) == 0
    salida = capsys.readouterr().out
    assert "inventario, no se modifica nada" in salida
    assert "eve-dw-123456789012-us-east-2" in salida
    assert "otro-proyecto/secreto" not in salida
    assert "/aws/redshift/otro/connectionlog" not in salida
    assert len(et.inventario(aws, CUENTA, REGION, False)) > 15  # todo sigue ahi


def test_confirmacion_incorrecta_no_borra(aws, monkeypatch):
    crear_todo(aws)
    monkeypatch.setattr("builtins.input", lambda _: "ELIMINAR")
    antes = len(et.inventario(aws, CUENTA, REGION, False))
    assert et.main(["--ejecutar"]) == 1
    assert len(et.inventario(aws, CUENTA, REGION, False)) == antes


def test_ejecutar_borra_todo_menos_el_presupuesto(aws, monkeypatch):
    crear_todo(aws)
    monkeypatch.setattr("builtins.input", lambda _: f"ELIMINAR {CUENTA}")
    assert et.main(["--ejecutar"]) == 0
    assert et.inventario(aws, CUENTA, REGION, False) == []
    assert aws.client("budgets", region_name="us-east-1").describe_budget(
        AccountId=CUENTA, BudgetName=et.PRESUPUESTO)["Budget"]
    nombres = [s["Name"] for s in aws.client("secretsmanager").list_secrets()["SecretList"]]
    assert nombres == ["otro-proyecto/secreto"]
    grupos = aws.client("logs").describe_log_groups(logGroupNamePrefix="/aws/redshift/")["logGroups"]
    assert [g["logGroupName"] for g in grupos] == ["/aws/redshift/otro/connectionlog"]
    # idempotente: una segunda corrida no encuentra nada y no falla
    assert et.main(["--ejecutar"]) == 0


def test_incluir_presupuesto(aws, monkeypatch):
    crear_todo(aws)
    monkeypatch.setattr("builtins.input", lambda _: f"ELIMINAR {CUENTA}")
    assert et.main(["--ejecutar", "--incluir-presupuesto"]) == 0
    assert et.inventario(aws, CUENTA, REGION, True) == []


def test_redshift_borra_topes_workgroup_y_namespace(monkeypatch):
    ses = boto3.Session(region_name=REGION, aws_access_key_id="x", aws_secret_access_key="x")
    rs = ses.client("redshift-serverless")
    wg = {"workgroupName": et.REDSHIFT, "workgroupArn": "arn:wg", "workgroupId": "w1", "status": "AVAILABLE",
          "baseCapacity": 4}
    ns = {"namespaceName": et.REDSHIFT, "namespaceArn": "arn:ns", "namespaceId": "n1", "status": "AVAILABLE"}
    st = Stubber(rs)
    st.add_response("get_workgroup", {"workgroup": wg}, {"workgroupName": et.REDSHIFT})
    st.add_response("list_usage_limits", {"usageLimits": [{"usageLimitId": "u1"}]}, {"resourceArn": "arn:wg"})
    st.add_response("get_namespace", {"namespace": ns}, {"namespaceName": et.REDSHIFT})
    st.add_response("list_snapshots", {"snapshots": []}, {"namespaceName": et.REDSHIFT})
    st.add_response("delete_usage_limit", {}, {"usageLimitId": "u1"})
    st.add_client_error("delete_workgroup", "ConflictException", http_status_code=409)  # ocupado: reintenta
    st.add_response("delete_workgroup", {"workgroup": wg}, {"workgroupName": et.REDSHIFT})
    st.add_client_error("get_workgroup", "ResourceNotFoundException", http_status_code=404)
    st.add_response("delete_namespace", {"namespace": ns}, {"namespaceName": et.REDSHIFT})
    st.add_client_error("get_namespace", "ResourceNotFoundException", http_status_code=404)
    st.activate()
    monkeypatch.setattr(ses, "client", lambda nombre, **kw: rs)
    recursos = et.redshift(ses)
    assert [(r.fase, r.tipo.split()[2]) for r in recursos] == [(1, "workgroup"), (2, "namespace")]
    assert et.ejecutar(recursos) == []
    st.assert_no_pending_responses()


def test_quicksight_objetos_vpc_y_suscripcion(monkeypatch):
    ses = boto3.Session(region_name=REGION, aws_access_key_id="x", aws_secret_access_key="x")
    qs = ses.client("quicksight")
    st = Stubber(qs)
    cta = {"AwsAccountId": CUENTA}
    st.add_response("describe_account_subscription", {"AccountInfo": {
        "AccountName": "eve-arq", "Edition": "ENTERPRISE", "AccountSubscriptionStatus": "ACCOUNT_CREATED"}}, cta)
    st.add_response("describe_dashboard", {"Dashboard": {"DashboardId": et.QS_DASHBOARD}},
                    {**cta, "DashboardId": et.QS_DASHBOARD})
    st.add_response("describe_analysis", {"Analysis": {"AnalysisId": et.QS_ANALISIS, "Status": "DELETED"}},
                    {**cta, "AnalysisId": et.QS_ANALISIS})  # ya en papelera: no se vuelve a borrar
    st.add_client_error("describe_data_set", "ResourceNotFoundException", http_status_code=404)
    st.add_response("describe_data_source", {"DataSource": {"DataSourceId": et.QS_FUENTE}},
                    {**cta, "DataSourceId": et.QS_FUENTE})
    st.add_response("describe_vpc_connection", {"VPCConnection": {"Status": "CREATION_SUCCESSFUL"}},
                    {**cta, "VPCConnectionId": et.QS_VPC})
    st.add_response("delete_dashboard", {}, {**cta, "DashboardId": et.QS_DASHBOARD})
    st.add_response("delete_data_source", {}, {**cta, "DataSourceId": et.QS_FUENTE})
    st.add_response("delete_vpc_connection", {}, {**cta, "VPCConnectionId": et.QS_VPC})
    st.add_response("describe_vpc_connection", {"VPCConnection": {"Status": "DELETION_IN_PROGRESS"}},
                    {**cta, "VPCConnectionId": et.QS_VPC})
    st.add_response("describe_vpc_connection", {"VPCConnection": {"Status": "DELETED"}},
                    {**cta, "VPCConnectionId": et.QS_VPC})
    st.add_response("update_account_settings", {},
                    {**cta, "DefaultNamespace": "default", "TerminationProtectionEnabled": False})
    st.add_response("delete_account_subscription", {}, cta)
    st.activate()
    monkeypatch.setattr(ses, "client", lambda nombre, **kw: qs)
    recursos = et.quicksight(ses, CUENTA)
    assert [r.nombre for r in recursos] == [et.QS_DASHBOARD, et.QS_FUENTE, et.QS_VPC, "eve-arq"]
    assert et.ejecutar(recursos) == []
    st.assert_no_pending_responses()


def test_no_cubiertos_detecta_recursos_etiquetados_fuera_del_inventario():
    r = et.Recurso(1, "x", "arq-datos-dw-mysql", "", et.nada, claves=["arq-datos-dw-mysql"])
    arns = ["arn:aws:rds:us-east-2:1:db:arq-datos-dw-mysql", "arn:aws:sns:us-east-2:1:alarma",
            f"arn:aws:budgets::1:budget/{et.PRESUPUESTO}"]
    assert et.no_cubiertos(arns, [r]) == ["arn:aws:sns:us-east-2:1:alarma"]
