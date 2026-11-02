import pytest

from ecommerce_dw import aws_conn
from ecommerce_dw.pipeline import quicksight


class FakeQs:
    def __init__(self, statuses):
        self.statuses, self.created = list(statuses), []

    def create_ingestion(self, **kw):
        self.created.append(kw)

    def describe_ingestion(self, **kw):
        status = self.statuses.pop(0)
        return {"Ingestion": {"IngestionStatus": status, "RowInfo": {"RowsIngested": 274}, "ErrorInfo": {"Message": "x"}}}


@pytest.fixture
def fake(monkeypatch):
    qs = FakeQs(["RUNNING", "COMPLETED"])

    class Sts:
        def get_caller_identity(self):
            return {"Account": "123"}

    import boto3
    monkeypatch.setattr(boto3, "client", lambda name: qs if name == "quicksight" else Sts())
    monkeypatch.setattr(quicksight.time, "sleep", lambda s: None)
    return qs


def test_sin_quicksight_se_omite(monkeypatch, fake):
    monkeypatch.setattr(aws_conn, "params", lambda: {})
    assert quicksight.main(["--lote", "x"]) == quicksight.EXIT_OMITIDO and fake.created == []


def test_refresco_completo_con_id_valido(monkeypatch, fake, capsys):
    monkeypatch.setattr(aws_conn, "params", lambda: {"quicksight_dataset_id": "arq-datos-ventas"})
    assert quicksight.main(["--lote", "scheduled__2026-11-01T11_00_00_00_00.x"]) == 0
    created = fake.created[0]
    assert created["DataSetId"] == "arq-datos-ventas" and created["IngestionType"] == "FULL_REFRESH"
    assert all(c.isalnum() or c in "-_" for c in created["IngestionId"])  # el punto no es valido en QuickSight
    assert "filas=274" in capsys.readouterr().out


def test_refresco_fallido_sale_con_1(monkeypatch, fake):
    fake.statuses = ["FAILED"]
    monkeypatch.setattr(aws_conn, "params", lambda: {"quicksight_dataset_id": "d"})
    assert quicksight.main(["--lote", "x"]) == 1
