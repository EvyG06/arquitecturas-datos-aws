"""Cliente minimo de la Redshift Data API.

La Data API autentica con IAM: con WorkgroupName y sin secreto, Redshift Serverless mapea el rol que llama a
un usuario de base de datos (IAMR:<rol>). No hay contrasenas, ni driver, ni puerto abierto para la carga.
"""
from __future__ import annotations

import time
from typing import Callable

TERMINAL_OK = "FINISHED"
TERMINAL_ERROR = ("FAILED", "ABORTED")


class RedshiftError(RuntimeError):
    """Una sentencia termino en FAILED o ABORTED. El mensaje incluye la sentencia y el error de Redshift."""


def _value(field: dict):
    if field.get("isNull"):
        return None
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue", "blobValue"):
        if key in field:
            return field[key]
    return None


class DataApi:
    def __init__(self, client, workgroup: str, database: str, secret_arn: str | None = None,
                 timeout: float = 900, sleep: Callable[[float], None] = time.sleep):
        self.client, self.timeout, self.sleep = client, timeout, sleep
        self.target = {"WorkgroupName": workgroup, "Database": database}
        if secret_arn:
            self.target["SecretArn"] = secret_arn

    def batch(self, sqls: list[str], name: str) -> dict:
        """Corre todas las sentencias en una sola transaccion: si una falla, no queda ninguna aplicada."""
        if not 1 <= len(sqls) <= 40:
            raise ValueError(f"BatchExecuteStatement acepta de 1 a 40 sentencias, se pidieron {len(sqls)}")
        statement = self.client.batch_execute_statement(Sqls=sqls, StatementName=name[:500], **self.target)
        return self.wait(statement["Id"])

    def execute(self, sql: str, name: str) -> dict:
        statement = self.client.execute_statement(Sql=sql, StatementName=name[:500], **self.target)
        return self.wait(statement["Id"])

    def query(self, sql: str, name: str = "consulta") -> list[list]:
        description = self.execute(sql, name)
        if not description.get("HasResultSet"):
            return []
        rows, token = [], None
        while True:
            page = self.client.get_statement_result(Id=description["Id"], **({"NextToken": token} if token else {}))
            rows.extend([_value(field) for field in record] for record in page["Records"])
            token = page.get("NextToken")
            if not token:
                return rows

    def wait(self, statement_id: str) -> dict:
        deadline = time.monotonic() + self.timeout
        delay = 0.5
        while True:
            description = self.client.describe_statement(Id=statement_id)
            status = description["Status"]
            if status == TERMINAL_OK:
                return description
            if status in TERMINAL_ERROR:
                raise RedshiftError(self._error_message(description))
            if time.monotonic() > deadline:
                self.client.cancel_statement(Id=statement_id)
                raise RedshiftError(f"La sentencia {statement_id} supero {self.timeout:.0f} s y se cancelo")
            self.sleep(delay)
            delay = min(delay * 1.5, 5)

    @staticmethod
    def _error_message(description: dict) -> str:
        message = f"{description['Status']} en {description.get('Id')}: {description.get('Error', 'sin detalle')}"
        for sub in description.get("SubStatements", []):
            if sub.get("Status") in TERMINAL_ERROR:
                sql = " ".join(sub.get("QueryString", "").split())[:300]
                message += f"\n  sentencia que fallo: {sql}\n  error: {sub.get('Error', 'sin detalle')}"
                break
        return message
