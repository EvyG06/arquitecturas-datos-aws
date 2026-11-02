"""Lectura de las fuentes con los usuarios de solo lectura y una ventana (desde, hasta] sobre _cdc_ts."""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import datetime, timezone
from functools import partial
from typing import Callable, Iterator

from .entities import Entity

UTC = timezone.utc
Fetch = Callable[[Entity, "datetime | None", datetime], Iterator[dict]]


def _naive_utc(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


class MySQLReader:
    """RDS for MySQL. Las cuatro tablas se leen dentro de un mismo snapshot consistente de InnoDB, asi un
    pedido y sus items nunca quedan partidos entre dos lotes por haberse leido en momentos distintos."""

    source = "mysql"

    def __init__(self, connect: Callable | None = None):
        self._connect = connect or self._default_connect

    @staticmethod
    def _default_connect():
        from pymysql.cursors import SSDictCursor

        from ecommerce_dw import aws_conn

        return aws_conn.mysql_connect(lector=True, cursorclass=SSDictCursor)

    def clock(self) -> datetime:
        """Hora del servidor, que es el mismo reloj que escribe _cdc_ts."""
        with closing(self._connect()) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT UTC_TIMESTAMP(6) AS ahora")
            return cursor.fetchone()["ahora"].replace(tzinfo=UTC)

    @contextmanager
    def snapshot(self) -> Iterator[Fetch]:
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                # TIMESTAMP se compara en la zona de la sesion: se fija en UTC para no depender del servidor.
                cursor.execute("SET time_zone = '+00:00'")
                cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            yield partial(self._rows, conn)
            conn.rollback()
        finally:
            conn.close()

    @staticmethod
    def _rows(conn, entity: Entity, low: datetime | None, high: datetime) -> Iterator[dict]:
        where, args = "_cdc_ts <= %s", [_naive_utc(high)]
        if low is not None:
            where += " AND _cdc_ts > %s"
            args.append(_naive_utc(low))
        # Los nombres vienen del contrato fijo de entities.py, nunca del usuario. Los valores van parametrizados.
        sql = (f"SELECT {', '.join(entity.columns)}, _cdc_ts FROM {entity.name} "
               f"WHERE {where} ORDER BY {entity.key}")
        with conn.cursor() as cursor:
            cursor.execute(sql, args)
            for row in cursor:
                row["_cdc_ts"] = row["_cdc_ts"].replace(tzinfo=UTC)
                if row.get("is_active") is not None:
                    row["is_active"] = bool(row["is_active"])
                yield row


class DocumentDBReader:
    """DocumentDB. Lee del primario: un secundario con retraso podria no tener aun documentos de la ventana."""

    source = "documentdb"
    database = "ecommerce"

    def __init__(self, client_factory: Callable | None = None):
        self._client_factory = client_factory or self._default_client

    @staticmethod
    def _default_client():
        from ecommerce_dw import aws_conn

        return aws_conn.mongo_client(lector=True, tz_aware=True)

    def clock(self) -> datetime:
        """_cdc_ts lo pone el generador en la misma EC2, asi que el reloj de referencia es el local."""
        return datetime.now(UTC)

    @contextmanager
    def snapshot(self) -> Iterator[Fetch]:
        client = self._client_factory()
        try:
            yield partial(self._rows, client[self.database])
        finally:
            client.close()

    @staticmethod
    def window_filter(low: datetime | None, high: datetime) -> dict:
        if low is not None:
            return {"_cdc_ts": {"$gt": low, "$lte": high}}
        # La extraccion inicial tambien toma documentos anteriores a la marca CDC.
        return {"$or": [{"_cdc_ts": {"$lte": high}}, {"_cdc_ts": {"$exists": False}}]}

    @classmethod
    def _rows(cls, db, entity: Entity, low: datetime | None, high: datetime) -> Iterator[dict]:
        from pymongo import ReadPreference

        from ecommerce_dw.ingestion.raw_sources import flatten_mongo

        collection = db.get_collection(entity.name, read_preference=ReadPreference.PRIMARY)
        for document in collection.find(cls.window_filter(low, high)).sort("_id", 1).batch_size(1000):
            # Mismo aplanado que la version local: campos de negocio mas el documento completo en JSON.
            _, values = flatten_mongo(entity.name, document, None)
            row = dict(zip(entity.columns, values[:len(entity.columns)]))
            row["_cdc_ts"] = document.get("_cdc_ts")
            yield row


def default_readers() -> dict:
    return {MySQLReader.source: MySQLReader(), DocumentDBReader.source: DocumentDBReader()}
