"""Escrituras a S3 con garantias: crear solo si no existe y compare-and-swap con ETag.

Es la misma tecnica del data lake. Se duplica a proposito porque el DW y el DL son paquetes que se
despliegan en venvs separados y no deben depender uno del otro.
"""
from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError


def _http(err: ClientError) -> int:
    return err.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)


def _json(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8")


def put_once(s3, bucket: str, key: str, body: bytes, content_type: str) -> bool:
    """Crea el objeto solo si la llave no existe (If-None-Match: *). True si lo creo, False si ya existia."""
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
        return True
    except ClientError as err:
        if _http(err) == 412:
            return False
        raise


def put_json_once(s3, bucket: str, key: str, obj: Any) -> bool:
    return put_once(s3, bucket, key, _json(obj), "application/json")


def head(s3, bucket: str, key: str) -> dict | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as err:
        if _http(err) == 404:
            return None
        raise


def read_bytes(s3, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def read_json(s3, bucket: str, key: str) -> Any | None:
    return read_json_etag(s3, bucket, key)[0]


def read_json_etag(s3, bucket: str, key: str) -> tuple[Any | None, str | None]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read()), obj["ETag"]
    except ClientError as err:
        if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None, None
        raise


class ConcurrentUpdate(RuntimeError):
    """Otro proceso modifico el objeto entre la lectura y la escritura."""


def put_json_cas(s3, bucket: str, key: str, obj: Any, etag: str | None) -> None:
    """Solo escribe si el objeto sigue como cuando se leyo (If-Match), o si no existia (If-None-Match)."""
    condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=_json(obj), ContentType="application/json", **condition)
    except ClientError as err:
        if _http(err) in (404, 409, 412):
            raise ConcurrentUpdate(f"{key} cambio desde que se leyo") from err
        raise
