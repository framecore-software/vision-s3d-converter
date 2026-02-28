from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from orchestrator.config import settings

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    """
    Retorna el cliente boto3 compartido, creándolo de forma lazy la primera vez.

    La creación lazy evita dos problemas del cliente global en tiempo de importación:
    1. Si las credenciales R2 están vacías (e.g. en el servicio progress-sse), el
       módulo se importa sin error; el fallo ocurrirá solo cuando se intente usar R2.
    2. Si las credenciales rotan, basta reiniciar el proceso — no hay estado cacheado
       en import time.

    boto3 es thread-safe para llamadas concurrentes sobre el mismo cliente.
    """
    global _client
    if _client is None:
        if not settings.r2_endpoint or not settings.r2_access_key:
            raise RuntimeError(
                "Credenciales R2 no configuradas. "
                "Define R2_ENDPOINT, R2_ACCESS_KEY y R2_SECRET_KEY en el entorno."
            )
        _client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint,
            aws_access_key_id=settings.r2_access_key,
            aws_secret_access_key=settings.r2_secret_key,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "adaptive"},
                # Timeout de conexión: 30s para establecer el TCP.
                # Timeout de lectura: 300s para transferencias de archivos grandes.
                # Sin estos timeouts, un cuelgue de red bloquea el worker para siempre.
                connect_timeout=30,
                read_timeout=300,
            ),
        )
    return _client


def download_file(
    key: str,
    local_path: Path,
    bucket: str | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> Path:
    """
    Descarga un archivo desde R2 a local_path.
    on_progress(bytes_downloaded, total_bytes) se llama durante la descarga.
    """
    client = _get_client()
    bucket = bucket or settings.r2_bucket
    local_path.parent.mkdir(parents=True, exist_ok=True)

    head = client.head_object(Bucket=bucket, Key=key)
    total_bytes = head["ContentLength"]
    downloaded = 0

    def _callback(chunk: int) -> None:
        nonlocal downloaded
        downloaded += chunk
        if on_progress:
            on_progress(downloaded, total_bytes)

    client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=str(local_path),
        Callback=_callback,
    )
    return local_path


def upload_file(
    local_path: Path,
    key: str,
    bucket: str | None = None,
    content_type: str | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> str:
    """
    Sube un archivo local a R2. Retorna la key del objeto subido.
    on_progress(bytes_uploaded, total_bytes) se llama durante la subida.
    """
    client = _get_client()
    bucket = bucket or settings.r2_bucket
    total_bytes = local_path.stat().st_size
    uploaded = 0

    def _callback(chunk: int) -> None:
        nonlocal uploaded
        uploaded += chunk
        if on_progress:
            on_progress(uploaded, total_bytes)

    extra_args: dict = {}
    if content_type:
        extra_args["ContentType"] = content_type

    client.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args or None,
        Callback=_callback,
    )
    return key


def delete_file(key: str, bucket: str | None = None) -> None:
    client = _get_client()
    bucket = bucket or settings.r2_bucket
    client.delete_object(Bucket=bucket, Key=key)


def file_exists(key: str, bucket: str | None = None) -> bool:
    client = _get_client()
    bucket = bucket or settings.r2_bucket
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def get_file_size(key: str, bucket: str | None = None) -> int:
    client = _get_client()
    bucket = bucket or settings.r2_bucket
    head = client.head_object(Bucket=bucket, Key=key)
    return head["ContentLength"]
