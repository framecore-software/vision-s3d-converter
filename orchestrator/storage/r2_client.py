from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from orchestrator.config import settings


def _make_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key,
        aws_secret_access_key=settings.r2_secret_key,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


# Cliente compartido (thread-safe para operaciones de lectura)
_client = _make_client()


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
    bucket = bucket or settings.r2_bucket
    local_path.parent.mkdir(parents=True, exist_ok=True)

    head = _client.head_object(Bucket=bucket, Key=key)
    total_bytes = head["ContentLength"]
    downloaded = 0

    def _callback(chunk: int) -> None:
        nonlocal downloaded
        downloaded += chunk
        if on_progress:
            on_progress(downloaded, total_bytes)

    _client.download_file(
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

    _client.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args or None,
        Callback=_callback,
    )
    return key


def delete_file(key: str, bucket: str | None = None) -> None:
    bucket = bucket or settings.r2_bucket
    _client.delete_object(Bucket=bucket, Key=key)


def file_exists(key: str, bucket: str | None = None) -> bool:
    bucket = bucket or settings.r2_bucket
    try:
        _client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def get_file_size(key: str, bucket: str | None = None) -> int:
    bucket = bucket or settings.r2_bucket
    head = _client.head_object(Bucket=bucket, Key=key)
    return head["ContentLength"]
