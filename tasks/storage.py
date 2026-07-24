"""Acesso ao MinIO (object store) e hashing de conteúdo."""
import io
import hashlib
from minio import Minio

from tasks.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY


def cliente() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def baixar_bytes(bucket: str, key: str) -> bytes:
    resp = cliente().get_object(bucket, key)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def enviar_bytes(bucket: str, key: str, data: bytes, content_type: str = "text/csv") -> None:
    cliente().put_object(
        bucket, key, io.BytesIO(data), length=len(data), content_type=content_type
    )


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
