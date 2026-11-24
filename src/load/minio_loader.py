"""Загрузка объектов в озеро данных (MinIO, S3-совместимый интерфейс).

Сырые события сохраняются в неизменном виде с партиционированием по дате;
очищенные данные образуют слой Staging в формате Parquet.
"""

from __future__ import annotations

import io
import time

from minio import Minio
from minio.error import S3Error

from src.common.config import get_env
from src.common.logging_setup import get_logger

logger = get_logger("load.minio")


class MinioLoader:
    """Клиент озера данных с повторными попытками подключения."""

    def __init__(self) -> None:
        endpoint = get_env("MINIO_ENDPOINT", required=True)
        self.client = Minio(
            endpoint,
            access_key=get_env("MINIO_ROOT_USER", required=True),
            secret_key=get_env("MINIO_ROOT_PASSWORD", required=True),
            secure=False,
        )
        self.bucket_raw = get_env("MINIO_BUCKET_RAW", "raw")
        self.bucket_staging = get_env("MINIO_BUCKET_STAGING", "staging")
        self._wait_ready()
        self._ensure_buckets()

    def _ensure_buckets(self) -> None:
        """Создаёт бакеты слоёв хранения при первом обращении."""
        for bucket in (self.bucket_raw, self.bucket_staging):
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info("Создан бакет: %s", bucket)

    def _wait_ready(self, attempts: int = 30) -> None:
        for attempt in range(1, attempts + 1):
            try:
                self.client.list_buckets()
                return
            except Exception as exc:
                logger.warning("Попытка %d: MinIO недоступен: %s", attempt, exc)
                time.sleep(2)
        raise RuntimeError("Не удалось подключиться к MinIO")

    def put_bytes(self, key: str, payload: bytes, content_type: str,
                  bucket: str | None = None) -> None:
        """Сохраняет объект; повторная запись по тому же ключу
        перезаписывает объект (идемпотентность загрузки)."""
        self.client.put_object(
            bucket or self.bucket_raw,
            key,
            io.BytesIO(payload),
            length=len(payload),
            content_type=content_type,
        )

    def get_bytes(self, key: str, bucket: str | None = None) -> bytes:
        """Читает объект целиком."""
        response = self.client.get_object(bucket or self.bucket_raw, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        """Возвращает список ключей по префиксу."""
        objects = self.client.list_objects(
            bucket or self.bucket_raw, prefix=prefix, recursive=True
        )
        return [obj.object_name for obj in objects]

    def exists(self, key: str, bucket: str | None = None) -> bool:
        try:
            self.client.stat_object(bucket or self.bucket_raw, key)
            return True
        except S3Error:
            return False
