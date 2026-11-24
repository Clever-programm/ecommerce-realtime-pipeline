"""Резервное копирование хранилища витрин.

Ежесуточная копия базы данных витрин выгружается в озеро данных;
хранятся не менее 7 последних копий (требование к эксплуатации).
"""

from __future__ import annotations

import os
import subprocess

from src.common.config import get_env, load_config
from src.common.logging_setup import get_logger
from src.load.minio_loader import MinioLoader

logger = get_logger("load.backup")

BACKUP_PREFIX = "backups/postgres_dwh"


def run(dt: str) -> str:
    """Выполняет копию и удаляет устаревшие; возвращает ключ объекта."""
    cfg = load_config()
    keep = cfg["batch"]["backup_retention"]

    env = os.environ.copy()
    env["PGPASSWORD"] = get_env("POSTGRES_DWH_PASSWORD", required=True)
    proc = subprocess.run(
        ["pg_dump", "-Fc",
         "-h", get_env("POSTGRES_DWH_HOST", required=True),
         "-p", get_env("POSTGRES_DWH_PORT", "5432"),
         "-U", get_env("POSTGRES_DWH_USER", required=True),
         "-d", get_env("POSTGRES_DWH_DB", required=True)],
        capture_output=True, env=env, check=True,
    )

    key = f"{BACKUP_PREFIX}/marts-{dt}.dump"
    MinioLoader().put_bytes(key, proc.stdout, "application/octet-stream")
    logger.info("Резервная копия сохранена: %s (%d байт)", key, len(proc.stdout))

    loader = MinioLoader()
    keys = sorted(loader.list_keys(BACKUP_PREFIX + "/"), reverse=True)
    for old in keys[keep:]:
        loader.client.remove_object(loader.bucket_raw, old)
        logger.info("Удалена устаревшая копия: %s", old)
    return key
