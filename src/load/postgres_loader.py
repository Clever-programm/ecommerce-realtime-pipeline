"""Загрузка витрин в локальную реляционную СУБД (PostgreSQL).

Основное хранилище готовых витрин и результатов пакетных расчётов.
Все записи выполняются по ключу с обновлением существующей записи
(INSERT ... ON CONFLICT DO UPDATE), что обеспечивает идемпотентность
повторных запусков (требование надёжности).
"""

from __future__ import annotations

import psycopg2
import psycopg2.extras

from src.common.config import get_env
from src.common.logging_setup import get_logger

logger = get_logger("load.postgres")


class PostgresLoader:
    """Клиент хранилища витрин с идемпотентными операциями записи."""

    def __init__(self) -> None:
        self.dsn = {
            "host": get_env("POSTGRES_DWH_HOST", required=True),
            "port": int(get_env("POSTGRES_DWH_PORT", "5432")),
            "dbname": get_env("POSTGRES_DWH_DB", required=True),
            "user": get_env("POSTGRES_DWH_USER", required=True),
            "password": get_env("POSTGRES_DWH_PASSWORD", required=True),
        }

    def execute(self, sql: str, params: tuple | None = None) -> None:
        with psycopg2.connect(**self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, params)

    def executemany(self, sql: str, rows: list[tuple]) -> int:
        if not rows:
            return 0
        with psycopg2.connect(**self.dsn) as conn, conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=1000)
        return len(rows)

    def fetchall(self, sql: str, params: tuple | None = None) -> list[tuple]:
        with psycopg2.connect(**self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def ensure_partition(self, day: str) -> None:
        """Создаёт партицию таблицы фактов на указанную дату."""
        name = f"fact_events_{day.replace('-', '')}"
        self.execute(
            f"CREATE TABLE IF NOT EXISTS {name} "
            f"PARTITION OF fact_events "
            f"FOR VALUES FROM (%(d)s) TO ((%(d)s)::date + 1)",
            {"d": day},
        )
