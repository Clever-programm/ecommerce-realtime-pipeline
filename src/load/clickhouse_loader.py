"""Загрузка потоковых витрин в аналитическую СУБД (ClickHouse).

Применяется для витрин, читаемых дашбордами с частотой обновления порядка
секунд: колоночная организация хранения даёт выигрыш на агрегирующих
запросах по широким временным интервалам. Таблицы семейства
ReplacingMergeTree обеспечивают идемпотентность повторной записи окна.
"""

from __future__ import annotations

import time

import clickhouse_connect
import pandas as pd

from src.common.config import get_env
from src.common.logging_setup import get_logger

logger = get_logger("load.clickhouse")


class ClickHouseLoader:
    """Клиент аналитической СУБД с повторными попытками подключения."""

    def __init__(self) -> None:
        self.client = None
        self.database = get_env("CLICKHOUSE_DB", "analytics")
        self._connect()

    def _connect(self, attempts: int = 30) -> None:
        for attempt in range(1, attempts + 1):
            try:
                self.client = clickhouse_connect.get_client(
                    host=get_env("CLICKHOUSE_HOST", required=True),
                    port=int(get_env("CLICKHOUSE_PORT", "8123")),
                    username=get_env("CLICKHOUSE_USER", required=True),
                    password=get_env("CLICKHOUSE_PASSWORD", required=True),
                    database=self.database,
                )
                self.client.ping()
                return
            except Exception as exc:
                logger.warning("Попытка %d: ClickHouse недоступен: %s",
                               attempt, exc)
                time.sleep(2)
        raise RuntimeError("Не удалось подключиться к ClickHouse")

    def insert_df(self, table: str, df: pd.DataFrame) -> int:
        """Вставляет кадр данных в таблицу; возвращает число строк."""
        if df.empty:
            return 0
        self.client.insert_df(table, df)
        return len(df)

    def query(self, sql: str) -> list[tuple]:
        """Выполняет произвольный запрос (служебное использование)."""
        return self.client.query(sql).result_rows
