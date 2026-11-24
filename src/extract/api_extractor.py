"""Извлечение подготовленных событий из внешней базы данных через API.

Инкрементальное чтение: нижняя граница метки времени хранится в состоянии
задачи и при каждом запуске сдвигается назад на величину допустимого
опоздания, чтобы не потерять события, поступившие в источник с задержкой.
Извлечённые страницы сохраняются в озеро данных в неизменном виде
(слой Raw); состояние задачи также хранится в озере.
"""

from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone

import requests

from src.common.config import get_env, load_config
from src.common.logging_setup import get_logger
from src.load.minio_loader import MinioLoader

logger = get_logger("extract.api_extractor")

STATE_OBJECT = "external_api/_state.json"


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ApiExtractor:
    """Инкрементальный модуль извлечения событий из внешнего API."""

    def __init__(self) -> None:
        cfg = load_config()
        self.base_url = get_env("EXTERNAL_API_URL", required=True)
        self.api_key = get_env("EXTERNAL_API_KEY", required=True)
        self.page_size = cfg["batch"]["api_page_size"]
        self.lag = timedelta(minutes=cfg["batch"]["api_max_lag_minutes"])
        self.prefix = cfg["storage"]["raw_prefix_api"]
        self.loader = MinioLoader()

    def _load_state(self) -> datetime:
        """Читает метку времени последнего обработанного события."""
        try:
            raw = self.loader.get_bytes(STATE_OBJECT)
            return _parse_ts(json.loads(raw)["last_ts"])
        except Exception:
            logger.info("Состояние не найдено, выполняется полная загрузка")
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _save_state(self, ts: datetime) -> None:
        payload = json.dumps({"last_ts": ts.isoformat()}).encode("utf-8")
        self.loader.put_bytes(STATE_OBJECT, payload, "application/json")

    def fetch_since(self, since: datetime) -> list[dict]:
        """Читает все страницы событий, новее указанной метки времени."""
        items, offset = [], 0
        headers = {"X-API-Key": self.api_key}
        while True:
            resp = requests.get(
                f"{self.base_url}/events",
                params={"since": since.isoformat(),
                        "limit": self.page_size, "offset": offset},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            page = resp.json()
            items.extend(page["items"])
            offset += len(page["items"])
            if offset >= page["total"] or not page["items"]:
                break
        return items

    def run(self, run_id: str) -> int:
        """Выполняет один цикл извлечения; возвращает число записей."""
        since = self._load_state() - self.lag
        logger.info("Извлечение событий новее %s", since.isoformat())
        items = self.fetch_since(since)
        if not items:
            logger.info("Новых событий нет")
            return 0

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"{self.prefix}/dt={today}/{run_id}.json"
        body = json.dumps(items, ensure_ascii=False).encode("utf-8")
        self.loader.put_bytes(key, body, "application/json")

        last_ts = max(_parse_ts(item["event_ts"]) for item in items)
        self._save_state(last_ts)
        logger.info("Извлечено %d событий, сохранено в %s, граница %s",
                    len(items), key, last_ts.isoformat())
        return len(items)


def extract(run_id: str) -> int:
    """Точка входа для оркестратора."""
    return ApiExtractor().run(run_id)


if __name__ == "__main__":
    extract(run_id=datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%S"))
