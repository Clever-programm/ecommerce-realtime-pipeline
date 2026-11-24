"""Извлечение журналов веб-сервера Nginx (модуль Extract).

Отслеживает файл журнала в формате combined log с добавленным временем
обработки запроса, разбирает каждую строку регулярным выражением и
публикует структурированные записи в топик брокера сообщений.

Строки, которые не удалось разобрать, не отбрасываются: они журналируются
с уровнем WARNING и учитываются в счётчике ошибок разбора.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer

from src.common.config import get_env, load_config
from src.common.logging_setup import get_logger

logger = get_logger("extract.log_shipper")

LOG_DIR = Path(os.environ.get("NGINX_LOG_DIR", "/var/log/nginx"))
LOG_FILE = LOG_DIR / "access.log"

# Формат: $remote_addr - - [$time_local] "$request" $status $body_bytes_sent
#         "$http_referer" "$http_user_agent" $request_time
LOG_PATTERN = re.compile(
    r'^(?P<ip>\S+) - - \[(?P<ts>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<uri>\S+) (?P<protocol>[^"]+)" '
    r'(?P<status>\d{3}) (?P<size>\d+) '
    r'"(?P<referer>[^"]*)" "(?P<agent>[^"]*)" (?P<rtime>[\d.]+)$'
)

BOT_MARKERS = ("bot", "crawler", "spider", "HealthCheck")


def parse_line(line: str) -> dict | None:
    """Разбирает строку журнала; возвращает None при несоответствии формату."""
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None
    fields = match.groupdict()
    agent = fields["agent"]
    return {
        "ip": fields["ip"],
        "ts": datetime.strptime(fields["ts"], "%d/%b/%Y:%H:%M:%S %z")
                      .isoformat(timespec="milliseconds"),
        "method": fields["method"],
        "uri": fields["uri"],
        "status": int(fields["status"]),
        "size": int(fields["size"]),
        "referer": fields["referer"],
        "agent": agent,
        "request_time": float(fields["rtime"]),
        # Обращения роботов и служебных проверок помечаются на этапе
        # извлечения и отсеиваются при расчёте показателей трафика.
        "is_bot": any(marker.lower() in agent.lower() for marker in BOT_MARKERS),
    }


def follow(path: Path):
    """Читает файл по мере появления строк (аналог tail -F)."""
    while not path.exists():
        logger.info("Ожидание появления файла журнала %s", path)
        time.sleep(2)
    with open(path, encoding="utf-8") as fh:
        while True:
            line = fh.readline()
            if not line:
                time.sleep(0.2)
                continue
            yield line


def make_producer() -> KafkaProducer:
    servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except Exception as exc:
            logger.warning("Попытка %d: брокер недоступен: %s", attempt, exc)
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к брокеру сообщений")


def main() -> None:
    cfg = load_config()
    topic = get_env("KAFKA_TOPIC_LOGS", cfg["streaming"]["kafka"]["topic_logs"])
    producer = make_producer()
    logger.info("Отправка записей журнала в топик %s", topic)

    parsed = skipped = 0
    for line in follow(LOG_FILE):
        record = parse_line(line)
        if record is None:
            skipped += 1
            logger.warning("Строка не разобрана: %s", line.strip()[:120])
            continue
        producer.send(topic, value=record)
        parsed += 1
        if parsed % 1000 == 0:
            producer.flush()
            logger.info("Разобрано записей: %d, пропущено: %d", parsed, skipped)


if __name__ == "__main__":
    main()
