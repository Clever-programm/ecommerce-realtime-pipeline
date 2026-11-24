"""Генератор событий фронтенда и журналов веб-сервера (эмуляция источников).

Эмулирует поведение посетителей интернет-магазина: формирует сессии,
внутри которых возможны события трёх типов — просмотр карточки товара,
добавление в корзину и оформление заказа. События публикуются в топик
брокера сообщений; параллельно в разделяемый том пишутся строки журнала
веб-сервера в формате combined log с добавленным временем обработки.

Для проверки устойчивости конвейера генератор намеренно воспроизводит
дефекты источников, выявленные при анализе (подраздел 1.3 ПЗ):
  * повторная публикация части событий (доставка «не менее одного раза»);
  * опоздание части событий относительно времени их наступления;
  * обращения поисковых роботов и служебные проверки доступности.
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

from src.common.config import get_env, load_config
from src.common.logging_setup import get_logger

logger = get_logger("extract.events_generator")

LOG_DIR = Path(os.environ.get("NGINX_LOG_DIR", "/var/log/nginx"))
LOG_FILE = LOG_DIR / "access.log"

DEVICES = ["desktop", "mobile", "tablet"]
PRODUCT_RANGE = (10000, 10500)
BOT_AGENTS = ["Googlebot/2.1", "YandexBot/3.0"]
BROWSERS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Linux; Android 14)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)",
]

EVENT_TYPE_BY_URI = {
    "product_view": ("/product/{pid}", "GET"),
    "add_to_cart": ("/api/cart/add", "POST"),
    "order_created": ("/api/order/create", "POST"),
}


def make_producer() -> KafkaProducer:
    """Создаёт продюсера Kafka с повторными попытками подключения."""
    servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
            )
        except Exception as exc:  # брокер ещё не готов
            logger.warning("Попытка %d: брокер недоступен: %s", attempt, exc)
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к брокеру сообщений")


def build_event(event_type: str, session: dict, product_id: int | None) -> dict:
    """Формирует событие пользовательской активности."""
    ts = datetime.now(timezone.utc)
    # Часть событий «опаздывает»: метка времени сдвинута в прошлое
    if random.random() < 0.05:
        ts = ts.replace(second=max(0, ts.second - random.randint(30, 300)))
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "session_id": session["session_id"],
        "user_key": session["user_key"],
        "product_id": product_id,
        "price": round(random.uniform(500, 9500), 2) if product_id else None,
        "device": session["device"],
    }


def nginx_line(event: dict, status: int = 200, rtime: float | None = None,
               agent: str | None = None, bot: bool = False) -> str:
    """Формирует строку журнала веб-сервера (combined log + request time)."""
    pid = event.get("product_id") or random.randint(*PRODUCT_RANGE)
    uri, method = EVENT_TYPE_BY_URI.get(
        event.get("event_type"), ("/", "GET")
    )
    uri = uri.format(pid=pid)
    ts = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S +0000")
    rtime = rtime if rtime is not None else round(random.lognormvariate(-2.5, 0.7), 3)
    agent = agent or (random.choice(BOT_AGENTS) if bot else random.choice(BROWSERS))
    ip = f"192.0.2.{random.randint(1, 254)}"
    size = random.randint(200, 5000)
    referer = f"https://shop.example/product/{pid}" if method == "POST" else "-"
    return (f'{ip} - - [{ts}] "{method} {uri} HTTP/1.1" '
            f'{status} {size} "{referer}" "{agent}" {rtime:.3f}')


def append_log(line: str) -> None:
    """Дописывает строку в файл журнала веб-сервера."""
    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def generate_session_events() -> tuple[list[dict], list[str]]:
    """Формирует события одной сессии и соответствующие строки журнала."""
    session = {
        "session_id": f"s-{uuid.uuid4().hex[:8]}",
        "user_key": f"u-{random.randint(1, 4000)}",
        "device": random.choice(DEVICES),
    }
    product = random.randint(*PRODUCT_RANGE)

    events, lines = [], []
    view = build_event("product_view", session, product)
    events.append(view)
    lines.append(nginx_line(view))

    r_cart, r_order = random.random(), random.random()
    if r_cart < 0.35:
        cart = build_event("add_to_cart", session, product)
        events.append(cart)
        lines.append(nginx_line(cart))
        if r_order < 0.55:
            order = build_event("order_created", session, product)
            events.append(order)
            lines.append(nginx_line(order))
    return events, lines


def main() -> None:
    cfg = load_config()
    topic = get_env("KAFKA_TOPIC_EVENTS", cfg["streaming"]["kafka"]["topic_events"])
    rate = float(get_env("GENERATOR_RATE_PER_SEC", "20"))
    interval = 1.0 / max(rate, 1)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.touch(exist_ok=True)

    producer = make_producer()
    logger.info("Генератор запущен: %s событий/с, топик %s", rate, topic)

    sent = 0
    while True:
        events, lines = generate_session_events()
        for event in events:
            producer.send(topic, key=event["session_id"], value=event)
            # Повторная публикация ~2 % событий — эмуляция доставки
            # «не менее одного раза», дедупликация выполняется на этапе
            # трансформации.
            if random.random() < 0.02:
                producer.send(topic, key=event["session_id"], value=event)
            sent += 1
        for line in lines:
            append_log(line)
        # Служебные и роботные обращения в журнале
        if random.random() < 0.10:
            append_log(nginx_line({}, agent="HealthCheck/1.0"))
        if random.random() < 0.05:
            append_log(nginx_line({}, bot=True))
        # Эпизодический всплеск ошибок 5xx
        if random.random() < 0.01:
            for _ in range(random.randint(5, 15)):
                append_log(nginx_line(events[0], status=500,
                                      rtime=round(random.uniform(1.0, 5.0), 3)))
            logger.warning("Эмулирован всплеск ошибок 5xx")

        if sent % 1000 < len(events):
            logger.info("Опубликовано событий: %d", sent)
        producer.flush()
        time.sleep(interval)


if __name__ == "__main__":
    main()
