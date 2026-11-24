"""Пакетная трансформация событий внешнего источника (Transform).

Читает извлечённые страницы из слоя Raw озера данных, выполняет очистку,
нормализацию и дедупликацию, после чего сохраняет результат в слой
Staging (Parquet) и загружает таблицу фактов хранилища витрин.

Проверки качества данных:
  * обязательные поля не пусты (полнота);
  * тип события входит в допустимый перечень (соответствие бизнес-правилам);
  * дедупликация по идентификатору события (уникальность);
  * метки времени приводятся к единому часовому поясу UTC.
"""

from __future__ import annotations

import io
import json
from datetime import date

import pandas as pd

from src.common.config import load_config
from src.common.logging_setup import get_logger
from src.load.minio_loader import MinioLoader
from src.load.postgres_loader import PostgresLoader

logger = get_logger("transform.batch")

VALID_EVENT_TYPES = {"product_view", "add_to_cart", "order_created"}


def read_raw_events(loader: MinioLoader, dt: str) -> pd.DataFrame:
    """Читает все страницы извлечённых событий за указанную дату."""
    cfg = load_config()
    prefix = f"{cfg['storage']['raw_prefix_api']}/dt={dt}/"
    frames = []
    for key in loader.list_keys(prefix):
        items = json.loads(loader.get_bytes(key))
        if items:
            frames.append(pd.DataFrame(items))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает и нормализует события; возвращает пригодный к загрузке набор."""
    if df.empty:
        return df
    df = df.copy()
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    df["event_type"] = df["event_type"].str.strip().str.lower()
    df = df[
        df["event_id"].notna()
        & df["event_ts"].notna()
        & df["session_id"].notna()
        & df["event_type"].isin(VALID_EVENT_TYPES)
    ]
    df = df.drop_duplicates(subset=["event_id"], keep="first")
    df["event_date"] = df["event_ts"].dt.date
    return df.reset_index(drop=True)


def load_facts(pdf: pd.DataFrame, run_id: str) -> None:
    """Загружает таблицу фактов и измерения; операции идемпотентны."""
    pg = PostgresLoader()
    for day in pdf["event_date"].unique():
        pg.ensure_partition(str(day))

    dim_sessions = (pdf.groupby("session_id")
                    .agg(user_key=("user_key", "first"),
                         device=("device", "first"),
                         first_seen_ts=("event_ts", "min"),
                         last_seen_ts=("event_ts", "max"))
                    .reset_index())
    pg.executemany(
        """INSERT INTO dim_sessions
               (session_key, user_key, device, first_seen_ts, last_seen_ts)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (session_key) DO UPDATE SET
               last_seen_ts = GREATEST(dim_sessions.last_seen_ts,
                                       EXCLUDED.last_seen_ts)""",
        list(dim_sessions.itertuples(index=False, name=None)),
    )

    products = pdf[pdf["product_id"].notna()]["product_id"].unique()
    pg.executemany(
        """INSERT INTO dim_products (product_id) VALUES (%s)
           ON CONFLICT (product_id) DO NOTHING""",
        [(int(p),) for p in products],
    )

    rows = [(*r, run_id) for r in pdf[
        ["event_id", "event_type", "event_ts", "session_id",
         "user_key", "product_id", "price", "event_date"]
    ].itertuples(index=False, name=None)]
    pg.executemany(
        """INSERT INTO fact_events
               (event_id, event_type, event_ts, session_key,
                user_key, product_id, price, event_date, run_id)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (event_id, event_date) DO NOTHING""",
        rows,
    )
    logger.info("Загружено фактов: %d", len(rows))


def run(run_id: str, dt: str | None = None) -> int:
    """Выполняет пакетную трансформацию за дату; возвращает число записей."""
    dt = dt or date.today().isoformat()
    loader = MinioLoader()
    raw = read_raw_events(loader, dt)
    if raw.empty:
        logger.info("Сырых данных за %s нет", dt)
        return 0

    cleaned = clean_events(raw)
    logger.info("Очищено %d из %d записей", len(cleaned), len(raw))

    buf = io.BytesIO()
    cleaned.to_parquet(buf, engine="pyarrow", index=False)
    key = f"external_events/dt={dt}/{run_id}.parquet"
    loader.put_bytes(key, buf.getvalue(), "application/octet-stream",
                     bucket=loader.bucket_staging)

    load_facts(cleaned, run_id)
    return len(cleaned)
