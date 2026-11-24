"""Контроль качества данных (Great Expectations).

Проверки описаны декларативно и хранятся в репозитории вместе с кодом.
Валидации выполняются над слоем Staging перед публикацией витрин;
результат каждой проверки фиксируется в таблице dq_results, а провал
критических проверок останавливает пакетный конвейер.
"""

from __future__ import annotations

import io
import json
from datetime import date

import great_expectations as gx
import pandas as pd

from src.common.config import load_config
from src.common.logging_setup import get_logger
from src.load.minio_loader import MinioLoader
from src.load.postgres_loader import PostgresLoader

logger = get_logger("quality.gx")

SUITE_NAME = "staging_events_suite"


def build_suite(context) -> gx.ExpectationSuite:
    """Декларативное описание проверок качества событий."""
    return context.suites.add(gx.ExpectationSuite(
        name=SUITE_NAME,
        expectations=[
            # Полнота: обязательные поля не содержат пропусков
            gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="event_ts"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="session_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type"),
            # Уникальность идентификатора события
            gx.expectations.ExpectColumnValuesToBeUnique(column="event_id"),
            # Соответствие бизнес-правилам
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="event_type",
                value_set=["product_view", "add_to_cart", "order_created"]),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="price", min_value=0, max_value=1_000_000,
                mostly=0.99),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="device",
                value_set=["desktop", "mobile", "tablet"],
                mostly=0.99),
        ],
    ))


def load_staging(dt: str) -> pd.DataFrame:
    """Читает очищенные события слоя Staging за указанную дату."""
    loader = MinioLoader()
    frames = []
    for key in loader.list_keys(f"external_events/dt={dt}/",
                                bucket=loader.bucket_staging):
        frames.append(pd.read_parquet(io.BytesIO(loader.get_bytes(
            key, bucket=loader.bucket_staging))))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run(run_id: str, dt: str | None = None) -> bool:
    """Выполняет проверки; возвращает итоговый признак успеха."""
    dt = dt or date.today().isoformat()
    df = load_staging(dt)
    if df.empty:
        logger.warning("Нет данных Staging за %s — проверки пропущены", dt)
        return True

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("pandas")
    asset = datasource.add_dataframe_asset("events")
    batch_def = asset.add_batch_definition_whole_dataframe("bd")
    batch = batch_def.get_batch({"dataframe": df})
    result = batch.validate(build_suite(context))

    stats = result.statistics
    failed = stats["evaluated_expectations"] - stats["successful_expectations"]
    logger.info("Проверок: %d, успешно: %d, провалено: %d",
                stats["evaluated_expectations"],
                stats["successful_expectations"], failed)

    details = {"expectations": [
        {"type": r.expectation_config.type, "success": r.success}
        for r in result.results
    ]}

    PostgresLoader().execute(
        """INSERT INTO dq_results
               (run_id, suite_name, success, checks_total, checks_failed, details)
           VALUES (%s, %s, %s, %s, %s, %s::jsonb)""",
        (run_id, SUITE_NAME, bool(result.success),
         stats["evaluated_expectations"], failed, json.dumps(details)),
    )

    if not result.success:
        raise RuntimeError(
            f"Проверки качества данных не пройдены: {failed} из "
            f"{stats['evaluated_expectations']}")
    return True
