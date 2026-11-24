"""Потоковая обработка событий (PySpark Structured Streaming).

Единый потоковый контур (модель Kappa) рассчитывает все четыре прикладные
метрики конвейера:

  1. конверсия воронки «просмотр — корзина — заказ» на неперекрывающихся
     5-минутных окнах по времени наступления события;
  2. брошенные корзины на 30-минутных окнах — сессии с добавлением товара,
     за которым не последовало оформление заказа;
  3. рейтинг популярных товаров на скользящем окне 15 минут с шагом
     5 минут (взвешенная сумма: просмотр — 1, корзина — 3, заказ — 5);
  4. аномалии трафика веб-сервера: z-оценка числа запросов и доли ошибок
     текущего окна относительно базовой линии предшествующего часа.

Очищенные события параллельно сохраняются в слой Staging озера данных
(Parquet, партиции по дате) и в потоковые витрины аналитической СУБД.

Отказоустойчивость обеспечивается контрольными точками: после сбоя
обработка возобновляется с последнего зафиксированного смещения.
"""

from __future__ import annotations

import io
from collections import deque
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import get_env, load_config
from src.common.logging_setup import get_logger
from src.load.clickhouse_loader import ClickHouseLoader
from src.load.minio_loader import MinioLoader
from src.transform.schemas import EVENT_SCHEMA, LOG_SCHEMA, VALID_EVENT_TYPES

logger = get_logger("transform.streaming")

cfg = load_config()
WM = cfg["streaming"]["watermark_delay"]


# ── Приёмники микропакетов ──────────────────────────────────────

class StagingSink:
    """Сохраняет очищенные события: Parquet в озеро + копия в ClickHouse."""

    def __init__(self) -> None:
        self.minio = MinioLoader()
        self.ch = ClickHouseLoader()
        self.prefix = cfg["storage"]["staging_prefix"]

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        if pdf.empty:
            return
        pdf["event_date"] = pd.to_datetime(pdf["event_ts"]).dt.date
        buf = io.BytesIO()
        pdf.to_parquet(buf, engine="pyarrow", index=False)
        day = pdf["event_date"].max().isoformat()
        key = f"{self.prefix}/dt={day}/batch-{batch_id}.parquet"
        self.minio.put_bytes(key, buf.getvalue(), "application/octet-stream",
                             bucket=self.minio.bucket_staging)
        self.ch.insert_df("events_staging", pdf)
        logger.info("Микропакет %d: %d событий -> %s", batch_id, len(pdf), key)


class TableSink:
    """Записывает агрегаты окна в таблицу аналитической СУБД."""

    def __init__(self, table: str) -> None:
        self.ch = ClickHouseLoader()
        self.table = table

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        if pdf.empty:
            return
        for col in ("window_start", "window_end"):
            if col in pdf.columns:
                pdf[col] = pd.to_datetime(pdf[col]).dt.tz_localize(None)
        rows = self.ch.insert_df(self.table, pdf)
        logger.info("Микропакет %d: %d строк -> %s", batch_id, rows, self.table)


class TrafficAnomalyDetector:
    """Выявляет аномалии трафика z-оценкой текущего окна.

    Базовая линия — скользящее среднее и стандартное отклонение метрик
    за предшествующие N окон (по умолчанию час); само текущее окно
    в базовую линию не входит.
    """

    def __init__(self) -> None:
        self.ch = ClickHouseLoader()
        self.baseline: deque[dict] = deque(
            maxlen=cfg["windows"]["traffic"]["baseline_windows"])
        self.threshold = cfg["windows"]["traffic"]["z_threshold"]
        self.stats_table = "traffic_stats_rt"
        self.anomalies_table = "traffic_anomalies_rt"

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        if pdf.empty:
            return
        pdf["window_start"] = pd.to_datetime(pdf["window_start"]).dt.tz_localize(None)
        pdf["window_end"] = pd.to_datetime(pdf["window_end"]).dt.tz_localize(None)
        self.ch.insert_df(self.stats_table, pdf)

        anomalies = []
        for row in pdf.itertuples():
            if len(self.baseline) >= 3:
                for metric in ("requests", "error_rate"):
                    values = [b[metric] for b in self.baseline]
                    mean = sum(values) / len(values)
                    std = pd.Series(values).std(ddof=0)
                    actual = getattr(row, metric)
                    if std > 0:
                        z = (actual - mean) / std
                        if abs(z) >= self.threshold:
                            anomalies.append({
                                "window_start": row.window_start,
                                "window_end": row.window_end,
                                "metric": metric,
                                "actual": float(actual),
                                "baseline_mean": float(mean),
                                "baseline_std": float(std),
                                "z_score": float(z),
                            })
            self.baseline.append({
                "requests": row.requests, "error_rate": row.error_rate})

        if anomalies:
            self.ch.insert_df(self.anomalies_table, pd.DataFrame(anomalies))
            logger.warning("Микропакет %d: выявлено аномалий: %d",
                           batch_id, len(anomalies))


# ── Потоки событий фронтенда ────────────────────────────────────

def read_events(spark: SparkSession) -> DataFrame:
    """Читает, валидирует и дедуплицирует поток событий фронтенда."""
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers",
                   get_env("KAFKA_BOOTSTRAP_SERVERS", required=True))
           .option("subscribe", cfg["streaming"]["kafka"]["topic_events"])
           .option("startingOffsets", cfg["streaming"]["kafka"]["starting_offsets"])
           .option("failOnDataLoss", "false")
           .load())
    parsed = (raw
              .select(F.from_json(F.col("value").cast("string"),
                                  EVENT_SCHEMA).alias("e"))
              .select("e.*")
              .withColumn("event_ts", F.to_timestamp("event_ts"))
              # Валидация: обязательные поля и допустимые типы событий
              .filter(F.col("event_id").isNotNull()
                      & F.col("event_ts").isNotNull()
                      & F.col("session_id").isNotNull()
                      & F.col("event_type").isin(*VALID_EVENT_TYPES))
              .withWatermark("event_ts", WM)
              # Дедупликация по идентификатору события в пределах watermark
              .dropDuplicates(["event_id"]))
    return parsed


def funnel_stream(events: DataFrame) -> DataFrame:
    """Конверсия воронки на неперекрывающихся окнах.

    Число уникальных сессий на каждом шаге вычисляется двухуровневой
    агрегацией: сначала признаки шагов по паре «окно — сессия», затем
    их сумма по окну (потоковый движок не поддерживает countDistinct).
    """
    duration = cfg["windows"]["funnel_duration"]
    sessions = (events
                .groupBy(F.window("event_ts", duration), F.col("session_id"))
                .agg(
                    F.max(F.when(F.col("event_type") == "product_view", 1)
                           .otherwise(0)).alias("has_view"),
                    F.max(F.when(F.col("event_type") == "add_to_cart", 1)
                           .otherwise(0)).alias("has_cart"),
                    F.max(F.when(F.col("event_type") == "order_created", 1)
                           .otherwise(0)).alias("has_order"),
                ))
    return (sessions
            .groupBy("window")
            .agg(
                F.sum("has_view").alias("sessions_view"),
                F.sum("has_cart").alias("sessions_cart"),
                F.sum("has_order").alias("sessions_order"),
            )
            .withColumn("cr_view_cart",
                        F.when(F.col("sessions_view") > 0,
                               F.col("sessions_cart") / F.col("sessions_view"))
                         .otherwise(F.lit(0.0)))
            .withColumn("cr_cart_order",
                        F.when(F.col("sessions_cart") > 0,
                               F.col("sessions_order") / F.col("sessions_cart"))
                         .otherwise(F.lit(0.0)))
            .select(F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "sessions_view", "sessions_cart", "sessions_order",
                    "cr_view_cart", "cr_cart_order"))


def product_rating_stream(events: DataFrame) -> DataFrame:
    """Рейтинг популярных товаров на скользящем окне."""
    win = cfg["windows"]["product_rating"]
    weights = cfg["scoring"]
    score_expr = (
        F.when(F.col("event_type") == "product_view", weights["product_view"])
         .when(F.col("event_type") == "add_to_cart", weights["add_to_cart"])
         .when(F.col("event_type") == "order_created", weights["order_created"])
         .otherwise(0)
    )
    return (events
            .filter(F.col("product_id").isNotNull())
            .withColumn("score", score_expr)
            .groupBy(F.window("event_ts", win["window_duration"],
                              win["slide_duration"]), F.col("product_id"))
            .agg(
                F.sum(F.when(F.col("event_type") == "product_view", 1)
                       .otherwise(0)).alias("views"),
                F.sum(F.when(F.col("event_type") == "add_to_cart", 1)
                       .otherwise(0)).alias("carts"),
                F.sum(F.when(F.col("event_type") == "order_created", 1)
                       .otherwise(0)).alias("orders"),
                F.sum("score").alias("score"),
            )
            .select(F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "product_id", "views", "carts", "orders", "score"))


def abandoned_carts_stream(events: DataFrame) -> DataFrame:
    """Брошенные корзины: корзина без заказа в пределах окна."""
    duration = cfg["windows"]["abandoned_cart_interval"]
    sessions = (events
                .groupBy(F.window("event_ts", duration), F.col("session_id"))
                .agg(
                    F.max(F.when(F.col("event_type") == "add_to_cart", 1)
                           .otherwise(0)).alias("has_cart"),
                    F.max(F.when(F.col("event_type") == "order_created", 1)
                           .otherwise(0)).alias("has_order"),
                ))
    return (sessions
            .groupBy("window")
            .agg(
                F.sum("has_cart").alias("sessions_cart"),
                F.sum(F.when((F.col("has_cart") == 1) & (F.col("has_order") == 0), 1)
                       .otherwise(0)).alias("abandoned_count"),
            )
            .withColumn("abandoned_share",
                        F.when(F.col("sessions_cart") > 0,
                               F.col("abandoned_count") / F.col("sessions_cart"))
                         .otherwise(F.lit(0.0)))
            .select(F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "sessions_cart", "abandoned_count", "abandoned_share"))


# ── Поток журналов веб-сервера ──────────────────────────────────

def read_logs(spark: SparkSession) -> DataFrame:
    """Читает поток разобранных записей журнала веб-сервера."""
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers",
                   get_env("KAFKA_BOOTSTRAP_SERVERS", required=True))
           .option("subscribe", cfg["streaming"]["kafka"]["topic_logs"])
           .option("startingOffsets", cfg["streaming"]["kafka"]["starting_offsets"])
           .option("failOnDataLoss", "false")
           .load())
    return (raw
            .select(F.from_json(F.col("value").cast("string"),
                                LOG_SCHEMA).alias("l"))
            .select("l.*")
            .withColumn("ts", F.to_timestamp("ts"))
            .filter(F.col("ts").isNotNull())
            .withWatermark("ts", WM))


def traffic_stats_stream(logs: DataFrame) -> DataFrame:
    """Статистика трафика на окнах; обращения роботов отсеиваются."""
    duration = cfg["windows"]["traffic"]["window_duration"]
    return (logs
            .filter(F.col("is_bot") == F.lit(False))
            .groupBy(F.window("ts", duration))
            .agg(
                F.count("*").alias("requests"),
                F.sum(F.when(F.col("status").between(400, 499), 1)
                       .otherwise(0)).alias("errors_4xx"),
                F.sum(F.when(F.col("status").between(500, 599), 1)
                       .otherwise(0)).alias("errors_5xx"),
                F.avg("request_time").alias("avg_rtime"),
                F.expr("percentile_approx(request_time, 0.95)").alias("p95_rtime"),
            )
            .withColumn("error_rate",
                        F.when(F.col("requests") > 0,
                               (F.col("errors_4xx") + F.col("errors_5xx"))
                               / F.col("requests"))
                         .otherwise(F.lit(0.0)))
            .select(F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "requests", "errors_4xx", "errors_5xx", "error_rate",
                    "avg_rtime", "p95_rtime"))


# ── Запуск ──────────────────────────────────────────────────────

def main() -> None:
    spark = (SparkSession.builder
             .appName("rtp-streaming")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    checkpoint = cfg["streaming"]["checkpoint_root"]
    trigger = {"processingTime": cfg["streaming"]["trigger_interval"]}

    events = read_events(spark)
    logs = read_logs(spark)

    queries = [
        # Слой Staging: очищенные события
        (events.writeStream, StagingSink(), "staging"),
        # Четыре прикладные метрики
        (funnel_stream(events).writeStream, TableSink("funnel_rt"), "funnel"),
        (product_rating_stream(events).writeStream,
         TableSink("product_rating_rt"), "product_rating"),
        (abandoned_carts_stream(events).writeStream,
         TableSink("abandoned_carts_rt"), "abandoned_carts"),
        (traffic_stats_stream(logs).writeStream,
         TrafficAnomalyDetector(), "traffic"),
    ]

    started = []
    for writer, sink, name in queries:
        query = (writer
                 .foreachBatch(sink)
                 .trigger(**trigger)
                 .option("checkpointLocation", f"{checkpoint}/{name}")
                 .start())
        started.append(query)
        logger.info("Потоковый запрос запущен: %s", name)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
