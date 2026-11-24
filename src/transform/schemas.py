"""Схемы сообщений потоковой обработки (модуль Transform).

Структура каждого типа события описана декларативно и единообразно
применяется во всех подсистемах (требование стандартизации).
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_ts", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("user_key", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("device", StringType(), True),
])

LOG_SCHEMA = StructType([
    StructField("ip", StringType(), True),
    StructField("ts", StringType(), False),
    StructField("method", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("status", LongType(), False),
    StructField("size", LongType(), True),
    StructField("referer", StringType(), True),
    StructField("agent", StringType(), True),
    StructField("request_time", DoubleType(), True),
    StructField("is_bot", BooleanType(), True),
])

VALID_EVENT_TYPES = ("product_view", "add_to_cart", "order_created")
