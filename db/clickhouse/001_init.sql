-- Инициализация аналитической СУБД (ClickHouse).
-- Потоковые витрины, обслуживающие дашборды с обновлением порядка секунд.

CREATE DATABASE IF NOT EXISTS analytics;

-- Конверсия воронки на неперекрывающихся 5-минутных окнах.
CREATE TABLE IF NOT EXISTS analytics.funnel_rt (
    window_start   DateTime,
    window_end     DateTime,
    sessions_view  UInt64,
    sessions_cart  UInt64,
    sessions_order UInt64,
    cr_view_cart   Float64,
    cr_cart_order  Float64,
    formed_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(formed_at)
ORDER BY (window_start);

-- Рейтинг популярных товаров на скользящем окне 15 минут.
CREATE TABLE IF NOT EXISTS analytics.product_rating_rt (
    window_start DateTime,
    window_end   DateTime,
    product_id   UInt64,
    views        UInt64,
    carts        UInt64,
    orders       UInt64,
    score        UInt64,
    formed_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(formed_at)
ORDER BY (window_start, product_id);

-- Брошенные корзины на окнах по 30 минут.
CREATE TABLE IF NOT EXISTS analytics.abandoned_carts_rt (
    window_start    DateTime,
    window_end      DateTime,
    sessions_cart   UInt64,
    abandoned_count UInt64,
    abandoned_share Float64,
    formed_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(formed_at)
ORDER BY (window_start);

-- Статистика трафика веб-сервера на 5-минутных окнах.
CREATE TABLE IF NOT EXISTS analytics.traffic_stats_rt (
    window_start  DateTime,
    window_end    DateTime,
    requests      UInt64,
    errors_4xx    UInt64,
    errors_5xx    UInt64,
    error_rate    Float64,
    avg_rtime     Float64,
    p95_rtime     Float64,
    formed_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(formed_at)
ORDER BY (window_start);

-- Аномалии трафика (z-оценка текущего окна против базовой линии).
CREATE TABLE IF NOT EXISTS analytics.traffic_anomalies_rt (
    window_start   DateTime,
    window_end     DateTime,
    metric         String,
    actual         Float64,
    baseline_mean  Float64,
    baseline_std   Float64,
    z_score        Float64,
    detected_at    DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (window_start, metric);

-- Очищенные события (потоковая копия staging-слоя для аналитики).
CREATE TABLE IF NOT EXISTS analytics.events_staging (
    event_id    String,
    event_type  LowCardinality(String),
    event_ts    DateTime64(3),
    session_id  String,
    user_key    String,
    product_id  UInt64,
    price       Float64,
    device      LowCardinality(String),
    event_date  Date
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_ts, event_id);
