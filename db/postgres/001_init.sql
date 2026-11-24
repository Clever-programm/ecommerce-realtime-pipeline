-- Инициализация хранилища витрин (локальная PostgreSQL).
-- Слой Marts: таблица фактов и аналитические витрины пакетного контура.
-- Схема «звезда»: факты событий + измерения сессий и товаров.

BEGIN;

-- ── Измерения ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_sessions (
    session_key   TEXT PRIMARY KEY,
    user_key      TEXT        NOT NULL,
    device        TEXT,
    first_seen_ts TIMESTAMPTZ NOT NULL,
    last_seen_ts  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id BIGINT PRIMARY KEY,
    category   TEXT,
    title      TEXT
);

-- ── Таблица фактов ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_events (
    event_id     TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    session_key  TEXT        NOT NULL,
    user_key     TEXT,
    product_id   BIGINT,
    price        NUMERIC(12, 2),
    event_date   DATE        NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id       TEXT        NOT NULL,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Партиции создаются пакетной задачей; партиция по умолчанию — страховка.
CREATE TABLE IF NOT EXISTS fact_events_default PARTITION OF fact_events DEFAULT;

CREATE INDEX IF NOT EXISTS idx_fact_events_ts      ON fact_events (event_ts);
CREATE INDEX IF NOT EXISTS idx_fact_events_session ON fact_events (session_key);
CREATE INDEX IF NOT EXISTS idx_fact_events_type    ON fact_events (event_type);

-- ── Витрины пакетного контура ────────────────────────────────
-- Суточная конверсия воронки.
CREATE TABLE IF NOT EXISTS mart_daily_funnel (
    report_date    DATE PRIMARY KEY,
    sessions_view  BIGINT NOT NULL,
    sessions_cart  BIGINT NOT NULL,
    sessions_order BIGINT NOT NULL,
    cr_view_cart   NUMERIC(6, 4),
    cr_cart_order  NUMERIC(6, 4),
    cr_full        NUMERIC(6, 4),
    formed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id         TEXT NOT NULL
);

-- Суточный рейтинг популярных товаров.
CREATE TABLE IF NOT EXISTS mart_daily_top_products (
    report_date DATE   NOT NULL,
    product_id  BIGINT NOT NULL,
    views       BIGINT NOT NULL DEFAULT 0,
    carts       BIGINT NOT NULL DEFAULT 0,
    orders      BIGINT NOT NULL DEFAULT 0,
    score       BIGINT NOT NULL DEFAULT 0,
    rank        INT    NOT NULL,
    formed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id      TEXT NOT NULL,
    PRIMARY KEY (report_date, product_id)
);

-- Брошенные корзины за сутки.
CREATE TABLE IF NOT EXISTS mart_daily_abandoned_carts (
    report_date      DATE PRIMARY KEY,
    sessions_cart    BIGINT NOT NULL,
    abandoned_count  BIGINT NOT NULL,
    abandoned_share  NUMERIC(6, 4),
    formed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id           TEXT NOT NULL
);

-- Результаты проверок качества данных.
CREATE TABLE IF NOT EXISTS dq_results (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT        NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    suite_name    TEXT        NOT NULL,
    success       BOOLEAN     NOT NULL,
    checks_total  INT         NOT NULL,
    checks_failed INT         NOT NULL,
    details       JSONB
);

COMMIT;
