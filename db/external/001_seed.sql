-- Внешний источник: база данных с подготовленными событиями.
-- Доступ к ней со стороны конвейера выполняется только через программный
-- интерфейс (сервис external-api); прямое подключение не используется.

BEGIN;

CREATE TABLE IF NOT EXISTS events (
    event_id   TEXT PRIMARY KEY,
    event_type TEXT        NOT NULL,
    event_ts   TIMESTAMPTZ NOT NULL,
    session_id TEXT        NOT NULL,
    user_key   TEXT,
    product_id BIGINT,
    price      NUMERIC(12, 2),
    device     TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON events (event_ts);

-- Наполнение: синтетические события за последние 7 суток.
-- Воронка: 100 % просмотры → ~35 % корзина → ~55 % от корзины заказ.
INSERT INTO events (event_id, event_type, event_ts, session_id, user_key, product_id, price, device)
SELECT
    'seed-' || g || '-' || s.step_no,
    s.event_type,
    ts,
    's-seed-' || g,
    'u-' || (g % 4000),
    10000 + (g % 500),
    round((random() * 9000 + 500)::numeric, 2),
    (ARRAY['desktop', 'mobile', 'tablet'])[1 + (g % 3)]
FROM generate_series(1, 20000) AS g
CROSS JOIN LATERAL (
    SELECT now() - (random() * interval '7 days') AS ts,
           random() AS r_cart,
           random() AS r_order
) t
CROSS JOIN (
    VALUES (1, 'product_view'), (2, 'add_to_cart'), (3, 'order_created')
) s(step_no, event_type)
WHERE s.step_no = 1
   OR (s.step_no = 2 AND t.r_cart < 0.35)
   OR (s.step_no = 3 AND t.r_cart < 0.35 AND t.r_order < 0.55)
ORDER BY ts
ON CONFLICT (event_id) DO NOTHING;

COMMIT;
