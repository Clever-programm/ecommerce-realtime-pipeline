"""Формирование суточных аналитических витрин (пакетный контур).

Витрины рассчитываются из таблицы фактов оконными агрегациями и записаны
по ключу с обновлением существующей записи: повторный запуск за ту же
дату даёт тот же результат без дублирования (идемпотентность).
"""

from __future__ import annotations

from datetime import date

from src.common.logging_setup import get_logger
from src.load.postgres_loader import PostgresLoader

logger = get_logger("load.marts")

SQL_FUNNEL = """
    INSERT INTO mart_daily_funnel
        (report_date, sessions_view, sessions_cart, sessions_order,
         cr_view_cart, cr_cart_order, cr_full, run_id)
    SELECT
        %(d)s::date,
        count(DISTINCT session_key) FILTER (WHERE event_type = 'product_view'),
        count(DISTINCT session_key) FILTER (WHERE event_type = 'add_to_cart'),
        count(DISTINCT session_key) FILTER (WHERE event_type = 'order_created'),
        round(count(DISTINCT session_key) FILTER (WHERE event_type = 'add_to_cart')
              ::numeric / NULLIF(count(DISTINCT session_key)
                  FILTER (WHERE event_type = 'product_view'), 0), 4),
        round(count(DISTINCT session_key) FILTER (WHERE event_type = 'order_created')
              ::numeric / NULLIF(count(DISTINCT session_key)
                  FILTER (WHERE event_type = 'add_to_cart'), 0), 4),
        round(count(DISTINCT session_key) FILTER (WHERE event_type = 'order_created')
              ::numeric / NULLIF(count(DISTINCT session_key)
                  FILTER (WHERE event_type = 'product_view'), 0), 4),
        %(run)s
    FROM fact_events
    WHERE event_date = %(d)s::date
    ON CONFLICT (report_date) DO UPDATE SET
        sessions_view  = EXCLUDED.sessions_view,
        sessions_cart  = EXCLUDED.sessions_cart,
        sessions_order = EXCLUDED.sessions_order,
        cr_view_cart   = EXCLUDED.cr_view_cart,
        cr_cart_order  = EXCLUDED.cr_cart_order,
        cr_full        = EXCLUDED.cr_full,
        formed_at      = now(),
        run_id         = EXCLUDED.run_id
"""

SQL_TOP_PRODUCTS = """
    INSERT INTO mart_daily_top_products
        (report_date, product_id, views, carts, orders, score, rank, run_id)
    SELECT report_date, product_id, views, carts, orders, score,
           rank() OVER (ORDER BY score DESC) AS rank, %(run)s
    FROM (
        SELECT %(d)s::date AS report_date, product_id,
               count(*) FILTER (WHERE event_type = 'product_view') AS views,
               count(*) FILTER (WHERE event_type = 'add_to_cart')  AS carts,
               count(*) FILTER (WHERE event_type = 'order_created') AS orders,
               count(*) FILTER (WHERE event_type = 'product_view') * 1
             + count(*) FILTER (WHERE event_type = 'add_to_cart')  * 3
             + count(*) FILTER (WHERE event_type = 'order_created') * 5 AS score
        FROM fact_events
        WHERE event_date = %(d)s::date AND product_id IS NOT NULL
        GROUP BY product_id
    ) s
    ON CONFLICT (report_date, product_id) DO UPDATE SET
        views = EXCLUDED.views, carts = EXCLUDED.carts,
        orders = EXCLUDED.orders, score = EXCLUDED.score,
        rank = EXCLUDED.rank, formed_at = now(), run_id = EXCLUDED.run_id
"""

SQL_ABANDONED = """
    INSERT INTO mart_daily_abandoned_carts
        (report_date, sessions_cart, abandoned_count, abandoned_share, run_id)
    WITH carts AS (
        SELECT DISTINCT session_key FROM fact_events
        WHERE event_date = %(d)s::date AND event_type = 'add_to_cart'
    ), orders AS (
        SELECT DISTINCT session_key FROM fact_events
        WHERE event_date = %(d)s::date AND event_type = 'order_created'
    )
    SELECT %(d)s::date,
           (SELECT count(*) FROM carts),
           (SELECT count(*) FROM carts
            WHERE session_key NOT IN (SELECT session_key FROM orders)),
           NULL, %(run)s
    ON CONFLICT (report_date) DO UPDATE SET
        sessions_cart   = EXCLUDED.sessions_cart,
        abandoned_count = EXCLUDED.abandoned_count,
        formed_at       = now(),
        run_id          = EXCLUDED.run_id
"""


def run(run_id: str, dt: str | None = None) -> None:
    """Пересчитывает суточные витрины за указанную дату."""
    dt = dt or date.today().isoformat()
    pg = PostgresLoader()
    params = {"d": dt, "run": run_id}
    pg.execute(SQL_FUNNEL, params)
    pg.execute(SQL_TOP_PRODUCTS, params)
    pg.execute(SQL_ABANDONED, params)
    # Доля брошенных корзин вычисляется отдельным шагом
    pg.execute(
        """UPDATE mart_daily_abandoned_carts
           SET abandoned_share =
               round(abandoned_count::numeric / NULLIF(sessions_cart, 0), 4)
           WHERE report_date = %(d)s::date""",
        {"d": dt},
    )
    logger.info("Суточные витрины за %s сформированы (запуск %s)", dt, run_id)
