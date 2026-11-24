"""Программный интерфейс доступа к внешней БД с подготовленными событиями.

Конвейер не подключается к внешней базе напрямую: записи возвращаются
постранично в формате JSON через этот сервис. Аутентификация — ключом
в заголовке X-API-Key (требование ФТ к подключению к API).
"""

from __future__ import annotations

import os
from datetime import datetime

import psycopg2
import psycopg2.extras
from fastapi import Depends, FastAPI, Header, HTTPException, Query

app = FastAPI(title="External events API", version="1.0.0")

DSN = {
    "host": os.environ.get("POSTGRES_EXT_HOST", "postgres-external"),
    "port": int(os.environ.get("POSTGRES_EXT_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_EXT_DB", "shop_source"),
    "user": os.environ.get("POSTGRES_EXT_USER", "source_ro"),
    "password": os.environ.get("POSTGRES_EXT_PASSWORD", ""),
}
API_KEY = os.environ.get("EXTERNAL_API_KEY", "")


def check_key(x_api_key: str = Header(default="")) -> None:
    """Проверяет ключ доступа."""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid API key")


@app.get("/health")
def health() -> dict:
    """Проверка доступности сервиса."""
    return {"status": "ok"}


@app.get("/events", dependencies=[Depends(check_key)])
def list_events(
    since: datetime | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Возвращает события постранично в формате JSON.

    Параметр since задаёт нижнюю границу метки времени и используется
    для инкрементального чтения со стороны конвейера.
    """
    sql = """
        SELECT event_id, event_type, event_ts, session_id,
               user_key, product_id, price, device
        FROM events
        WHERE (%s IS NULL OR event_ts > %s)
        ORDER BY event_ts, event_id
        LIMIT %s OFFSET %s
    """
    with psycopg2.connect(**DSN) as conn, conn.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.execute(sql, (since, since, limit, offset))
        rows = cur.fetchall()
        cur.execute(
            "SELECT count(*) AS n FROM events WHERE (%s IS NULL OR event_ts > %s)",
            (since, since),
        )
        total = cur.fetchone()["n"]
    for row in rows:
        row["event_ts"] = row["event_ts"].isoformat().replace("+00:00", "Z")
        row["price"] = float(row["price"]) if row["price"] is not None else None
    return {"items": rows, "total": total, "limit": limit, "offset": offset}
