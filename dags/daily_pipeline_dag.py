"""Ежесуточный пакетный конвейер (Apache Airflow).

Граф задач:
    extract_api >> transform_batch >> quality_check >> load_marts >> backup_dwh

  * extract_api      — инкрементальное извлечение событий из внешней
                       базы данных через программный интерфейс (слой Raw);
  * transform_batch  — очистка, нормализация и дедупликация (слой Staging),
                       загрузка таблицы фактов;
  * quality_check    — проверки качества данных Great Expectations;
  * load_marts       — пересчёт суточных аналитических витрин;
  * backup_dwh       — резервная копия хранилища витрин в озеро данных.

При сбое задача перезапускается: три попытки с увеличивающимся интервалом
ожидания (требование надёжности). Все задачи идемпотентны: повторный
запуск за ту же дату даёт тот же результат.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.config import load_config
from src.common.logging_setup import get_logger
from src.extract.api_extractor import extract as extract_api
from src.load import backup as backup_module
from src.load import marts_loader
from src.quality import validate_staging
from src.transform import batch_transform

logger = get_logger("dags.daily_pipeline")

cfg = load_config()

DEFAULT_ARGS = {
    "owner": "rtp",
    "depends_on_past": False,
    "retries": 3,                                   # требование надёжности
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def _dt(context) -> str:
    """Логическая дата запуска — целевые сутки пакетной обработки."""
    return context["ds"]


with DAG(
    dag_id="daily_pipeline",
    description="Ежесуточная агрегация, контроль качества и резервное "
                "копирование конвейера АС «RTP»",
    schedule=cfg["batch"]["daily_schedule"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["rtp", "batch"],
) as dag:

    extract_api_task = PythonOperator(
        task_id="extract_api",
        python_callable=lambda **ctx: extract_api(run_id=ctx["run_id"]),
    )

    transform_task = PythonOperator(
        task_id="transform_batch",
        python_callable=lambda **ctx: batch_transform.run(
            run_id=ctx["run_id"], dt=_dt(ctx)),
    )

    quality_task = PythonOperator(
        task_id="quality_check",
        python_callable=lambda **ctx: validate_staging.run(
            run_id=ctx["run_id"], dt=_dt(ctx)),
    )

    marts_task = PythonOperator(
        task_id="load_marts",
        python_callable=lambda **ctx: marts_loader.run(
            run_id=ctx["run_id"], dt=_dt(ctx)),
    )

    backup_task = PythonOperator(
        task_id="backup_dwh",
        python_callable=lambda **ctx: backup_module.run(dt=_dt(ctx)),
    )

    extract_api_task >> transform_task >> quality_task >> marts_task >> backup_task
