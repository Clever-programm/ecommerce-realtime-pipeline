"""Загрузка конфигурации конвейера.

Изменяемые параметры вынесены в config/pipeline.yaml (требование
конфигурируемости НФТ); секреты и сетевые адреса — в переменные окружения.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "pipeline.yaml"


@lru_cache(maxsize=1)
def load_config() -> dict:
    """Читает и кэширует конфигурацию конвейера."""
    path = Path(os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH))
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def get_env(name: str, default: str | None = None, required: bool = False) -> str:
    """Возвращает переменную окружения с контролем обязательности."""
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения {name}")
    return value
