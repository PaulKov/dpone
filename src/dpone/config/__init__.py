"""Конфигурация dpone ETL фреймворка."""

from dpone.config.env import ENV_CODE, get_env_code
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy

__all__ = [
    "LoadConfig",
    "LoadStrategy",
    "ENV_CODE",
    "get_env_code",
]
