"""Утилиты для логирования SQL запросов с маскированием чувствительных данных."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from dpone.lib.utils.security import mask_sensitive_params
from dpone.runtime.connector_logging import etl_logger

logger = logging.getLogger(__name__)


def log_query_with_params(
    query: Any,
    params: Iterable[Any] | None = None,
    *,
    sensitive_keys: set[str] | None = None,
    log_level: int = logging.INFO,
) -> None:
    """
    Логирует SQL запрос с параметрами, автоматически маскируя чувствительные данные.

    Автоматически определяет и маскирует: password, access_key, secret_key, token, и т.д.

    Args:
        query: SQL запрос (строка или объект)
        params: Параметры запроса (dict, list, tuple или None)
        sensitive_keys: Дополнительные ключи для маскировки
        log_level: Уровень логирования (по умолчанию INFO)
    """
    if params:
        masked_params = mask_sensitive_params(params, sensitive_keys)
        etl_logger.info("SQL: %s | params: %s", query, masked_params)
    else:
        etl_logger.info("SQL: %s", query)
