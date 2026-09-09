"""Утилиты для работы с чувствительными данными (credentials, secrets, keys)."""

from __future__ import annotations

from typing import Any

# Список sensitive параметров для автоматического маскирования
SENSITIVE_PARAM_NAMES: set[str] = {
    # Password variations
    "password",
    "passwd",
    "pwd",
    # Access keys
    "access_key",
    "access_key_id",
    "ak",
    # Secret keys
    "secret_key",
    "secret_access_key",
    "sk",
    # API keys
    "api_key",
    "apikey",
    # Tokens
    "token",
    "access_token",
    "refresh_token",
    "auth_token",
    # Credentials
    "credential",
    "credentials",
    # Private keys
    "private_key",
    "priv_key",
    # Session IDs
    "session_id",
    "sessid",
}


def mask_sensitive_value(value: str, visible_chars: int = 4) -> str:
    """
    Маскирует чувствительное значение звездочками, оставляя видимыми только первые N символов.

    Args:
        value: Строка для маскировки (password, access key, token, и т.д.)
        visible_chars: Количество видимых символов в начале

    Returns:
        Замаскированная строка (например: "GOOG****")
    """
    if not value or len(value) <= visible_chars:
        return "****"
    return f"{value[:visible_chars]}****"


def mask_sensitive_params(
    params: Any | None,
    sensitive_keys: set[str] | None = None,
    visible_chars: int = 4,
) -> Any | None:
    """
    Маскирует чувствительные параметры в структуре данных.

    Обрабатывает dict, list, tuple. Автоматически определяет sensitive поля по именам.

    Args:
        params: Параметры для маскировки (dict, list, tuple или примитив)
        sensitive_keys: Дополнительные ключи для маскировки (добавляются к SENSITIVE_PARAM_NAMES)
        visible_chars: Количество видимых символов

    Returns:
        Копия структуры с замаскированными значениями
    """
    if params is None:
        return None

    # Объединяем дефолтные и кастомные sensitive keys
    all_sensitive_keys = SENSITIVE_PARAM_NAMES.copy()
    if sensitive_keys:
        all_sensitive_keys.update(k.lower() for k in sensitive_keys)

    # Dict обработка
    if isinstance(params, dict):
        masked = {}
        for key, value in params.items():
            key_lower = str(key).lower()
            if key_lower in all_sensitive_keys and isinstance(value, str):
                masked[key] = mask_sensitive_value(value, visible_chars)
            elif isinstance(value, dict | list | tuple):
                # Рекурсивно обрабатываем вложенные структуры
                masked[key] = mask_sensitive_params(value, sensitive_keys, visible_chars)
            else:
                masked[key] = value
        return masked

    # List/Tuple обработка - маскируем строковые значения после sensitive ключей
    if isinstance(params, list | tuple):
        masked = []
        for i, item in enumerate(params):
            # Если предыдущий элемент — sensitive ключ, маскируем текущий
            if i > 0 and isinstance(params[i - 1], str):
                prev_key_lower = str(params[i - 1]).lower()
                if prev_key_lower in all_sensitive_keys and isinstance(item, str):
                    masked.append(mask_sensitive_value(item, visible_chars))
                    continue

            if isinstance(item, dict | list | tuple):
                masked.append(mask_sensitive_params(item, sensitive_keys, visible_chars))
            else:
                masked.append(item)

        return type(params)(masked) if isinstance(params, tuple) else masked

    # Примитивные типы возвращаем как есть
    return params
