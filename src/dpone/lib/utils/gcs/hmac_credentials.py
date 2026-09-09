"""
GCS HMAC credentials manager.

Модуль для получения и управления HMAC credentials для доступа к Google Cloud Storage.
Используется для S3-compatible API доступа к GCS (например, из ClickHouse).
"""

from __future__ import annotations

import logging

from dpone.runtime.connector_logging import etl_logger

logger = logging.getLogger(__name__)


def get_gcs_hmac_credentials(
    *,
    hmac_key: str | None = None,
    hmac_secret: str | None = None,
    vault_path: str | None = None,
    auto_generate_vault_path: bool = False,
) -> tuple[str, str]:
    """
    Получает HMAC credentials для GCS доступа через S3-compatible API.

    Приоритет получения credentials:
    1. Явно переданные hmac_key и hmac_secret
    2. Из Vault по указанному пути (vault_path)
    3. Из Vault по явно указанному vault_path

    Args:
        hmac_key: Явно указанный HMAC access key (опционально)
        hmac_secret: Явно указанный HMAC secret key (опционально)
        vault_path: Полный путь в Vault для получения credentials (опционально)
                   Формат: 'mount_point/path'
        auto_generate_vault_path: Deprecated no-op; implicit Vault paths are disabled

    Returns:
        Tuple[str, str]: (hmac_key, hmac_secret)
    """
    # 1. Если credentials явно переданы
    if hmac_key and hmac_secret:
        etl_logger.info("Using explicitly provided HMAC credentials")
        return hmac_key, hmac_secret

    if auto_generate_vault_path:
        from dpone.runtime.errors import raise_legacy_runtime_defaults_disabled

        raise_legacy_runtime_defaults_disabled(
            detail="Automatic GCS HMAC Vault path generation is disabled; set vault_path explicitly.",
        )

    if vault_path:
        return _get_hmac_from_vault(vault_path)

    from dpone.runtime.errors import raise_legacy_runtime_defaults_disabled

    raise_legacy_runtime_defaults_disabled(
        detail="Explicit GCS HMAC credentials or vault_path are required.",
    )


def _get_hmac_from_vault(vault_path: str) -> tuple[str, str]:
    """
    Внутренняя функция для получения HMAC credentials из Vault.

    Args:
        vault_path: Явно указанный путь в Vault

    Returns:
        Tuple[str, str]: (hmac_key, hmac_secret)

    Raises:
        ValueError: Если credentials не найдены или имеют неверный формат
        RuntimeError: При ошибке получения из Vault
    """
    try:
        from vault_kv_client import get_default_manager

        resolved_vault_path = vault_path.strip()
        if not resolved_vault_path:
            raise ValueError("Vault path must be a non-empty mount_point/path value")

        vault_manager = get_default_manager()

        # Парсим vault_path: mount_point/path
        parts = resolved_vault_path.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid Vault path format: {resolved_vault_path}. Expected: mount_point/path")

        mount_point = parts[0]
        path = parts[1]

        # Читаем секрет из Vault
        secret_data = vault_manager.get_secret(mount_point=mount_point, path=path)

        if not secret_data:
            raise ValueError(f"HMAC credentials not found at Vault path: {resolved_vault_path}")

        # GCS HMAC keys хранятся как access_key и secret_key
        hmac_key = secret_data.get("access_key") or secret_data.get("access_id")
        hmac_secret = secret_data.get("secret_key") or secret_data.get("secret")

        if not hmac_key or not hmac_secret:
            raise ValueError(
                f"Invalid HMAC credentials format in Vault at {resolved_vault_path}. "
                f"Expected 'access_key' and 'secret_key' fields."
            )

        etl_logger.info("Successfully loaded GCS HMAC credentials from Vault: %s", resolved_vault_path)
        return hmac_key, hmac_secret

    except Exception as e:
        raise RuntimeError(
            f"Failed to get GCS HMAC credentials: {str(e)}. Ensure hmac_key/hmac_secret are set or Vault is configured."
        ) from e
