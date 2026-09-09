"""GCS client creation utilities."""

from __future__ import annotations

from google.cloud import storage
from google.oauth2 import service_account

from dpone.config import get_env_code
from dpone.runtime.credentials.config import CredentialsSource
from dpone.runtime.credentials.manager import CredentialsManager


def create_gcs_client(
    env_code: str | None = None,
    target_conn_id: str = "bigquery_default",
    vault_path: str | None = None,
) -> tuple[storage.Client, str]:
    """
    Создает authenticated GCS client из Vault credentials.

    **Универсальная функция для переиспользования в разных модулях:**
    - GCSCleaner
    - GCSIncrementalExporter
    - Другие утилиты работы с GCS

    Args:
        env_code: Код окружения (dev/prod). По умолчанию из get_env_code()
        target_conn_id: ID подключения в Vault (по умолчанию 'bigquery_default')
        vault_path: Путь к секрету в Vault. Обязателен; implicit defaults отключены.
    """
    # Определяем env_code
    if not env_code:
        env_code = get_env_code()

    if not vault_path or not str(vault_path).strip():
        from dpone.runtime.errors import raise_legacy_runtime_defaults_disabled

        raise_legacy_runtime_defaults_disabled(
            detail="Explicit vault_path is required to create a GCS client.",
        )
    vault_path = str(vault_path).strip()

    # Получаем credentials из Vault
    creds_manager = CredentialsManager()
    creds_obj = creds_manager.get_credentials(
        connection_name=target_conn_id,
        source=CredentialsSource.VAULT,
        mount_point=env_code,
        path=vault_path,
    )

    # Извлекаем service account info
    key_info = creds_obj.service_account_info or creds_obj.additional_params or {}

    if not key_info:
        raise ValueError(
            f"Service account credentials not found in Vault at {env_code}/{vault_path}. "
            "Ensure the secret contains 'service_account_info' or 'service_account' field."
        )

    # Создаем credentials и client
    try:
        credentials = service_account.Credentials.from_service_account_info(key_info)
        project_id = creds_obj.project_id or key_info.get("project_id")
        if not project_id:
            raise ValueError(
                f"GCP project_id not found in Vault credentials at {env_code}/{vault_path}. "
                "Ensure the secret exposes project_id explicitly."
            )

        client = storage.Client(
            project=project_id,
            credentials=credentials,
        )

        return client, project_id

    except Exception as exc:
        raise RuntimeError(
            f"Failed to create GCS client from credentials: {exc}. "
            f"Check service account format in Vault at {env_code}/{vault_path}"
        ) from exc
