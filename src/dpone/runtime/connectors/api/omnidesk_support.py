from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any

from dataclasses import dataclass, field

from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.base import APICredentials, PaginationConfig


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


@dataclass
class OmnideskCredentials(APICredentials):
    """
    Креденшиалы для Omnidesk API.

    Omnidesk использует Basic Auth:
    - email: Email пользователя
    - token: API токен
    """

    auth_type: str = field(default="basic", init=False)

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
    ) -> OmnideskCredentials:
        """
        Создаёт креденшиалы из Vault.

        mount_point определяется автоматически из ENV_CODE (переменная окружения).

        Args:
            vault_path: Путь к секрету в Vault (обязательный)
            vault_manager: VaultManager (опционально, иначе используется default)

        Returns:
            OmnideskCredentials

        Example:
            # Для пути {ENV_CODE}/api/omnidesk:
            creds = OmnideskCredentials.from_vault(vault_path="api/omnidesk")
        """
        vm = vault_manager or get_default_manager()
        mount_point = get_env_code()

        secret = vm.get_secret(mount_point=mount_point, path=vault_path)

        return cls(
            endpoint=secret["endpoint"],
            email=secret.get("email"),
            token=secret.get("token"),
        )

    @classmethod
    def from_dict(cls, config: dict[str, str]) -> OmnideskCredentials:
        """Создаёт креденшиалы из словаря."""
        return cls(
            endpoint=config["endpoint"],
            email=config.get("email"),
            token=config.get("token"),
        )


class OmnideskPagination(PaginationConfig):
    """
    Пагинация для Omnidesk API.

    Особенности Omnidesk:
    - Использует page-based пагинацию
    - Максимум 100 записей на страницу
    - Ответ содержит числовые ключи: {"0": {...}, "1": {...}, "total_count": N}
    """

    def __init__(self):
        super().__init__(
            strategy="page",
            page_param="page",
            limit_param="limit",
            default_page_size=100,
            max_page_size=100,
            total_count_key="total_count",
            data_key=None,  # Omnidesk возвращает специфичный формат
        )
