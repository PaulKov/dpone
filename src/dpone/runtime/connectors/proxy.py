"""GCP Proxy Manager для работы с корпоративными прокси серверами."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import quote

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection

logger = logging.getLogger(__name__)


class _AuthorizedSession(Protocol):
    """Minimal session surface used by the proxy manager."""

    credentials: Any
    trust_env: bool
    proxies: MutableMapping[str, str]


AuthorizedSessionFactory = Callable[[Any], _AuthorizedSession]


@dataclass
class GCPProxyManager:
    """
    Управление proxy конфигурацией для GCP сервисов.

    Загружает конфигурацию прокси из HashiCorp Vault и создает
    AuthorizedSession для использования в GCP клиентах (BigQuery, Storage, etc).

    Args:
        credentials: GCP service account credentials
        proxy_mount_point: Vault mount point для proxy конфига
        proxy_path: Путь к секрету с proxy конфигурацией в Vault
        vault_manager_loader: Callable для получения VaultManager
        authorized_session_factory: Optional factory override for tests and
            composition roots. The Google implementation is imported lazily
            when the session capability is first requested.
    """

    credentials: Any
    proxy_mount_point: str
    proxy_path: str = "network/proxy/gcp/current"
    vault_manager_loader: Callable[[], Any] | None = None
    resolved_proxy_config: Mapping[str, Any] | None = field(
        default=None,
        repr=False,
    )
    authorized_session_factory: AuthorizedSessionFactory | None = field(default=None, repr=False, kw_only=True)

    _proxy_session: _AuthorizedSession | None = field(default=None, init=False, repr=False)
    _proxy_config: dict[str, Any] | None = field(default=None, init=False, repr=False)

    DEFAULT_SCOPES: tuple[str, ...] = ("https://www.googleapis.com/auth/cloud-platform",)

    @classmethod
    def from_resolved_connection(
        cls,
        *,
        credentials: Any,
        connection: ResolvedBindingConnection,
    ) -> GCPProxyManager:
        """Build a proxy manager from one workload-scoped credential snapshot."""

        config = _resolved_proxy_config(connection)
        return cls(
            credentials=credentials,
            proxy_mount_point="resolved",
            proxy_path="resolved",
            resolved_proxy_config=config,
        )

    @property
    def proxy_config(self) -> dict[str, Any] | None:
        """
        Возвращает текущую конфигурацию прокси.

        Returns:
            Dict с конфигурацией прокси или None если не загружена
        """
        return self._proxy_config

    def get_authorized_session(self) -> _AuthorizedSession:
        """
        Возвращает AuthorizedSession настроенную для работы через proxy.

        Сессия кэшируется, повторные вызовы возвращают тот же объект.

        Returns:
            AuthorizedSession с настроенным proxy

        Raises:
            RuntimeError: Если vault_manager_loader не настроен
            ValueError: Если proxy конфигурация некорректна
            ModuleNotFoundError: If the optional GCP transport is unavailable
        """
        if self._proxy_session:
            return self._proxy_session

        proxy_config = self._load_proxy_config()
        scoped_credentials = self._get_scoped_credentials()

        session_factory = self.authorized_session_factory or _load_authorized_session_factory()
        session = session_factory(scoped_credentials)
        session.trust_env = False

        proxy_url = self._build_proxy_url(proxy_config)
        session.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }

        no_proxy = proxy_config.get("no_proxy")
        if no_proxy:
            session.proxies["no_proxy"] = no_proxy

        self._proxy_session = session
        return session

    def _load_proxy_config(self) -> dict[str, Any]:
        """
        Загружает конфигурацию прокси из HashiCorp Vault.

        Конфигурация кэшируется в _proxy_config.

        Returns:
            Dict с полями: host, port, protocol, user, password, proxy_name, no_proxy

        Raises:
            ValueError: Если не указаны mount_point/path или отсутствуют обязательные поля
            RuntimeError: Если vault_manager_loader не настроен или ошибка чтения Vault
        """
        if self._proxy_config:
            return self._proxy_config

        if self.resolved_proxy_config is not None:
            self._proxy_config = dict(self.resolved_proxy_config)
            return self._proxy_config

        if not self.proxy_mount_point or not self.proxy_path:
            raise ValueError("Proxy configuration requires mount_point and path")

        if not self.vault_manager_loader:
            raise RuntimeError("Vault manager loader is not configured for GCPProxyManager")

        # Формируем ключ для логирования
        cache_key = f"{self.proxy_mount_point}/{self.proxy_path}"

        manager = self.vault_manager_loader()
        try:
            secret = manager.get_secret(
                mount_point=self.proxy_mount_point,
                path=self.proxy_path.strip("/"),
            )
        except Exception as exc:
            raise RuntimeError(f"Unable to load proxy configuration from Vault ({cache_key}): {exc}") from exc

        # Валидация обязательных полей
        required_keys = ["host", "port", "protocol", "user", "password"]
        missing = [key for key in required_keys if not secret.get(key)]
        if missing:
            raise ValueError(f"Proxy secret `{self.proxy_path}` is missing required fields: {', '.join(missing)}")

        config = {
            "host": secret["host"],
            "port": str(secret["port"]),
            "protocol": secret.get("protocol", "http"),
            "user": secret["user"],
            "password": secret["password"],
            "proxy_name": secret.get("proxy_name"),
            "no_proxy": secret.get("no_proxy"),
            "vault_path": cache_key,
        }

        self._proxy_config = config
        return config

    def _get_scoped_credentials(self) -> Any:
        """
        Возвращает credentials с необходимыми GCP scopes.

        Returns:
            service_account.Credentials с добавленными scopes
        """
        credentials = self.credentials
        if getattr(credentials, "requires_scopes", False):
            credentials = credentials.with_scopes(self.DEFAULT_SCOPES)
        return credentials

    @staticmethod
    def _build_proxy_url(config: dict[str, Any]) -> str:
        """
        Строит proxy URL в формате: protocol://user:password@host:port

        Args:
            config: Dict с полями user, password, protocol, host, port

        Returns:
            Полный proxy URL с URL-encoded credentials
        """
        user = quote(str(config["user"]))
        password = quote(str(config["password"]))
        protocol = config.get("protocol", "http")
        host = config["host"]
        port = config["port"]
        return f"{protocol}://{user}:{password}@{host}:{port}"


def _load_authorized_session_factory() -> AuthorizedSessionFactory:
    """Load the optional Google transport only at the proxy capability boundary."""

    try:
        from google.auth.transport.requests import AuthorizedSession
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in minimal envs
        raise ModuleNotFoundError(
            "google-auth is required for a GCP proxy session. Install dpone with the 'gcp' extra."
        ) from exc
    return cast(AuthorizedSessionFactory, AuthorizedSession)


def _resolved_proxy_config(
    connection: ResolvedBindingConnection,
) -> dict[str, Any]:
    descriptor = connection.descriptor
    if descriptor is None:
        raise ValueError("Resolved proxy connection descriptor is required")
    properties = descriptor.properties
    credentials = connection.credentials
    params = credentials.additional_params or {}
    config = {
        "host": credentials.host or properties.get("host"),
        "port": str(credentials.port or properties.get("port") or ""),
        "protocol": params.get("protocol") or properties.get("protocol") or "http",
        "user": credentials.username,
        "password": credentials.password,
        "proxy_name": params.get("proxy_name") or properties.get("proxy_name"),
        "no_proxy": params.get("no_proxy") or properties.get("no_proxy"),
    }
    missing = [key for key in ("host", "port", "protocol", "user", "password") if not config.get(key)]
    if missing:
        raise ValueError("Resolved proxy connection is missing required fields: " + ", ".join(missing))
    return config
