"""Canonical runtime registry for API-backed sources."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection

import inspect
from collections.abc import Mapping
from importlib import import_module
from typing import Any

from dpone.contracts.api_sources import list_api_source_types
from dpone.runtime.api_providers.models import APIProviderRuntimeSpec
from dpone.runtime.api_providers.specs import load_runtime_specs
from dpone.runtime.credentials.config import CredentialsSource
from dpone.runtime.credentials.manager import CredentialsManager
from dpone.runtime.errors import RuntimeConfigurationError

_RUNTIME_SPECS: dict[str, APIProviderRuntimeSpec] = load_runtime_specs()


def _load_object(target: str) -> Any:
    module_name, attr = target.split(":", 1)
    module = import_module(module_name)
    return getattr(module, attr)


def get_api_provider_spec(api_type: str | None) -> APIProviderRuntimeSpec:
    key = str(api_type or "").strip().lower()
    if not key:
        raise RuntimeConfigurationError(
            "Для source type='api' необходимо указать api_type "
            f"(поддерживаемые/зарегистрированные: {', '.join(list_api_source_types())})"
        )
    spec = _RUNTIME_SPECS.get(key)
    if spec is None:
        raise RuntimeConfigurationError(
            f"Неизвестный api_type='{key}'. Поддерживаемые/зарегистрированные: {', '.join(list_api_source_types())}"
        )
    return spec


def list_registered_api_provider_specs() -> list[APIProviderRuntimeSpec]:
    return [_RUNTIME_SPECS[name] for name in list_api_source_types() if name in _RUNTIME_SPECS]


def build_api_runtime_source(
    *,
    source_cfg: Mapping[str, Any],
    vault_path: str | None,
    vault_mount_point: str | None = None,
    sink_connector: Any = None,
    logger: Any = None,
    credentials_manager: CredentialsManager | None = None,
) -> Any:
    """Builds a runtime source for ``type=api`` using canonical registry wiring."""

    spec = get_api_provider_spec(source_cfg.get("api_type"))
    if not spec.is_implemented:
        raise RuntimeConfigurationError(
            f"API provider '{spec.api_type}' зарегистрирован, но runtime-реализация ещё не добавлена в этот build."
        )

    options = source_cfg.get("options", {}) or {}
    connector_kwargs = spec.connector_kwargs_factory(options) if spec.connector_kwargs_factory else {}
    connection_type = str(source_cfg.get("connection_type") or spec.defaults.default_connection_type or "vault")
    connection_id = str(source_cfg.get("connection_id") or spec.defaults.connection_id())
    if spec.defaults.credentials_mode == "vault" and connection_type == "vault" and not vault_path:
        raise RuntimeConfigurationError(f"Для api_type='{spec.api_type}' требуется vault_path (credentials_mode=vault)")

    try:
        connector_cls = _load_object(spec.connector_target or "")
        source_cls = _load_object(spec.source_target or "")
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on staged rollout
        raise RuntimeConfigurationError(
            f"API provider '{spec.api_type}' зарегистрирован, но модули runtime-реализации отсутствуют: {exc}"
        ) from exc

    if spec.defaults.credentials_mode == "vault":
        if connection_type == "vault":
            if not vault_path:
                raise RuntimeConfigurationError(
                    f"Для api_type='{spec.api_type}' требуется vault_path (credentials_mode=vault)"
                )
            if not hasattr(connector_cls, "from_vault"):
                raise RuntimeConfigurationError(
                    f"Connector для api_type='{spec.api_type}' не поддерживает from_vault(), хотя credentials_mode=vault"
                )
            connector = _call_legacy_from_vault(
                connector_cls,
                api_type=spec.api_type,
                vault_path=vault_path,
                vault_mount_point=vault_mount_point,
                connector_kwargs=connector_kwargs,
            )
        else:
            if not hasattr(connector_cls, "from_credentials"):
                raise RuntimeConfigurationError(
                    f"Connector для api_type='{spec.api_type}' не поддерживает connection_type='{connection_type}'. "
                    "Добавьте from_credentials() или используйте connection_type='vault'."
                )
            manager = credentials_manager or CredentialsManager()
            credentials = manager.get_credentials(
                connection_id,
                CredentialsSource(connection_type),
                vault_mount_point,
                vault_path,
            )
            connector = connector_cls.from_credentials(credentials, **connector_kwargs)
    else:
        connector = connector_cls(**connector_kwargs)

    return source_cls(
        connector=connector,
        sink_connector=sink_connector,
        logger=logger,
    )


def build_api_runtime_source_from_connection(
    *,
    source_cfg: Mapping[str, Any],
    resolved_connection: ResolvedBindingConnection | None,
    sink_connector: Any = None,
    logger: Any = None,
) -> Any:
    """Build an API source from explicit workload-scoped credentials only."""

    spec = get_api_provider_spec(source_cfg.get("api_type"))
    if not spec.is_implemented:
        raise RuntimeConfigurationError(
            f"API provider '{spec.api_type}' зарегистрирован, но runtime-реализация отсутствует."
        )
    options = source_cfg.get("options", {}) or {}
    connector_kwargs = spec.connector_kwargs_factory(options) if spec.connector_kwargs_factory else {}
    try:
        connector_cls = _load_object(spec.connector_target or "")
        source_cls = _load_object(spec.source_target or "")
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on staged rollout.
        raise RuntimeConfigurationError(f"API provider '{spec.api_type}' runtime modules are unavailable.") from exc

    if spec.defaults.credentials_mode == "none":
        connector = connector_cls(**connector_kwargs)
    else:
        if resolved_connection is None:
            raise RuntimeConfigurationError(f"API provider '{spec.api_type}' requires a resolved connection_ref.")
        if not spec.credentials_target:
            raise RuntimeConfigurationError(f"API provider '{spec.api_type}' has no explicit credentials adapter.")
        credentials_cls = _load_object(spec.credentials_target)
        from_dict = getattr(credentials_cls, "from_dict", None)
        if not callable(from_dict):
            raise RuntimeConfigurationError(
                f"API provider '{spec.api_type}' credentials adapter has no from_dict contract."
            )
        provider_credentials = from_dict(_api_credentials_payload(resolved_connection))
        connector = _build_resolved_connector(
            spec=spec,
            connector_cls=connector_cls,
            credentials=provider_credentials,
            connector_kwargs=connector_kwargs,
        )
    return source_cls(
        connector=connector,
        sink_connector=sink_connector,
        logger=logger,
    )


def _call_legacy_from_vault(
    connector_cls: Any,
    *,
    api_type: str,
    vault_path: str,
    vault_mount_point: str | None,
    connector_kwargs: Mapping[str, Any],
) -> Any:
    """Invoke ``from_vault`` with only parameters declared by the connector."""

    try:
        signature = inspect.signature(connector_cls.from_vault)
    except (TypeError, ValueError) as exc:
        raise RuntimeConfigurationError(
            f"API provider '{api_type}' from_vault() signature is not inspectable."
        ) from exc
    parameters = signature.parameters
    accepts_var_keyword = any(item.kind is inspect.Parameter.VAR_KEYWORD for item in parameters.values())
    bound: dict[str, Any] = {}
    if "vault_path" in parameters:
        bound["vault_path"] = vault_path
    elif accepts_var_keyword:
        bound["vault_path"] = vault_path
    else:
        raise RuntimeConfigurationError(f"API provider '{api_type}' from_vault() must accept vault_path.")
    mount = vault_mount_point
    if mount is not None:
        if "vault_mount_point" in parameters:
            bound["vault_mount_point"] = mount
        elif "mount_point" in parameters:
            bound["mount_point"] = mount
        elif accepts_var_keyword:
            bound["vault_mount_point"] = mount
    for key, value in connector_kwargs.items():
        if key in {"vault_path", "vault_mount_point", "mount_point"}:
            continue
        if key in parameters or accepts_var_keyword:
            bound[key] = value
    try:
        return connector_cls.from_vault(**bound)
    except TypeError as exc:
        raise RuntimeConfigurationError(
            f"API provider '{api_type}' from_vault() rejected the legacy factory arguments."
        ) from exc


def _api_credentials_payload(
    connection: ResolvedBindingConnection,
) -> dict[str, Any]:
    credentials = connection.credentials
    descriptor = connection.descriptor
    payload = dict(descriptor.properties) if descriptor is not None else {}
    payload.update(dict(credentials.additional_params or {}))
    for key in (
        "host",
        "port",
        "database",
        "username",
        "password",
        "schema",
        "project_id",
        "service_account_key_file",
        "service_account_info",
        "endpoint",
        "token",
        "api_key",
        "client_id",
    ):
        value = getattr(credentials, key)
        if value is not None:
            payload[key] = value
    if credentials.api_key is not None:
        payload.setdefault("access_token", credentials.api_key)
    if credentials.token is not None:
        payload.setdefault("oauth_token", credentials.token)
    return payload


def _build_resolved_connector(
    *,
    spec: APIProviderRuntimeSpec,
    connector_cls: Any,
    credentials: Any,
    connector_kwargs: Mapping[str, Any],
) -> Any:
    """Adapt declarative options to the connector's explicit typed contract."""

    if spec.resolved_connector_mode == "direct":
        return connector_cls(credentials, **dict(connector_kwargs))

    from dpone.runtime.connectors.api.base import (
        APIRateLimitConfig,
        APIRetryConfig,
        ConcurrencyConfig,
    )

    max_retries = connector_kwargs.get("max_retries")
    retry_config = (
        APIRetryConfig(
            max_retries=int(max_retries),
            backoff_factor=float(connector_kwargs.get("retry_delay", 0.5)),
        )
        if max_retries is not None
        else None
    )
    rate_limit_delay = connector_kwargs.get("rate_limit_delay")
    rate_limit_config = None
    if rate_limit_delay is not None:
        delay = float(rate_limit_delay)
        default_rate_limit = getattr(connector_cls, "DEFAULT_RATE_LIMIT", None)
        rate_limit_config = APIRateLimitConfig(
            requests_per_second=1.0 / delay if delay > 0 else 10.0,
            burst_limit=int(getattr(default_rate_limit, "burst_limit", 20)),
        )
    adapted: dict[str, Any] = {
        "retry_config": retry_config,
        "rate_limit_config": rate_limit_config,
        "timeout": int(connector_kwargs.get("timeout", 60)),
    }
    if spec.resolved_connector_mode in {"concurrent", "appsflyer"}:
        parallel_workers = connector_kwargs.get("parallel_workers")
        adapted["concurrency_config"] = (
            ConcurrencyConfig(max_workers=int(parallel_workers)) if parallel_workers is not None else None
        )
    if spec.resolved_connector_mode == "appsflyer":
        adapted["default_app_id"] = connector_kwargs.get("default_app_id")
    return connector_cls(credentials, **adapted)


__all__ = [
    "APIProviderRuntimeSpec",
    "build_api_runtime_source",
    "build_api_runtime_source_from_connection",
    "get_api_provider_spec",
    "list_registered_api_provider_specs",
]
