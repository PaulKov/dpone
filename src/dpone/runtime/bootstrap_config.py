"""Small configuration helpers for runtime bootstrap."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.env import ENV_CODE
from dpone.runtime.errors import RuntimeConfigurationError


def mapping_or_empty(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def resolve_state_vault_path(
    state_cfg: Mapping[str, Any],
    sink_cfg: Mapping[str, Any] | None = None,
) -> tuple[str, str]:
    """Resolve vault mount point and path for state storage."""

    del sink_cfg
    mount_point = state_cfg.get("vault_mount_point")
    path = state_cfg.get("vault_path")
    if mount_point is None or str(mount_point).strip() == "":
        raise RuntimeConfigurationError(
            "state.vault_mount_point must be set explicitly; implicit ENV_CODE default is disabled"
        )
    if path is None or str(path).strip() == "":
        raise RuntimeConfigurationError(
            "state.vault_path must be set explicitly; implicit tenant Vault path defaults are disabled"
        )
    return str(mount_point), str(path)


def require_state_vault_mount(state_cfg: Mapping[str, Any], credentials_source: Any) -> str:
    """Resolve an explicit state Vault mount without environment fallbacks."""

    mount_point = state_cfg.get("vault_mount_point")
    if str(credentials_source).strip().lower() == "vault" and not _has_text(mount_point):
        raise RuntimeConfigurationError(
            "state.vault_mount_point must be set explicitly when credentials_source='vault'"
        )
    if not _has_text(mount_point):
        raise RuntimeConfigurationError(
            "state.vault_mount_point must be set explicitly; implicit ENV_CODE default is disabled"
        )
    return str(mount_point)


def resolve_proxy_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Extract global BigQuery proxy settings."""

    proxy_cfg = mapping_or_empty(config.get("bigquery_proxy", {}))
    proxy_enable = proxy_cfg.get("proxy_enable", False)
    if not isinstance(proxy_enable, bool):
        raise RuntimeConfigurationError("bigquery_proxy.proxy_enable должен быть булевым значением")
    proxy_mount_point = proxy_cfg.get("vault_mount_point", ENV_CODE)
    return {
        "proxy_enable": proxy_enable,
        "vault_mount_point": proxy_mount_point,
        "vault_path": proxy_cfg.get("vault_path", "network/proxy/gcp/current"),
    }


def _has_text(value: object) -> bool:
    return value is not None and bool(str(value).strip())


__all__ = [
    "mapping_or_empty",
    "require_state_vault_mount",
    "resolve_proxy_config",
    "resolve_state_vault_path",
]
