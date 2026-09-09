"""Shared API-source defaults used by DAG/manifest and runtime layers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

APICredentialsMode = Literal["vault", "none"]


@dataclass(frozen=True)
class APISourceDefaults:
    """Pure metadata for a logical ``api_type``.

    This contract is intentionally runtime-free so DAG/manifest layers can derive
    compatibility defaults without importing runtime modules.
    """

    api_type: str
    credentials_mode: APICredentialsMode = "vault"
    default_connection_type: str | None = "vault"

    def connection_id(self) -> str:
        return f"api__{self.api_type}"

    def source_schema(self) -> str:
        return f"api__{self.api_type}"

    def source_table(self, resource: str | None) -> str:
        return resource or "data"


_API_SOURCE_DEFAULTS: dict[str, APISourceDefaults] = {
    "omnidesk": APISourceDefaults(api_type="omnidesk", credentials_mode="vault", default_connection_type="vault"),
    "appsflyer": APISourceDefaults(api_type="appsflyer", credentials_mode="vault", default_connection_type="vault"),
    "mindbox": APISourceDefaults(api_type="mindbox", credentials_mode="vault", default_connection_type="vault"),
    "similarweb": APISourceDefaults(api_type="similarweb", credentials_mode="vault", default_connection_type="vault"),
    "openexchangerates": APISourceDefaults(
        api_type="openexchangerates",
        credentials_mode="vault",
        default_connection_type="vault",
    ),
    "google_sheets": APISourceDefaults(
        api_type="google_sheets",
        credentials_mode="vault",
        default_connection_type="vault",
    ),
    "google_ads": APISourceDefaults(
        api_type="google_ads",
        credentials_mode="vault",
        default_connection_type="vault",
    ),
    "yandex_webmaster": APISourceDefaults(
        api_type="yandex_webmaster",
        credentials_mode="vault",
        default_connection_type="vault",
    ),
    "cbr": APISourceDefaults(api_type="cbr", credentials_mode="none", default_connection_type=None),
    "rest": APISourceDefaults(api_type="rest", credentials_mode="vault", default_connection_type="vault"),
    "amplitude": APISourceDefaults(api_type="amplitude", credentials_mode="vault", default_connection_type="vault"),
    "fasttrack": APISourceDefaults(api_type="fasttrack", credentials_mode="vault", default_connection_type="vault"),
    "fastrack": APISourceDefaults(api_type="fasttrack", credentials_mode="vault", default_connection_type="vault"),
}


def get_api_source_defaults(api_type: str | None) -> APISourceDefaults:
    """Returns defaults for a known ``api_type`` or a generic compatibility profile."""

    key = str(api_type or "api").strip().lower() or "api"
    return _API_SOURCE_DEFAULTS.get(
        key,
        APISourceDefaults(api_type=key, credentials_mode="vault", default_connection_type="vault"),
    )


def list_api_source_types() -> list[str]:
    """Lists known API provider identifiers."""

    return sorted(_API_SOURCE_DEFAULTS.keys())


__all__ = [
    "APICredentialsMode",
    "APISourceDefaults",
    "get_api_source_defaults",
    "list_api_source_types",
]
