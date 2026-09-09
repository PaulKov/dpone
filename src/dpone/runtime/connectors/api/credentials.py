"""Credential models for API connectors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass
class APICredentials:
    """
    Базовые креденшиалы для API.

    Наследники могут расширять для специфичных методов аутентификации:
    - Basic Auth (email + token)
    - Bearer Token
    - OAuth2
    - API Key в header/query
    """

    endpoint: str
    auth_type: str = "basic"

    # Basic Auth
    email: str | None = None
    token: str | None = None

    # Bearer / API Key
    api_key: str | None = None
    api_key_header: str = "Authorization"
    api_key_prefix: str = "Bearer"

    # Additional headers
    extra_headers: dict[str, str] = field(default_factory=dict)

    def get_auth(self) -> tuple[str, str] | None:
        """Возвращает tuple для requests.auth (Basic Auth)."""
        if self.auth_type == "basic" and self.email and self.token:
            return (self.email, self.token)
        return None

    def get_headers(self) -> dict[str, str]:
        """Возвращает headers для запроса."""
        headers = {"Content-Type": "application/json"}
        headers.update(self.extra_headers)

        if self.auth_type == "bearer" and self.api_key:
            headers[self.api_key_header] = f"{self.api_key_prefix} {self.api_key}"
        elif self.auth_type == "api_key" and self.api_key:
            headers[self.api_key_header] = self.api_key

        return headers


def resolve_generic_api_token(
    config: Mapping[str, Any],
    *,
    required: bool = True,
    context: str = "API credentials",
) -> str | None:
    """Resolve a generic token-like credential with backward compatibility.

    Preferred external key is ``token``. ``api_key`` remains supported as a
    compatibility alias for existing Vault secrets and config payloads.
    """

    token = config.get("token")
    if token is not None:
        return str(token)

    api_key = config.get("api_key")
    if api_key is not None:
        return str(api_key)

    if required:
        raise KeyError(f"{context} must define either 'token' or 'api_key'")
    return None


__all__ = ["APICredentials", "resolve_generic_api_token"]
