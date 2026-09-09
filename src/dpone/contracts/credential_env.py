"""Shared environment-variable naming contract for dpone connections."""

from __future__ import annotations

import re

CANONICAL_CONNECTION_ENV_PREFIX = "DPONE_CONN_"
CONNECTION_REF_MAX_LENGTH = 128
CONNECTION_REF_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,126}[A-Za-z0-9])?$"
ENV_VAR_NAME_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"

_CONNECTION_REF_RE = re.compile(CONNECTION_REF_PATTERN)
_ENV_NAME_RE = re.compile(r"[^A-Za-z0-9]+")
_ENV_VAR_NAME_RE = re.compile(ENV_VAR_NAME_PATTERN)


def connection_env_suffix(connection_id: str) -> str:
    """Return the portable uppercase suffix used in connection env variables."""

    suffix = _ENV_NAME_RE.sub("_", str(connection_id or "").strip()).strip("_").upper()
    return suffix


def connection_env_name(
    connection_id: str,
    field: str,
    *,
    prefix: str = CANONICAL_CONNECTION_ENV_PREFIX,
) -> str:
    """Build the canonical env var name for one connection field."""

    connection_suffix = connection_env_suffix(connection_id)
    field_suffix = connection_env_suffix(field)
    return f"{prefix}{connection_suffix}_{field_suffix}".upper()


def is_valid_env_var_name(value: object) -> bool:
    """Return True when value is a portable environment variable name."""

    return isinstance(value, str) and bool(_ENV_VAR_NAME_RE.fullmatch(value))


def is_valid_connection_ref(value: object) -> bool:
    """Return True when value is a safe logical connection alias."""

    return (
        isinstance(value, str) and len(value) <= CONNECTION_REF_MAX_LENGTH and bool(_CONNECTION_REF_RE.fullmatch(value))
    )


__all__ = [
    "CANONICAL_CONNECTION_ENV_PREFIX",
    "CONNECTION_REF_MAX_LENGTH",
    "CONNECTION_REF_PATTERN",
    "ENV_VAR_NAME_PATTERN",
    "connection_env_name",
    "connection_env_suffix",
    "is_valid_connection_ref",
    "is_valid_env_var_name",
]
