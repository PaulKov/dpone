"""Dependency-free Vault reference validators."""

from __future__ import annotations

import re

VAULT_MOUNT_MAX_LENGTH = 128
VAULT_MOUNT_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
VAULT_LOGICAL_PATH_MAX_LENGTH = 512
VAULT_LOGICAL_PATH_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]*(/[A-Za-z0-9][A-Za-z0-9_.-]*)*$"

_MOUNT_RE = re.compile(VAULT_MOUNT_PATTERN)
_LOGICAL_PATH_RE = re.compile(VAULT_LOGICAL_PATH_PATTERN)
_FORBIDDEN_API_PATH_RE = re.compile(r"(^|/)(v1|data)(/|$)")


def is_valid_vault_mount(value: object) -> bool:
    """Return True when value is a logical Vault mount name, not an API path."""

    if not isinstance(value, str):
        return False
    normalized = value.strip()
    return bool(normalized) and len(normalized) <= VAULT_MOUNT_MAX_LENGTH and bool(_MOUNT_RE.fullmatch(normalized))


def is_valid_vault_logical_path(value: object) -> bool:
    """Return True when value is a relative logical Vault secret path."""

    if not isinstance(value, str):
        return False
    normalized = value.strip()
    if not normalized or len(normalized) > VAULT_LOGICAL_PATH_MAX_LENGTH:
        return False
    if _FORBIDDEN_API_PATH_RE.search(normalized):
        return False
    return bool(_LOGICAL_PATH_RE.fullmatch(normalized))


__all__ = [
    "VAULT_LOGICAL_PATH_MAX_LENGTH",
    "VAULT_LOGICAL_PATH_PATTERN",
    "VAULT_MOUNT_MAX_LENGTH",
    "VAULT_MOUNT_PATTERN",
    "is_valid_vault_logical_path",
    "is_valid_vault_mount",
]
