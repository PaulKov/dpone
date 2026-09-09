from __future__ import annotations

from dpone.vault_references import (
    VAULT_LOGICAL_PATH_MAX_LENGTH,
    VAULT_LOGICAL_PATH_PATTERN,
    VAULT_MOUNT_MAX_LENGTH,
    VAULT_MOUNT_PATTERN,
)


def vault_mount_schema() -> dict[str, object]:
    return {
        "type": "string",
        "pattern": VAULT_MOUNT_PATTERN,
        "maxLength": VAULT_MOUNT_MAX_LENGTH,
    }


def vault_logical_path_schema() -> dict[str, object]:
    return {
        "type": "string",
        "pattern": VAULT_LOGICAL_PATH_PATTERN,
        "maxLength": VAULT_LOGICAL_PATH_MAX_LENGTH,
        "not": {"pattern": "(^|/)(v1|data)(/|$)"},
    }


__all__ = ["vault_logical_path_schema", "vault_mount_schema"]
