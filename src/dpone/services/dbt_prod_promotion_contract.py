"""Compatibility imports and filesystem validation for dbt promotion metadata."""

from __future__ import annotations

import stat
from pathlib import Path

from dpone.contracts.dbt_promotion import (
    DbtProdMirrorError,
    DbtPromotionTrustDescriptor,
    dbt_json_object,
    require_dbt_digest,
    safe_dbt_reference,
    validated_relative_path,
)


def validated_repository_root(value: Path) -> Path:
    root = Path(value).absolute()
    try:
        metadata = root.lstat()
    except OSError as exc:
        raise DbtProdMirrorError("repository root does not exist") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise DbtProdMirrorError("repository root must be a real directory")
    if not (root / ".git").exists():
        raise DbtProdMirrorError("repository root must be a Git checkout")
    return root


__all__ = [
    "DbtProdMirrorError",
    "DbtPromotionTrustDescriptor",
    "dbt_json_object",
    "require_dbt_digest",
    "safe_dbt_reference",
    "validated_relative_path",
    "validated_repository_root",
]
