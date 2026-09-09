"""Bounded filesystem inventory reader for dbt dev evidence."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.dbt_dev_evidence_campaign import (
    MAX_DBT_DEV_EVIDENCE_FILES_PER_CATEGORY,
)
from dpone.services.dbt_dev_evidence_contracts import read_evidence_object
from dpone.services.dbt_dev_evidence_workflow_verification import (
    EvidenceViolation,
)

_MAX_EVIDENCE_BYTES = 16 * 1024 * 1024


def read_evidence_category(
    root: Path,
    category: str,
) -> tuple[Mapping[str, Any], ...]:
    """Read one confined, non-empty evidence category in stable order."""

    base = root.absolute()
    directory = base / category
    if base.is_symlink() or directory.is_symlink() or not directory.is_dir():
        raise EvidenceViolation(f"{category}_evidence_directory_invalid")
    paths = sorted(directory.iterdir(), key=lambda item: item.name)
    if (
        not paths
        or len(paths) > MAX_DBT_DEV_EVIDENCE_FILES_PER_CATEGORY
        or any(path.is_symlink() or not path.is_file() or path.suffix != ".json" for path in paths)
    ):
        raise EvidenceViolation(f"{category}_evidence_inventory_invalid")
    return tuple(
        read_evidence_object(
            base,
            f"{category}/{path.name}",
            max_bytes=_MAX_EVIDENCE_BYTES,
        )
        for path in paths
    )


__all__ = ["read_evidence_category"]
