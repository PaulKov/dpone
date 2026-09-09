"""Validate runtime-image SPDX 2.3 evidence without mutating attested bytes."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

SPDX_IDENTITY = {
    "spdxVersion": "SPDX-2.3",
    "SPDXID": "SPDXRef-DOCUMENT",
    "dataLicense": "CC0-1.0",
}


def document_describes(spdx: dict[str, Any]) -> list[str]:
    """Return document subjects from top-level field or DESCRIBES relationships.

    Modern Syft/anchore SPDX 2.3 exports often omit ``documentDescribes`` and
    encode the same link as ``SPDXRef-DOCUMENT DESCRIBES <package>``.
    """
    describes = spdx.get("documentDescribes")
    if isinstance(describes, list):
        subjects = [item for item in describes if isinstance(item, str) and item]
        if subjects:
            return subjects
    relationships = spdx.get("relationships")
    if not isinstance(relationships, list):
        return []
    subjects: list[str] = []
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        if rel.get("relationshipType") != "DESCRIBES":
            continue
        if rel.get("spdxElementId") != "SPDXRef-DOCUMENT":
            continue
        related = rel.get("relatedSpdxElement")
        if isinstance(related, str) and related:
            subjects.append(related)
    return subjects


def load_validated_spdx(
    path: Path,
    *,
    max_bytes: int,
    read_json: Callable[[Path, int, str], Any],
    error_type: type[Exception],
    error_code: str,
) -> dict[str, Any]:
    """Load SPDX JSON and fail closed unless identity/packages/subjects are present."""
    spdx = read_json(path, max_bytes, error_code)
    packages = spdx.get("packages") if isinstance(spdx, dict) else None
    if (
        not isinstance(spdx, dict)
        or any(spdx.get(key) != value for key, value in SPDX_IDENTITY.items())
        or not isinstance(packages, list)
        or not packages
        or not document_describes(spdx)
    ):
        raise error_type(error_code)
    return spdx
