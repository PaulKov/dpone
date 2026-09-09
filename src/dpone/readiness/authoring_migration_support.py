"""Security-boundary helpers for manifest authoring migration reports."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.airflow_secret_redaction import redacted_mapping
from dpone.readiness.error_contract import dpone_error, error_docs_url


def redacted_authoring_mapping(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one recursively redacted authoring mapping for safe diffs."""

    return redacted_mapping(dict(payload))


def authoring_migration_error(code: str, message: str, *, path: str = "") -> dict[str, Any]:
    """Build one stable, documented, secret-free migration error."""

    return dpone_error(
        code,
        message,
        stage="authoring_migration",
        entity={"kind": "pipeline", "id": path or "unknown"},
        path=path or None,
        docs_url=error_docs_url(code),
    )


__all__ = ["authoring_migration_error", "redacted_authoring_mapping"]
