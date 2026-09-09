"""Physical projection helpers for the immutable snapshot-envelope route."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.support.mssql_snapshot_projection import (
    business_columns,
    business_schema,
    explicit_mssql_column_types,
    require_indexable_key_types,
    resolved_business_nullability,
    resolved_source_type,
    resolved_target_type,
)


def load_id(load_config: Any) -> str:
    """Read the immutable load identity injected by runtime orchestration."""

    options = getattr(load_config, "options", {}) or {}
    identity = options.get("__dpone_load_identity")
    value = options.get("load_id") or (identity.get("load_id") if isinstance(identity, Mapping) else None)
    if not value:
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.load_id_missing")
    return str(value)


def load_identity(load_config: Any) -> tuple[str, str, datetime]:
    """Return run/load IDs and immutable source-extraction time."""

    options = getattr(load_config, "options", {}) or {}
    identity = options.get("__dpone_load_identity")
    values = identity if isinstance(identity, Mapping) else {}
    run_id = options.get("run_id") or values.get("run_id")
    current_load_id = options.get("load_id") or values.get("load_id")
    extracted_at = values.get("extracted_at")
    if not run_id or not current_load_id or not extracted_at:
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.load_identity_missing")
    try:
        parsed = extracted_at if isinstance(extracted_at, datetime) else datetime.fromisoformat(str(extracted_at))
    except ValueError as exc:
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.extracted_at_invalid") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)  # noqa: UP017
    return str(run_id), str(current_load_id), parsed


def changed_lineage_assignments(quote_identifier: Any) -> tuple[str, ...]:
    """Render lineage assignments that change only with business mutations."""

    return tuple(
        f"{quote_identifier(column)} = ?" for column in ("__dpone__run_id", "__dpone__load_id", "__dpone__extracted_at")
    )


def staging_scalar_expression(strategy: Any, staging: StagingTableArtifact, column: str, alias: str) -> str:
    """Render a decode expression that is valid outside a SELECT list.

    The shared character-wire renderer intentionally adds an output alias for
    SELECT projection.  UPDATE assignments, predicates, and nested hash
    expressions require the scalar form.
    """

    rendered = strategy._staging_select_expression(staging, column, alias)
    quoted = strategy.connector.quote_identifier(column)
    suffix = re.compile(rf"\s+AS\s+{re.escape(quoted)}\s*$", re.IGNORECASE)
    return suffix.sub("", rendered)


__all__ = [
    "business_columns",
    "business_schema",
    "changed_lineage_assignments",
    "explicit_mssql_column_types",
    "load_id",
    "load_identity",
    "require_indexable_key_types",
    "resolved_business_nullability",
    "resolved_source_type",
    "resolved_target_type",
    "staging_scalar_expression",
]
