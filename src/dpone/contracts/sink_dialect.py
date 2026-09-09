"""Explicit sink-dialect authority shared by incremental cursor policies.

Runtime safety gates must use hydrated route metadata or an adapter-declared
capability.  Python class and module names are not connector authority: custom
wrappers are common, and a misleading name must neither grant nor revoke a
data-safety capability.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.connector_declarations import canonical_connector_id


class SinkDialectAuthorityConflictError(ValueError):
    """Raised when hydrated route metadata conflicts with connector authority."""

    def __init__(self, *, configured: str, connector: str) -> None:
        super().__init__(
            "incremental_cursor.sink_dialect_authority_conflict: "
            f"configured sink dialect {configured!r} conflicts with connector dialect {connector!r}"
        )


class SinkDialectAuthorityMissingError(ValueError):
    """Raised when a direct safety gate has no trustworthy sink dialect."""

    def __init__(self) -> None:
        super().__init__(
            "incremental_cursor.sink_dialect_authority_required: "
            "provide hydrated sink_type/target_type or an adapter-declared dialect capability"
        )


def is_mssql_dialect(value: Any) -> bool:
    """Return whether a normalized configuration token denotes SQL Server."""

    return _normalize_dialect(value) == "mssql"


def resolve_sink_dialect(*, configured: Any, connector: Any) -> str | None:
    """Resolve one sink dialect from config and an explicit adapter capability.

    Wrappers must forward either ``dialect`` or a mapping-valued
    ``connection_descriptor``.  When both configuration and connector
    authorities exist, disagreement is a fail-closed configuration error.
    """

    configured_dialect = _normalize_dialect(configured)
    connector_dialect = _connector_declared_dialect(connector)
    if configured_dialect and connector_dialect and configured_dialect != connector_dialect:
        raise SinkDialectAuthorityConflictError(
            configured=configured_dialect,
            connector=connector_dialect,
        )
    return configured_dialect or connector_dialect


def require_sink_dialect_authority(*, configured: Any, connector: Any) -> str:
    """Resolve an explicit dialect and reject an authority-free direct call."""

    dialect = resolve_sink_dialect(configured=configured, connector=connector)
    if dialect is None:
        raise SinkDialectAuthorityMissingError
    return dialect


def _connector_declared_dialect(connector: Any) -> str | None:
    if connector is None:
        return None
    dialect = _normalize_dialect(getattr(connector, "dialect", None))
    if dialect:
        return dialect
    descriptor = getattr(connector, "connection_descriptor", None)
    if isinstance(descriptor, Mapping):
        return _normalize_dialect(
            descriptor.get("dialect") or descriptor.get("connection_type") or descriptor.get("type")
        )
    return None


def _normalize_dialect(value: Any) -> str | None:
    normalized = canonical_connector_id(str(value or ""))
    return normalized or None


__all__ = [
    "SinkDialectAuthorityConflictError",
    "SinkDialectAuthorityMissingError",
    "is_mssql_dialect",
    "require_sink_dialect_authority",
    "resolve_sink_dialect",
]
