"""Database lease and internal-column options for MSSQL staging."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sinks.staging_managers.mssql_staging_evidence import schema_label

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_DIRECT_NATIVE_STAGING_AUTHORITY = object()


class MssqlStagingDatabaseAuthorityMixin:
    """Hold the signed staging-database lease across each physical action."""

    connector: Any
    _database_authority: Any | None
    _authority_leases: dict[int, Any]

    @contextmanager
    def database_authority_scope(self, artifact: StagingTableArtifact | None = None):
        existing = self._authority_leases.get(id(artifact)) if artifact is not None else None
        lease = existing or self._acquire_database_authority_lease()
        owns_lease = lease is not None and lease is not existing
        try:
            self._assert_database_authority_lease(lease)
            yield
            self._assert_database_authority_lease(lease)
        finally:
            if owns_lease:
                self._close_database_authority_lease(lease)

    def _acquire_database_authority_lease(self) -> Any | None:
        if self._database_authority is None:
            return None
        acquire = getattr(self._database_authority, "acquire_staging_database_authority_lease", None)
        return acquire(self.connector) if callable(acquire) else None

    def release_authority_lease(self, artifact: StagingTableArtifact) -> None:
        """Close an artifact authority session while retaining its table."""

        lease = self._authority_leases.pop(id(artifact), None)
        self._close_database_authority_lease(lease)

    @staticmethod
    def _assert_database_authority_lease(lease: Any | None) -> None:
        if lease is not None:
            lease.assert_current()

    @staticmethod
    def _close_database_authority_lease(lease: Any | None) -> None:
        if lease is not None:
            lease.close()

    def _qualified_name(self, schema: str, table: str, *, database: str | None = None) -> str:
        try:
            return self.connector.qualified_name(schema, table, database=database)
        except TypeError:
            return self.connector.qualified_name(schema_label(schema, database), table)


def internal_not_null_columns(load_config: LoadConfig) -> frozenset[str]:
    options = getattr(load_config, "options", {}) or {}
    values = options.get("__dpone_mssql_native_not_null_columns")
    if not isinstance(values, (list, tuple, set, frozenset)):
        values = options.get("__dpone_snapshot_native_not_null_columns")
    if not isinstance(values, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(str(value) for value in values)


def internal_column_collations(load_config: LoadConfig) -> dict[str, str]:
    options = getattr(load_config, "options", {}) or {}
    values = options.get("__dpone_mssql_native_collations")
    if not isinstance(values, Mapping):
        values = options.get("__dpone_snapshot_native_collations")
    if not isinstance(values, Mapping):
        return {}
    return {str(column): str(collation) for column, collation in values.items()}


def internal_typed_file_ingestion(load_config: LoadConfig) -> bool:
    """Return the closed runtime-issued typed snapshot transport decision."""

    options = getattr(load_config, "options", {}) or {}
    return options.get("__dpone_mssql_typed_file_staging_v1") is True


def internal_direct_native_staging(load_config: LoadConfig) -> bool:
    """Return the runtime-issued business-prefix native staging decision."""

    options = getattr(load_config, "options", {}) or {}
    return options.get("__dpone_mssql_direct_native_staging_v1") is _DIRECT_NATIVE_STAGING_AUTHORITY


def issue_direct_native_staging_authority(options: dict[str, Any]) -> None:
    """Bind an in-memory decision that manifest data cannot construct."""

    options["__dpone_mssql_direct_native_staging_v1"] = _DIRECT_NATIVE_STAGING_AUTHORITY


def internal_omitted_native_columns(load_config: LoadConfig) -> frozenset[str]:
    """Return native suffix columns intentionally absent from the BCP wire."""

    if not internal_direct_native_staging(load_config):
        return frozenset()
    options = getattr(load_config, "options", {}) or {}
    values = options.get("__dpone_mssql_native_omitted_columns")
    if not isinstance(values, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(str(value) for value in values)


def internal_physical_not_null_columns(
    load_config: LoadConfig,
    column_types: Mapping[str, str],
) -> frozenset[str]:
    """Keep omitted framework fields nullable until SQL projection completes."""

    intended = internal_not_null_columns(load_config)
    omitted = internal_omitted_native_columns(load_config)
    if not omitted.issubset(column_types):
        raise ValueError("mssql_native_staging.omitted_column_missing")
    return intended - omitted


def internal_wire_schema(load_config: LoadConfig) -> tuple[tuple[str, str], ...] | None:
    """Return the immutable source wire schema for a widened native table."""

    if not internal_direct_native_staging(load_config):
        return None
    options = getattr(load_config, "options", {}) or {}
    values = options.get("__dpone_mssql_wire_schema")
    if not isinstance(values, (list, tuple)):
        return None
    output: list[tuple[str, str]] = []
    for value in values:
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise ValueError("mssql_native_staging.wire_schema_invalid")
        output.append((str(value[0]), str(value[1])))
    return tuple(output)
