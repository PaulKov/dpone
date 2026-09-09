"""Pure PostgreSQL→MSSQL manifest type-contract policy.

The manifest validator, dry-run planner, and runtime projection must make the
same explicit-contract decision.  This module deliberately has no runtime
dependencies so those three consumers cannot drift or invert the dependency
direction.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper


class PostgresMssqlContractOptions:
    """Read the canonical logical and physical per-column declarations."""

    def __init__(self, options: Mapping[str, Any]) -> None:
        self._options = options

    def explicit_source(self, column: str) -> str | None:
        """Return the approved source representation, if semantically valid.

        Complex PostgreSQL values currently use PostgreSQL's textual COPY
        representation.  A physical override alone, or an unrelated logical
        type, therefore cannot authorize the transfer.
        """

        logical = self._logical_column(column)
        if logical is None:
            return None
        declared = str(logical.get("type", logical.get("logical_type", "")) or "").strip().lower()
        return "schema_contract:string" if declared == "string" else None

    def physical_override(self, column: str) -> str | None:
        physical = self._options.get("physical_design")
        columns = physical.get("columns") if isinstance(physical, Mapping) else None
        raw = columns.get(column) if isinstance(columns, Mapping) else None
        target = raw.get("target_type") if isinstance(raw, Mapping) else None
        value = target.get("mssql") if isinstance(target, Mapping) else None
        return str(value).strip() if value not in (None, "") else None

    def nullable(self, column: str) -> bool | None:
        logical = self._logical_column(column)
        value = logical.get("nullable") if logical is not None else None
        return value if isinstance(value, bool) else None

    def collation(self, column: str) -> str | None:
        physical = self._options.get("physical_design")
        columns = physical.get("columns") if isinstance(physical, Mapping) else None
        raw = columns.get(column) if isinstance(columns, Mapping) else None
        collations = raw.get("collation") if isinstance(raw, Mapping) else None
        value = collations.get("mssql") if isinstance(collations, Mapping) else None
        return str(value).strip() if value not in (None, "") else None

    def file_enforcement_blocker(self) -> str | None:
        """Reject row-policy modes not implemented by the opaque file route."""

        contract = self._options.get("schema_contract")
        if not isinstance(contract, Mapping):
            return None
        enforcement = str(contract.get("enforcement", "strict")).strip().lower()
        if enforcement == "strict":
            return None
        return f"postgres_mssql.type_contract.file_enforcement_unsupported:{enforcement}"

    def _logical_column(self, column: str) -> Mapping[str, Any] | None:
        contract = self._options.get("schema_contract")
        columns = contract.get("columns") if isinstance(contract, Mapping) else None
        raw = columns.get(column) if isinstance(columns, Mapping) else None
        return raw if isinstance(raw, Mapping) else None


def declared_postgres_mssql_contract_blockers(
    relation_schema: Sequence[tuple[str, str]],
    options: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Return stable blockers for a manifest-declared PostgreSQL schema."""

    relation = tuple((str(name), str(dtype)) for name, dtype in relation_schema)
    names = tuple(name for name, _dtype in relation)
    identities = tuple(name.casefold() for name in names)
    if not relation or any(not name or not dtype for name, dtype in relation) or len(set(identities)) != len(names):
        return ("postgres_mssql.type_contract.source_provenance_invalid",)

    config = PostgresMssqlContractOptions(options or {})
    blockers: list[str] = []
    if enforcement_blocker := config.file_enforcement_blocker():
        blockers.append(enforcement_blocker)
    mapper = PostgresMssqlTypeMapper()
    for name, source_type in relation:
        if name.casefold().startswith("__dpone__"):
            blockers.append(f"postgres_mssql.type_contract.source_reserved_column:{name}")
            continue
        decision = mapper.resolve(source_type)
        if not decision.requires_explicit_contract:
            continue
        if config.explicit_source(name) is None:
            blockers.append(f"postgres_mssql.type_contract.explicit_contract_required:{name}")
            continue
        override = config.physical_override(name)
        if override is not None and not is_textual_mssql_target(override):
            blockers.append(f"postgres_mssql.type_contract.textual_target_required:{name}")
    return tuple(blockers)


def postgres_mssql_file_enforcement_blocker(
    options: Mapping[str, Any] | None,
) -> str | None:
    """Return the canonical opaque-file enforcement blocker without schema IO."""

    return PostgresMssqlContractOptions(options or {}).file_enforcement_blocker()


def is_textual_mssql_target(dtype: str) -> bool:
    """Return whether ``dtype`` is an approved exact textual landing family."""

    try:
        normalized = normalize_mssql_physical_type(dtype)
    except ValueError:
        return False
    return normalized.split("(", 1)[0] in {"char", "nchar", "nvarchar", "varchar"}


__all__ = [
    "declared_postgres_mssql_contract_blockers",
    "is_textual_mssql_target",
    "PostgresMssqlContractOptions",
    "postgres_mssql_file_enforcement_blocker",
]
