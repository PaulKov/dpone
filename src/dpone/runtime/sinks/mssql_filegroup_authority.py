"""Catalog-bound SQL Server ROWS filegroup placement authority."""

from __future__ import annotations

from dataclasses import replace

from dpone.contracts.mssql_physical_design import MssqlPhysicalDesignContract


def resolve_mssql_physical_filegroups(
    contract: MssqlPhysicalDesignContract,
    *,
    available_row_filegroups: tuple[str, ...],
) -> MssqlPhysicalDesignContract:
    """Resolve authored placement to exact catalog names or fail before DDL.

    ``sys.filegroups`` returns the canonical catalog spelling.  We deliberately
    require that exact spelling: Python identifier folding cannot reproduce an
    arbitrary target database collation and could merge distinct names in a
    case-sensitive catalog.  One resolved contract keeps CREATE DDL, schema
    expectations, and physical expectations on the same authority.
    """

    if not contract.active:
        return contract
    available = frozenset(available_row_filegroups)
    storage = contract.storage
    filegroup = _require_filegroup(storage.filegroup, field="filegroup", available=available)
    textimage = _require_filegroup(
        storage.textimage_filegroup,
        field="textimage_filegroup",
        available=available,
    )
    return replace(
        contract,
        storage=replace(
            storage,
            filegroup=filegroup,
            textimage_filegroup=textimage,
        ),
    )


def _require_filegroup(
    value: str | None,
    *,
    field: str,
    available: frozenset[str],
) -> str | None:
    if value is None:
        return None
    if value not in available:
        raise RuntimeError(f"mssql_transaction.target_row_filegroup_unavailable:{field}:{value}")
    return value


__all__ = ["resolve_mssql_physical_filegroups"]
