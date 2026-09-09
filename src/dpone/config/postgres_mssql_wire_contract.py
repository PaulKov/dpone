"""Typed public-to-runtime wire policy for PostgreSQL→SQL Server."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from dpone.contracts.postgres_mssql_type_policy import postgres_mssql_file_enforcement_blocker


class PostgresMssqlWireContractError(ValueError):
    """Stable pre-source rejection for an unsafe route transport."""

    code = "DPONE_POSTGRES_MSSQL_WIRE_CONTRACT_BLOCKED"

    def __init__(self, blocker: str, detail: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}: {blocker}: {detail}")


@dataclass(frozen=True, slots=True)
class PostgresMssqlWirePolicy:
    """One executable wire decision shared by check, plan, and runtime."""

    public_export_format: str
    runtime_export_format: str
    compress_export: bool
    batch_commit_mode: str
    snapshot_scope: str = "single_copy_statement"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def normalize_postgres_mssql_wire(load_config: Any) -> PostgresMssqlWirePolicy:
    """Resolve the safe single-COPY policy or reject before source I/O."""

    options_raw = getattr(load_config, "options", {}) or {}
    options = options_raw if isinstance(options_raw, Mapping) else {}
    authored_format = str(getattr(load_config, "export_format", "csv") or "csv").strip().lower()
    compressed = bool(getattr(load_config, "compress_export", False))
    internal = bool(options.get("__dpone_postgres_mssql_internal_wire"))
    if authored_format != "csv" and not (internal and authored_format in {"mssql-delimited", "mssql_delimited"}):
        _blocked("postgres_mssql.export_format", "public authoring requires export_format=csv")
    if compressed:
        _blocked("postgres_mssql.compress_export", "SQL Server bcp route requires compress_export=false")
    if enforcement_blocker := postgres_mssql_file_enforcement_blocker(options):
        _blocked(
            enforcement_blocker,
            "file-backed route currently supports strict enforcement only",
        )

    authored_mode = options.get("batch_commit_mode")
    effective_mode = "whole" if authored_mode in (None, "") else str(authored_mode).strip().lower()
    if effective_mode != "whole":
        _blocked(
            "postgres_mssql.batch_commit_mode",
            "multi-file PostgreSQL export has no shared MVCC snapshot coordinator; use whole",
        )
    partitioning = options.get("partitioning")
    if isinstance(partitioning, Mapping) and partitioning.get("enabled", True) is not False:
        if str(partitioning.get("column") or "").strip():
            _blocked(
                "postgres_mssql.partitioning",
                "partitioned PostgreSQL export has no shared MVCC snapshot coordinator",
            )
    return PostgresMssqlWirePolicy(
        public_export_format=authored_format,
        runtime_export_format="mssql-delimited",
        compress_export=False,
        batch_commit_mode=effective_mode,
    )


def _blocked(blocker: str, detail: str) -> None:
    raise PostgresMssqlWireContractError(blocker, detail)


__all__ = [
    "PostgresMssqlWireContractError",
    "PostgresMssqlWirePolicy",
    "normalize_postgres_mssql_wire",
]
