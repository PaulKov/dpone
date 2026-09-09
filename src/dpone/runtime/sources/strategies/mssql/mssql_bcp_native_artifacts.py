"""MSSQL BCP native artifact factory helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


def build_mssql_bcp_native_artifact(
    file_path: str,
    schema: Sequence[tuple[str, str]],
    *,
    query: str,
    bcp_version: str,
    type_policy: MssqlClickHouseTypePolicy,
    estimated_rows: int | None,
    bulk_wire_contract: Any,
) -> FileExportArtifact:
    """Build a source-native BCP artifact with its decode contract."""

    return SourceNativeArtifact(
        file_path,
        columns=[column for column, _ in schema],
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=schema,
            query=query,
            bcp_version=bcp_version,
            type_policy=type_policy,
            target_format=str(getattr(bulk_wire_contract, "input_format", "RowBinary")),
        ),
        estimated_rows=estimated_rows,
        bulk_wire_contract=bulk_wire_contract,
    )


__all__ = ["build_mssql_bcp_native_artifact"]
