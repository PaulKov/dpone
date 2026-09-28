"""MSSQL BCP native artifact factory helpers."""

from __future__ import annotations

import tempfile
from collections.abc import Sequence
from typing import Any

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import type_policy as _type_policy
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


def build_single_file_bcp_artifact(
    factory: Any,
    load_config: Any,
    *,
    query: str,
    artifact_schema: list[tuple[str, str]],
    artifact_format: str,
    artifact_text_codec: Any | None,
    bulk_wire_contract: Any | None,
    bcp_options: Any,
    source_table: str,
    bcp_native_wire: bool,
    tmp_dir: Any,
) -> Any:
    file_path = tempfile.NamedTemporaryFile(
        prefix="dpone_mssql_queryout_",
        suffix=".bcp",
        dir=tmp_dir,
        delete=False,
    ).name
    rows = factory.connector.bcp_queryout(query, file_path, options=bcp_options)
    factory.logger.log_etl_progress("MSSQL_BCP_QUERYOUT", {"Rows": rows, "File": file_path, "Source": source_table})
    if bcp_native_wire:
        artifact = build_mssql_bcp_native_artifact(
            file_path,
            artifact_schema,
            query=query,
            bcp_version=bcp_options.bcp_path,
            type_policy=_type_policy(load_config),
            estimated_rows=rows or None,
            bulk_wire_contract=bulk_wire_contract,
        )
    else:
        artifact = FileExportArtifact(
            file_path=file_path,
            columns=[column for column, _ in artifact_schema],
            compressed=False,
            format=artifact_format,
            estimated_rows=rows or None,
            rows_exported=rows if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 else None,
            bulk_text_codec=artifact_text_codec,
        )
        if bulk_wire_contract is not None:
            setattr(artifact, "bulk_wire_contract", bulk_wire_contract)
    if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 and artifact.rows_exported is None:
        artifact.rows_exported = rows
    return artifact


__all__ = ["build_mssql_bcp_native_artifact", "build_single_file_bcp_artifact"]
