"""Closed direct-native staging plan for PostgreSQL XMin initial loads.

The initial route exports an immutable business-only PostgreSQL COPY file.
When its wire identities and native SQL Server projection are exact, the file
can be decoded once and loaded directly into the final staging shape.  Routes
that cannot prove this contract return ``None`` and retain the generic
character-wire normalizer.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.postgres_xmin_initial_metadata import (
    is_postgres_xmin_initial_metadata_route,
)
from dpone.runtime.sinks.staging_managers.mssql_staging_support import (
    issue_direct_native_staging_authority,
)
from dpone.runtime.sinks.staging_managers.mssql_typed_values import (
    MssqlTypedValueError,
    require_typed_target_type,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import ResolvedMssqlNativeSchema


@dataclass(frozen=True, slots=True)
class MssqlDirectInitialStagingPlan:
    """One runtime-issued materialization config and exact staging schema."""

    load_config: Any
    staging_schema: tuple[tuple[str, str], ...]


def plan_direct_xmin_initial_staging(
    load_config: Any,
    artifact: Any,
    wire_schema: Sequence[tuple[str, str]],
    resolved: ResolvedMssqlNativeSchema,
) -> MssqlDirectInitialStagingPlan | None:
    """Select direct native staging only when every prerequisite is proven."""

    frozen_wire_schema = tuple((str(name), str(dtype)) for name, dtype in wire_schema)
    wire_names = tuple(name for name, _dtype in frozen_wire_schema)
    if not is_direct_xmin_initial_staging_candidate(
        load_config,
        artifact,
        frozen_wire_schema,
    ):
        return None
    if tuple(resolved.wire_to_target) != wire_names:
        return None
    if any(resolved.wire_to_target[name] != name for name in wire_names):
        return None
    if tuple(resolved.ordered_target_names[: len(wire_names)]) != wire_names:
        return None
    generated = tuple(column.name for column in resolved.generated_columns)
    if not generated or tuple(resolved.ordered_target_names[len(wire_names) :]) != generated:
        return None
    try:
        for column in wire_names:
            require_typed_target_type(resolved.types[column], column=column)
    except MssqlTypedValueError:
        return None

    options = dict(getattr(load_config, "options", {}) or {})
    options.update(
        {
            "__dpone_mssql_typed_file_staging_v1": True,
            "__dpone_mssql_native_staging": True,
            "__dpone_mssql_native_column_types": dict(resolved.types),
            "__dpone_mssql_native_not_null_columns": [
                name for name, nullable in resolved.nullability.items() if not nullable
            ],
            # The BCP host file intentionally omits framework-owned suffix
            # columns.  They stay physically nullable until the normalizer
            # projects their authoritative SQL Server values.
            "__dpone_mssql_native_omitted_columns": list(generated),
            "__dpone_mssql_native_collations": dict(resolved.collations),
            "__dpone_mssql_wire_schema": [[name, dtype] for name, dtype in frozen_wire_schema],
        }
    )
    issue_direct_native_staging_authority(options)
    return MssqlDirectInitialStagingPlan(
        load_config=replace(load_config, options=options),
        staging_schema=resolved.target_schema,
    )


def is_direct_xmin_initial_staging_candidate(
    load_config: Any,
    artifact: Any,
    wire_schema: Sequence[tuple[str, str]],
) -> bool:
    """Return whether lineage-aware direct planning can be attempted safely."""

    wire_names = tuple(str(name) for name, _dtype in wire_schema)
    return (
        is_postgres_xmin_initial_metadata_route(load_config)
        and bool(wire_names)
        and _supports_file_contract(artifact, wire_names)
    )


def _supports_file_contract(artifact: Any, wire_names: tuple[str, ...]) -> bool:
    artifact = _validated_file_contract_artifact(artifact)
    if isinstance(artifact, FileExportArtifact):
        return _supports_file(artifact, wire_names)
    if isinstance(artifact, PartitionedFileExportArtifact):
        return (
            tuple(str(column) for column in artifact.columns) == wire_names
            and bool(artifact.partitions)
            and all(_supports_file(part, wire_names) for part in artifact.partitions)
        )
    return False


def _validated_file_contract_artifact(artifact: Any) -> Any:
    """Unwrap only a wrapper-issued, already validated file capability."""

    current = artifact
    seen: set[int] = set()
    for _depth in range(4):
        identity = id(current)
        if identity in seen:
            raise RuntimeError("mssql_native_projection.file_contract_wrapper_cycle")
        seen.add(identity)
        if isinstance(current, (FileExportArtifact, PartitionedFileExportArtifact)):
            return current
        if not hasattr(type(current), "validated_file_contract_artifact"):
            return current
        current = current.validated_file_contract_artifact
    raise RuntimeError("mssql_native_projection.file_contract_wrapper_depth_exceeded")


def _supports_file(artifact: FileExportArtifact, wire_names: tuple[str, ...]) -> bool:
    codec = artifact.bulk_text_codec
    return (
        not artifact.compressed
        and artifact.format == "mssql-delimited"
        and tuple(str(column) for column in artifact.columns) == wire_names
        and codec is not None
        and callable(getattr(codec, "decode", None))
    )


__all__ = [
    "MssqlDirectInitialStagingPlan",
    "is_direct_xmin_initial_staging_candidate",
    "plan_direct_xmin_initial_staging",
]
