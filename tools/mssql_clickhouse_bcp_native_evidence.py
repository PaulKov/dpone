"""Observed accelerator and exact receipt helpers for the BCP-native live tool."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.decision_audit import RuntimeDecision
from local_route_certification_receipt import (
    LocalRouteExpectedSubject,
    LocalRouteIdentity,
    LocalSourceSnapshot,
    capture_local_source_snapshot,
    verify_local_route_certification_receipt,
    write_local_route_certification_receipt,
)
from mssql_clickhouse_target_schema import ClickHouseTargetSchemaMetrics
from mssql_dbt_wide_evidence import clickhouse_connection_sha256, mssql_connection_sha256


@dataclass(slots=True)
class BcpDecisionCollector:
    decisions: list[RuntimeDecision]

    def publish(self, decision: RuntimeDecision) -> None:
        self.decisions.append(decision)


def assert_acceleration_decisions(
    config: Any,
    decisions: list[RuntimeDecision],
) -> None:
    """Require observed backend selection to match the certification request."""

    acceleration = [item for item in decisions if item.decision_id == "native_transfer.acceleration"]
    if config.binary_format != "native":
        if config.acceleration_mode == "required":
            raise RuntimeError("native_acceleration_requires_clickhouse_native_format")
        if acceleration:
            raise RuntimeError("native_acceleration_runtime_decision_unexpected")
        return
    if not acceleration:
        raise RuntimeError("native_acceleration_runtime_decision_missing")
    expected = {
        "off": "python_reference",
        "required": "native_accelerated",
    }.get(config.acceleration_mode)
    for decision in acceleration:
        if decision.requested != config.acceleration_mode:
            raise RuntimeError("native_acceleration_runtime_requested_mode_mismatch")
        if expected is not None and decision.selected != expected:
            raise RuntimeError("native_acceleration_runtime_backend_mismatch")
        if decision.blockers:
            raise RuntimeError("native_acceleration_runtime_decision_blocked")
        if config.acceleration_mode == "off":
            if (
                decision.release_gate not in {"green", "warning"}
                or decision.fallback_reason != "native_acceleration_disabled"
                or "native_acceleration_disabled" not in decision.warnings
            ):
                raise RuntimeError("native_acceleration_runtime_off_evidence_invalid")
        elif decision.release_gate != "green":
            raise RuntimeError("native_acceleration_runtime_decision_blocked")


def write_bcp_route_receipt(
    *,
    config: Any,
    result: Any,
    result_path: Path,
    source_snapshot: LocalSourceSnapshot,
    decisions: list[RuntimeDecision],
    loaded_slices: tuple[dict[str, int], ...],
    target_schema: ClickHouseTargetSchemaMetrics | None,
) -> None:
    """Bind a BCP result to exact route and observed runtime decisions."""

    written = write_local_route_certification_receipt(
        output_dir=config.output_dir,
        source_snapshot=source_snapshot,
        final_source_snapshot=capture_local_source_snapshot(),
        result_path=result_path,
        release_id=config.release_id,
        route=LocalRouteIdentity(source="mssql", sink="clickhouse", strategy="full_refresh"),
        source_relation=f"{config.source_schema}.{config.source_table}",
        target_relation=f"{config.target_database}.{config.target_table}",
        transport="typed_binary_bcp_native",
        transport_details={
            "hashed_rows": config.typed_hash_rows,
            "target_rows_per_partition": config.target_rows_per_partition,
            "export_workers": config.export_workers,
            "load_workers": config.load_workers,
            "bcp_file_format": "native",
            "loaded_slices": loaded_slices,
        },
        binary_format=config.binary_format,
        requested_acceleration_mode=config.acceleration_mode,
        runtime_decisions=tuple(item.to_jsonable() for item in decisions),
        mssql_connection_sha256=mssql_connection_sha256(config.mssql_params),
        clickhouse_connection_sha256=clickhouse_connection_sha256(config.clickhouse_params),
        target_schema_sha256=target_schema.observed_sha256 if target_schema is not None else None,
        upstream_evidence_path=config.upstream_evidence,
        cleanup_verified=not _remaining_partition_files(config.output_dir),
        remaining_artifacts=_remaining_partition_files(config.output_dir),
        passed=result.passed,
    )
    payload = json.loads(written.path.read_text(encoding="utf-8"))
    if payload.get("evidence_status") == "PASS":
        if target_schema is None:
            raise ValueError("local_route_certification_target_schema_missing")
        verify_local_route_certification_receipt(
            written.path,
            source_snapshot=source_snapshot,
            expected=LocalRouteExpectedSubject(
                release_id=config.release_id,
                route=LocalRouteIdentity(source="mssql", sink="clickhouse", strategy="full_refresh"),
                source_relation=f"{config.source_schema}.{config.source_table}",
                target_relation=f"{config.target_database}.{config.target_table}",
                transport="typed_binary_bcp_native",
                binary_format=config.binary_format,
                requested_acceleration_mode=config.acceleration_mode,
                expected_rows=config.rows,
                expected_column_count=config.column_count,
                mssql_connection_sha256=mssql_connection_sha256(config.mssql_params),
                clickhouse_connection_sha256=clickhouse_connection_sha256(config.clickhouse_params),
                target_schema_sha256=target_schema.expected_sha256,
            ),
        )


def loaded_slice_closure(artifact: Any, *, loaded_rows: int | None = None) -> tuple[dict[str, int], ...]:
    """Project the exact completed runtime slices into closed receipt evidence."""

    evidence = getattr(artifact, "slice_evidence", None)
    if not isinstance(evidence, list):
        evidence = []
    closure: list[dict[str, int]] = []
    for item in evidence:
        if not isinstance(item, dict):
            raise ValueError("local_route_certification_loaded_slice_invalid")
        closure.append(
            {
                "partition_index": int(item["partition_index"]),
                "slice_index": int(item["slice_index"]),
                "rows_exported": int(item["rows_exported"]),
                "rows_loaded": int(item["rows_loaded"]),
            }
        )
    if closure:
        return tuple(closure)
    rows_exported = getattr(artifact, "rows_exported", None)
    if (
        isinstance(rows_exported, int)
        and not isinstance(rows_exported, bool)
        and rows_exported > 0
        and isinstance(loaded_rows, int)
        and not isinstance(loaded_rows, bool)
        and loaded_rows > 0
    ):
        return (
            {
                "partition_index": 0,
                "slice_index": 0,
                "rows_exported": rows_exported,
                "rows_loaded": loaded_rows,
            },
        )
    return tuple()


def _remaining_partition_files(output_dir: Path) -> tuple[str, ...]:
    root = output_dir / "partition_files"
    if not root.exists():
        return tuple()
    return tuple(sorted(path.relative_to(output_dir).as_posix() for path in root.rglob("*") if path.is_file()))


__all__ = [
    "BcpDecisionCollector",
    "assert_acceleration_decisions",
    "loaded_slice_closure",
    "write_bcp_route_receipt",
]
