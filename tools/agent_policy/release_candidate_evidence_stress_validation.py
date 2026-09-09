"""Pure validator for the frozen release-candidate stress workload."""

from __future__ import annotations

import importlib.util
import math
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_stress", "release_candidate_evidence_codec.py")
policy = _load_sibling("dpone_release_candidate_policy_stress", "release_candidate_evidence_policy.py")
STRESS_KEYS = frozenset(
    {"rows", "partitioning", "metrics", "phase_metrics", "transfers", "mssql_count", "clickhouse_count"}
)
METRIC_KEYS = frozenset({"name", "rows", "seconds", "rows_per_second"})
PHASE_NAMES = (
    "postgres_to_mssql.source_export",
    "postgres_to_mssql.target_load_finalize",
    "mssql_to_clickhouse.source_export",
    "mssql_to_clickhouse.target_load_finalize",
    "mssql_count_reconciliation",
    "clickhouse_count_reconciliation",
)
ARTIFACT_KEYS = frozenset(
    {
        "artifact_type",
        "partition_count",
        "estimated_rows",
        "total_bytes",
        "total_mb",
        "mb_per_second",
        "partition_row_skew",
        "slowest_partition",
        "partitions",
    }
)
PARTITION_KEYS = frozenset({"index", "artifact_type", "file_path", "exists", "bytes", "mb", "estimated_rows"})
_BYTES_IN_MIB = 1024 * 1024


def validate_stress(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Require reconciled rows, frozen SLO metrics, and complete artifacts."""

    codec.require_exact_keys(payload, STRESS_KEYS, field="stress_benchmark")
    rows = codec.require_positive_int(payload.get("rows"), field="stress_benchmark.rows")
    if rows != policy.ROW_COUNT or payload.get("mssql_count") != rows or payload.get("clickhouse_count") != rows:
        raise ValueError("stress benchmark row reconciliation does not match frozen workload")
    expected_partitioning = dict(policy.STRESS_PARTITIONING)
    expected_partitioning["upper_bound"] = rows
    if _mapping(payload.get("partitioning"), "stress_benchmark.partitioning") != expected_partitioning:
        raise ValueError("stress benchmark partitioning does not match frozen policy")
    metrics = _list(payload.get("metrics"), "stress_benchmark.metrics")
    by_name = {
        codec.require_string(_mapping(item, "stress_benchmark.metric").get("name"), field="metric.name"): _mapping(
            item, "stress_benchmark.metric"
        )
        for item in metrics
    }
    if frozenset(by_name) != frozenset(policy.STRESS_METRICS) or len(metrics) != len(policy.STRESS_METRICS):
        raise ValueError("stress benchmark metric inventory is invalid")
    observed = _validate_metrics(by_name, rows=rows)
    phases = _validate_phase_metrics(payload.get("phase_metrics"), rows=rows)
    _validate_transfers(payload.get("transfers"), rows=rows, metrics=by_name, phases=phases)
    return {"status": "PASS", "rows": rows, "failure_rate": 0.0, "rows_per_second": observed}


def _validate_metrics(metrics: Mapping[str, Mapping[str, Any]], *, rows: int) -> dict[str, float]:
    observed: dict[str, float] = {}
    for name, metric in metrics.items():
        seconds, rows_per_second = _validate_metric(metric, expected_name=name, rows=rows)
        del seconds
        observed[name] = rows_per_second
    return dict(sorted(observed.items()))


def _validate_phase_metrics(value: Any, *, rows: int) -> tuple[Mapping[str, Any], ...]:
    phases = _list(value, "stress_benchmark.phase_metrics")
    names = [
        codec.require_string(_mapping(item, "stress_benchmark.phase_metric").get("name"), field="phase.name")
        for item in phases
    ]
    if tuple(names) != PHASE_NAMES:
        raise ValueError("stress benchmark phase metric inventory or order is invalid")
    validated: list[Mapping[str, Any]] = []
    for name, item in zip(PHASE_NAMES, phases, strict=True):
        metric = _mapping(item, f"stress_benchmark.phase_metrics.{name}")
        _validate_metric(metric, expected_name=name, rows=rows)
        validated.append(metric)
    return tuple(validated)


def _validate_metric(metric: Mapping[str, Any], *, expected_name: str, rows: int) -> tuple[float, float]:
    field = f"stress_benchmark.metrics.{expected_name}"
    codec.require_exact_keys(metric, METRIC_KEYS, field=field)
    if metric.get("name") != expected_name or metric.get("rows") != rows:
        raise ValueError(f"stress benchmark metric {expected_name} identity or row count is invalid")
    seconds = codec.require_positive_number(metric.get("seconds"), field=f"{field}.seconds")
    rows_per_second = codec.require_positive_number(metric.get("rows_per_second"), field=f"{field}.rows_per_second")
    if not _rounded_metric_is_consistent(rows=rows, seconds=seconds, rows_per_second=rows_per_second):
        raise ValueError(f"stress benchmark metric {expected_name} rows_per_second is inconsistent with rows/seconds")
    if rows_per_second < policy.MINIMUM_ROWS_PER_SECOND:
        raise ValueError(f"stress benchmark metric {expected_name} is below the frozen performance floor")
    return seconds, rows_per_second


def _rounded_metric_is_consistent(*, rows: int, seconds: float, rows_per_second: float) -> bool:
    """Accept only elapsed/rate values producible by the frozen 3/2-digit rounding."""

    seconds_low = max(0.0, seconds - 0.0005)
    seconds_high = math.nextafter(seconds + 0.0005, math.inf)
    rate_low = max(0.0, rows_per_second - 0.005)
    rate_high = math.nextafter(rows_per_second + 0.005, math.inf)
    duration_low = math.nextafter(rows / rate_high, -math.inf)
    duration_high = math.inf if rate_low == 0.0 else math.nextafter(rows / rate_low, math.inf)
    return max(seconds_low, duration_low) <= min(seconds_high, duration_high)


def _validate_transfers(
    value: Any,
    *,
    rows: int,
    metrics: Mapping[str, Mapping[str, Any]],
    phases: tuple[Mapping[str, Any], ...],
) -> None:
    transfers = _mapping(value, "stress_benchmark.transfers")
    expected = {"postgres_to_mssql_full_refresh", "mssql_to_clickhouse_full_refresh"}
    if frozenset(transfers) != expected:
        raise ValueError("stress benchmark transfer inventory is invalid")
    expected_phases = {
        "postgres_to_mssql_full_refresh": phases[:2],
        "mssql_to_clickhouse_full_refresh": phases[2:4],
    }
    for name, current in transfers.items():
        transfer = _mapping(current, f"stress_benchmark.transfers.{name}")
        codec.require_exact_keys(
            transfer, frozenset({"metric", "phases", "artifact"}), field=f"stress_benchmark.transfers.{name}"
        )
        metric = _mapping(transfer.get("metric"), f"stress_benchmark.transfers.{name}.metric")
        if metric.get("name") != name or metric.get("rows") != rows:
            raise ValueError(f"stress benchmark transfer {name} is not bound to the frozen workload")
        if metric != metrics[name]:
            raise ValueError(f"stress benchmark transfer {name} metric does not match its top-level metric")
        transfer_phases = _list(transfer.get("phases"), f"stress_benchmark.transfers.{name}.phases")
        if transfer_phases != list(expected_phases[name]):
            raise ValueError(f"stress benchmark transfer {name} phases do not match top-level phase metrics")
        _validate_artifact(
            transfer.get("artifact"),
            name=name,
            rows=rows,
            source_phase_seconds=float(expected_phases[name][0]["seconds"]),
        )


def _validate_artifact(value: Any, *, name: str, rows: int, source_phase_seconds: float) -> None:
    field = f"stress_benchmark.transfers.{name}.artifact"
    artifact = _mapping(value, field)
    codec.require_exact_keys(artifact, ARTIFACT_KEYS, field=field)
    if artifact.get("artifact_type") != policy.STRESS_ARTIFACT_TYPE:
        raise ValueError(f"stress benchmark transfer {name} artifact type is invalid")
    partitions = _list(artifact.get("partitions"), f"{field}.partitions")
    partition_count = codec.require_positive_int(artifact.get("partition_count"), field=f"{field}.partition_count")
    total_bytes = codec.require_positive_int(artifact.get("total_bytes"), field=f"{field}.total_bytes")
    if partition_count != policy.STRESS_PARTITION_COUNT or len(partitions) != partition_count:
        raise ValueError(f"stress benchmark transfer {name} artifact is incomplete")
    indices: list[int] = []
    observed_bytes = 0
    observed_estimates: list[int] = []
    paths: list[str] = []
    for position, item in enumerate(partitions):
        partition = _mapping(item, f"{field}.partitions[{position}]")
        codec.require_exact_keys(partition, PARTITION_KEYS, field=f"{field}.partitions[{position}]")
        indices.append(
            codec.require_nonnegative_int(partition.get("index"), field=f"{field}.partitions[{position}].index")
        )
        current_bytes = codec.require_positive_int(
            partition.get("bytes"), field=f"{field}.partitions[{position}].bytes"
        )
        observed_bytes += current_bytes
        path = codec.require_string(partition.get("file_path"), field=f"{field}.partitions[{position}].file_path")
        paths.append(path)
        if (
            partition.get("artifact_type") != "FileExportArtifact"
            or partition.get("exists") is not True
            or partition.get("mb") != round(current_bytes / _BYTES_IN_MIB, 3)
        ):
            raise ValueError(f"stress benchmark transfer {name} artifact is incomplete")
        estimate = partition.get("estimated_rows")
        if estimate is not None:
            observed_estimates.append(
                codec.require_positive_int(estimate, field=f"{field}.partitions[{position}].estimated_rows")
            )
    if (
        sorted(indices) != list(range(partition_count))
        or len(set(paths)) != len(paths)
        or observed_bytes != total_bytes
        or artifact.get("total_mb") != round(total_bytes / _BYTES_IN_MIB, 3)
        or artifact.get("mb_per_second") != round((total_bytes / _BYTES_IN_MIB) / source_phase_seconds, 6)
    ):
        raise ValueError(f"stress benchmark transfer {name} artifact is incomplete")
    if name == "postgres_to_mssql_full_refresh":
        if artifact.get("estimated_rows") is not None or observed_estimates:
            raise ValueError("Postgres to MSSQL artifact row estimates are not the frozen producer shape")
        expected_skew = 0
    else:
        if artifact.get("estimated_rows") != rows or len(observed_estimates) != partition_count:
            raise ValueError("MSSQL to ClickHouse artifact row estimates are incomplete")
        if sum(observed_estimates) != rows:
            raise ValueError("MSSQL to ClickHouse artifact row estimates do not reconcile")
        expected_skew = max(observed_estimates) - min(observed_estimates)
    slowest = max(partitions, key=lambda item: item.get("estimated_rows") or 0)
    if artifact.get("partition_row_skew") != expected_skew or artifact.get("slowest_partition") != slowest:
        raise ValueError(f"stress benchmark transfer {name} partition diagnostics are inconsistent")


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    return value


__all__ = ["validate_stress"]
