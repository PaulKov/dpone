from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

_BYTES_IN_MIB = 1024 * 1024
_PHASE_NAMES = (
    "postgres_to_mssql.source_export",
    "postgres_to_mssql.target_load_finalize",
    "mssql_to_clickhouse.source_export",
    "mssql_to_clickhouse.target_load_finalize",
    "mssql_count_reconciliation",
    "clickhouse_count_reconciliation",
)


def valid_stress_benchmark(
    *,
    row_count: int,
    partition_count: int,
    metric_names: tuple[str, ...],
    partitioning: Mapping[str, Any],
    artifact_type: str,
) -> dict[str, Any]:
    """Build the exact native stress serializer shape used by gate tests."""

    metrics = [_metric(name, rows=row_count) for name in metric_names]
    phase_metrics = [_metric(name, rows=row_count) for name in _PHASE_NAMES]
    return {
        "rows": row_count,
        "partitioning": {**partitioning, "upper_bound": row_count},
        "metrics": metrics,
        "phase_metrics": phase_metrics,
        "transfers": {
            "postgres_to_mssql_full_refresh": {
                "metric": copy.deepcopy(metrics[1]),
                "phases": copy.deepcopy(phase_metrics[:2]),
                "artifact": _artifact(
                    rows=row_count,
                    partition_count=partition_count,
                    estimated=False,
                    prefix="postgres-mssql",
                    artifact_type=artifact_type,
                ),
            },
            "mssql_to_clickhouse_full_refresh": {
                "metric": copy.deepcopy(metrics[2]),
                "phases": copy.deepcopy(phase_metrics[2:4]),
                "artifact": _artifact(
                    rows=row_count,
                    partition_count=partition_count,
                    estimated=True,
                    prefix="mssql-clickhouse",
                    artifact_type=artifact_type,
                ),
            },
        },
        "mssql_count": row_count,
        "clickhouse_count": row_count,
    }


def _metric(name: str, *, rows: int) -> dict[str, Any]:
    return {
        "name": name,
        "rows": rows,
        "seconds": 1.0,
        "rows_per_second": float(rows),
    }


def _artifact(*, rows: int, partition_count: int, estimated: bool, prefix: str, artifact_type: str) -> dict[str, Any]:
    partition_rows = rows // partition_count
    partitions = [
        {
            "index": index,
            "artifact_type": "FileExportArtifact",
            "file_path": f"/tmp/{prefix}/part-{index}.tsv",
            "exists": True,
            "bytes": 1024,
            "mb": round(1024 / _BYTES_IN_MIB, 3),
            "estimated_rows": partition_rows if estimated else None,
        }
        for index in range(partition_count)
    ]
    total_bytes = sum(int(item["bytes"]) for item in partitions)
    return {
        "artifact_type": artifact_type,
        "partition_count": partition_count,
        "estimated_rows": rows if estimated else None,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / _BYTES_IN_MIB, 3),
        "mb_per_second": round(total_bytes / _BYTES_IN_MIB, 6),
        "partition_row_skew": 0,
        "slowest_partition": copy.deepcopy(partitions[0]),
        "partitions": partitions,
    }


__all__ = ["valid_stress_benchmark"]
