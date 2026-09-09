"""Native transfer benchmark certification services.

This module turns raw benchmark JSON into a stable SLO/evidence contract. It is
pure and connector-free so tools, CI workflows and docs generators can reuse it
without importing runtime adapters.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class NativeBenchmarkCertificationPolicy:
    """SLO policy for a native transfer benchmark artifact."""

    min_rows: int = 1
    min_mssql_to_clickhouse_rows_per_second: float | None = None
    min_postgres_to_mssql_rows_per_second: float | None = None


@dataclass(frozen=True)
class NativeBenchmarkCertificationResult:
    """Machine-readable benchmark certification result."""

    passed: bool
    rows: int
    rows_per_second: dict[str, float]
    bottleneck_phase: str | None
    checks: tuple[dict[str, Any], ...]
    recommendations: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class NativeBenchmarkCertificationService:
    """Evaluate native-transfer benchmark correctness and SLO checks."""

    def certify(
        self,
        artifact: dict[str, Any],
        policy: NativeBenchmarkCertificationPolicy | None = None,
    ) -> NativeBenchmarkCertificationResult:
        effective_policy = policy or NativeBenchmarkCertificationPolicy()
        rows = int(artifact.get("rows") or 0)
        phase_metrics = tuple(_phase_metric(item) for item in artifact.get("phase_metrics") or ())
        aggregate_metrics = tuple(_phase_metric(item) for item in artifact.get("metrics") or ())
        rows_per_second = {
            "postgres_to_mssql": _path_rows_per_second(
                rows=rows,
                phase_metrics=phase_metrics,
                aggregate_metrics=aggregate_metrics,
                aggregate_name="postgres_to_mssql_full_refresh",
                phase_names=("postgres_to_mssql.source_export", "postgres_to_mssql.target_load_finalize"),
            ),
            "mssql_to_clickhouse": _path_rows_per_second(
                rows=rows,
                phase_metrics=phase_metrics,
                aggregate_metrics=aggregate_metrics,
                aggregate_name="mssql_to_clickhouse_full_refresh",
                phase_names=("mssql_to_clickhouse.source_export", "mssql_to_clickhouse.target_load_finalize"),
            ),
        }
        bottleneck_phase = _bottleneck_phase(phase_metrics or aggregate_metrics)
        checks = (
            _check(
                "row_volume_at_or_above_policy",
                rows >= effective_policy.min_rows,
                min_rows=effective_policy.min_rows,
            ),
            _check("mssql_count_matches_rows", _optional_count_matches(artifact, "mssql_count", rows), rows=rows),
            _check(
                "clickhouse_count_matches_rows",
                _optional_count_matches(artifact, "clickhouse_count", rows),
                rows=rows,
            ),
            _slo_check(
                "postgres_to_mssql_slo",
                rows_per_second["postgres_to_mssql"],
                effective_policy.min_postgres_to_mssql_rows_per_second,
            ),
            _slo_check(
                "mssql_to_clickhouse_slo",
                rows_per_second["mssql_to_clickhouse"],
                effective_policy.min_mssql_to_clickhouse_rows_per_second,
            ),
        )
        return NativeBenchmarkCertificationResult(
            passed=all(bool(check["passed"]) for check in checks),
            rows=rows,
            rows_per_second=rows_per_second,
            bottleneck_phase=bottleneck_phase,
            checks=checks,
            recommendations=_recommendations(bottleneck_phase),
        )


def _phase_metric(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(value.get("name") or ""),
        "rows": int(value.get("rows") or 0),
        "seconds": float(value.get("seconds") or 0.0),
        "rows_per_second": float(value.get("rows_per_second") or 0.0),
    }


def _combined_rows_per_second(rows: int, phase_metrics: tuple[dict[str, Any], ...], *phase_names: str) -> float:
    seconds = sum(float(metric["seconds"]) for metric in phase_metrics if metric["name"] in phase_names)
    return round(rows / seconds, 3) if rows > 0 and seconds > 0 else 0.0


def _path_rows_per_second(
    *,
    rows: int,
    phase_metrics: tuple[dict[str, Any], ...],
    aggregate_metrics: tuple[dict[str, Any], ...],
    aggregate_name: str,
    phase_names: tuple[str, ...],
) -> float:
    split_value = _combined_rows_per_second(rows, phase_metrics, *phase_names)
    if split_value:
        return split_value
    for metric in aggregate_metrics:
        if metric["name"] == aggregate_name:
            return float(metric["rows_per_second"])
    return 0.0


def _bottleneck_phase(phase_metrics: tuple[dict[str, Any], ...]) -> str | None:
    if not phase_metrics:
        return None
    return max(phase_metrics, key=lambda item: float(item["seconds"]))["name"]


def _optional_count_matches(artifact: dict[str, Any], key: str, rows: int) -> bool:
    if key not in artifact:
        return True
    return int(artifact[key]) == rows


def _slo_check(name: str, actual: float, expected: float | None) -> dict[str, Any]:
    if expected is None:
        return _check(name, True, actual_rows_per_second=actual, min_rows_per_second=None)
    return _check(name, actual >= expected, actual_rows_per_second=actual, min_rows_per_second=expected)


def _check(name: str, passed: bool, **details: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "details": details}


def _recommendations(bottleneck_phase: str | None) -> tuple[str, ...]:
    if bottleneck_phase == "mssql_to_clickhouse.source_export":
        return (
            "Tune MSSQL bcp queryout: partitioning.column, export_workers, source predicate, bcp packet size, and temp artifact disk.",
        )
    if bottleneck_phase == "mssql_to_clickhouse.target_load_finalize":
        return (
            "Tune ClickHouse ingest/finalizer: load_workers, insert block size, async insert wait, target parts, and staging engine.",
        )
    if bottleneck_phase == "postgres_to_mssql.source_export":
        return (
            "Tune the single PostgreSQL COPY export: bounded indexed source predicate, projection width, source indexes, and network path.",
        )
    if bottleneck_phase == "postgres_to_mssql.target_load_finalize":
        return (
            "Tune MSSQL bcp load/finalizer: bulk batch size, table lock, indexes, recovery model, and transaction size.",
        )
    return ()
