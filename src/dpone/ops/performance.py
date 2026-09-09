"""Production performance and cost planning."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class PerformancePlan:
    sink: str
    strategy: str
    recommendations: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class ProductionPerformancePlanner:
    def plan(self, *, sink: str, strategy: str, row_count: int | None = None) -> PerformancePlan:
        recommendations: list[str] = []
        if sink == "mssql":
            recommendations.append("Use bcp native or lossless delimited bulk files; keep finalization staging-first.")
        if sink == "clickhouse":
            recommendations.append(
                "Use HTTP/Native streaming with TabSeparated or native formats; avoid Python parsing."
            )
        if strategy in {"partition_replace", "backfill"}:
            recommendations.append(
                "Partition chunks should stay below max_partitions_per_run and produce per-chunk load_ids."
            )
        if row_count and row_count >= 1_000_000:
            recommendations.append("Enable partitioned parallel extraction and write throughput artifacts.")
        if not recommendations:
            recommendations.append("Default staged path is acceptable; capture throughput in run artifacts.")
        return PerformancePlan(sink=sink, strategy=strategy, recommendations=tuple(recommendations))
