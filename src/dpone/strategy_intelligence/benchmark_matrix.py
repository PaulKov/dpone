"""Reusable native benchmark scenario models.

The module is intentionally pure. It knows nothing about MSSQL, ClickHouse,
Postgres clients, Docker, or subprocess execution. Tooling and CI workflows use
these models to build concrete commands without duplicating tuning matrix
semantics.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class BenchmarkScenario:
    """One benchmark matrix point."""

    name: str
    rows: int
    partitions: int
    export_workers: int
    load_workers: int
    output_file: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def parse_csv_ints(value: str | None, *, default: int | None = None) -> list[int]:
    """Parse comma-separated positive integers.

    Args:
        value: Comma-separated value such as ``"2,4,8"``.
        default: Optional default when ``value`` is empty.

    Raises:
        ValueError: when any parsed value is below one.
    """

    if not value:
        return [default] if default is not None else []
    parsed = [int(item.strip()) for item in value.split(",") if item.strip()]
    if any(item < 1 for item in parsed):
        raise ValueError("Benchmark matrix values must be positive integers.")
    return parsed


def build_benchmark_scenarios(
    *,
    rows: list[int],
    partitions: list[int],
    export_workers: list[int],
    load_workers: list[int],
    output_dir: Path,
) -> list[BenchmarkScenario]:
    """Build deterministic scenario names for native transfer tuning."""

    scenarios: list[BenchmarkScenario] = []
    for row_count in rows:
        for partition_count in partitions:
            effective_export_workers = [1] if partition_count <= 1 else export_workers
            effective_load_workers = [1] if partition_count <= 1 else load_workers
            for export_worker_count in effective_export_workers:
                for load_worker_count in effective_load_workers:
                    name = f"rows{row_count}_p{partition_count}_ew{export_worker_count}_lw{load_worker_count}"
                    scenarios.append(
                        BenchmarkScenario(
                            name=name,
                            rows=row_count,
                            partitions=partition_count,
                            export_workers=export_worker_count,
                            load_workers=load_worker_count,
                            output_file=str(output_dir / f"{name}.json"),
                        )
                    )
    return scenarios
