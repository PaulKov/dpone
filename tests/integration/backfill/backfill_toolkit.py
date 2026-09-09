"""Reusable seed / parity / run harness for backfill integration tests.

The toolkit keeps route tests declarative: each source/target engine is an
endpoint object with a uniform contract (seed, checksum, source/sink factory),
so the route x inner-strategy matrix composes endpoints instead of copying
SQL per test.

Deterministic dataset shared by every route::

    id            1..rows
    business_date start_date + ((id - 1) % days)
    bucket        ((id - 1) % buckets) + 1
    amount        id * 7

Parity contract: ``(count, sum(id), sum(amount))`` must match between the
seeded source window and the loaded target.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.etl.backfill_orchestrator import execute_process_with_backfill
from dpone.runtime.etl.processor import ETLProcessor

Checksum = tuple[int, int, int]


@dataclass(frozen=True, slots=True)
class SeedSpec:
    """Deterministic dataset shape shared by all routes."""

    rows: int = 120
    start_date: date = date(2025, 1, 1)
    days: int = 6
    buckets: int = 4

    @property
    def end_date(self) -> date:
        return self.start_date + timedelta(days=self.days - 1)

    def expected_checksum(self) -> Checksum:
        total_id = self.rows * (self.rows + 1) // 2
        return (self.rows, total_id, total_id * 7)


@dataclass(frozen=True, slots=True)
class BackfillWindow:
    """Chunk window declaration for one matrix case."""

    column: str
    start: str
    end: str
    step: str
    kind: str | None = None

    def to_options(self) -> dict[str, Any]:
        chunk: dict[str, Any] = {"column": self.column, "from": self.start, "to": self.end, "step": self.step}
        if self.kind:
            chunk["kind"] = self.kind
        return chunk


def date_window(spec: SeedSpec, *, step: str = "2d") -> BackfillWindow:
    return BackfillWindow(
        column="business_date",
        start=spec.start_date.isoformat(),
        end=spec.end_date.isoformat(),
        step=step,
    )


def integer_window(spec: SeedSpec, *, column: str = "bucket", step: str = "1") -> BackfillWindow:
    upper = spec.buckets if column == "bucket" else spec.rows
    return BackfillWindow(column=column, start="1", end=str(upper), step=step, kind="integer")


def full_window(spec: SeedSpec) -> BackfillWindow:
    """Single-chunk window covering the whole dataset (for full_refresh)."""

    return BackfillWindow(column="id", start="1", end=str(spec.rows), step=str(spec.rows), kind="integer")


@dataclass(slots=True)
class BackfillCase:
    """One route x inner-strategy matrix case."""

    inner_mode: str
    window: BackfillWindow
    unique_key: str | None = None
    partition_column: str | None = None
    parallel_workers: int = 1
    export_format: str = "csv"
    source_options: dict[str, Any] = field(default_factory=dict)
    sink_options: dict[str, Any] = field(default_factory=dict)


def build_load_config(
    *,
    case: BackfillCase,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    source_type: str,
    sink_type: str,
    state_dir: Path,
    extra_backfill: dict[str, Any] | None = None,
) -> LoadConfig:
    backfill_options: dict[str, Any] = {
        "inner_mode": case.inner_mode,
        "parallel_workers": case.parallel_workers,
        "state_dir": str(state_dir),
        "chunk": case.window.to_options(),
    }
    backfill_options.update(extra_backfill or {})
    options: dict[str, Any] = {
        "source_type": source_type,
        "sink_type": sink_type,
        "backfill": backfill_options,
        # Audit table lifecycles are covered by governance suites; keep matrix
        # cases isolated from shared etl_state tables in the long-lived stack.
        "load_governance": {"enabled": False},
        # Row lineage projection is covered by lineage suites; the matrix
        # keeps parity checks strategy-focused and byte-deterministic.
        "lineage": {"enabled": False},
    }
    options.update(case.source_options)
    options.update(case.sink_options)
    return LoadConfig(
        source_conn_id=f"{source_type}_it",
        target_conn_id=f"{sink_type}_it",
        source_schema=source_schema,
        source_table=source_table,
        target_schema=target_schema,
        target_table=target_table,
        load_strategy=LoadStrategy.BACKFILL,
        unique_key=case.unique_key,
        partition={"column": case.partition_column} if case.partition_column else {},
        export_format=case.export_format,
        options=options,
    )


def run_backfill(source: Any, sink: Any, load_config: LoadConfig) -> dict[str, Any]:
    """Real end-to-end run: extraction, staging and finalization per chunk."""

    sink_connector = getattr(sink, "connector", None)
    if getattr(sink_connector, "dialect", None) == "mssql":
        return _run_governed_mssql_backfill(source, sink_connector, load_config)
    processor = ETLProcessor(source=source, sink=sink)
    return execute_process_with_backfill(processor, load_config)


def _run_governed_mssql_backfill(
    source: Any,
    sink_connector: Any,
    load_config: LoadConfig,
) -> dict[str, Any]:
    """Compose the mandatory external MSSQL transaction authority for live routes."""

    from tests.integration.postgres.postgres_mssql_governance_live_support import (
        bind_factual_postgres_source_authority,
        governed_mssql_route,
    )

    target_database = str(sink_connector.database)
    load_config.target_database = target_database
    load_config.staging_database = target_database
    with governed_mssql_route(
        sink_connector,
        target_database=target_database,
        target_schema=str(load_config.target_schema),
        target_table=str(load_config.target_table),
    ) as route:
        if getattr(getattr(source, "connector", None), "dialect", None) == "postgres":
            bind_factual_postgres_source_authority(
                source,
                source.connector,
                load_config=load_config,
            )
        logger = getattr(source, "logger", None)
        governed_sink = route.sink(logger=logger)
        processor = ETLProcessor(source=source, sink=governed_sink, etl_logger=logger)
        label = f"backfill_{load_config.target_schema}_{load_config.target_table}"
        return execute_process_with_backfill(
            processor,
            load_config,
            route.run_context(label),
            dag_id="DAG__integration__backfill__postgres_mssql",
        )


def read_ledger(result: dict[str, Any]) -> dict[str, Any]:
    return json.loads(Path(result["backfill"]["state_path"]).read_text(encoding="utf-8"))


def assert_campaign_committed(result: dict[str, Any], *, chunks: int) -> None:
    """Ledger invariants for a fully committed campaign."""

    assert result["status"] == "success", f"backfill failed: {result.get('errors')}"
    summary = result["backfill"]
    assert summary["chunks_total"] == chunks
    assert summary["chunks_committed"] == chunks
    assert summary["chunks_failed"] == 0
    ledger = read_ledger(result)
    statuses = [chunk["status"] for chunk in ledger["chunks"]]
    assert statuses == ["success"] * chunks
    keys = [chunk["idempotency_key"] for chunk in ledger["chunks"]]
    assert len(set(keys)) == chunks, "idempotency keys must be unique per chunk"
    boundaries = [(chunk["start"], chunk["end"]) for chunk in ledger["chunks"]]
    assert len(set(boundaries)) == chunks, "chunk windows must not overlap"


def assert_parity(actual: Checksum, expected: Checksum, *, context: str) -> None:
    assert actual == expected, f"{context}: checksum mismatch actual={actual} expected={expected}"


__all__ = [
    "BackfillCase",
    "BackfillWindow",
    "Checksum",
    "SeedSpec",
    "assert_campaign_committed",
    "assert_parity",
    "build_load_config",
    "date_window",
    "full_window",
    "integer_window",
    "read_ledger",
    "run_backfill",
]
