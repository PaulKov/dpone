"""Generated dbt transfer windows must remain executable after interval binding."""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.contracts.dbt_publish_models import (
    DbtColumnArtifact,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishProfile,
    DbtPublishStrategyPolicy,
)
from dpone.contracts.run_interval import RunInterval
from dpone.dag.config_models import ETLProcessConfig
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
from dpone.services.dbt_publish_planning import DbtPublishPlanner
from dpone.services.interval_context import IntervalContextService


@pytest.mark.parametrize(("window_days", "lookback_days"), [(1, 0), (2, 1)])
def test_compiled_partition_window_resolves_both_bounds(tmp_path: Path, window_days: int, lookback_days: int) -> None:
    model = DbtModelArtifact(
        unique_id="model.synthetic.events",
        name="events",
        original_file_path="models/events.sql",
        database="warehouse",
        schema="mart",
        alias="events",
        materialized="table",
        contract_enforced=True,
        columns=("event_id", "event_date"),
        column_contracts=(
            DbtColumnArtifact("event_id", "int", False, ("not_null",)),
            DbtColumnArtifact("event_date", "date", False, ("not_null",)),
        ),
        group="synthetic",
        tags=(),
        meta={},
        unique_key=(),
        depends_on=(),
        fqn=("synthetic", "events"),
    )
    profile = DbtPublishProfile(
        name="synthetic_route",
        source_type="mssql",
        source_connection_ref="mssql_source",
        sink_type="clickhouse",
        sink_connection_ref="clickhouse_sink",
        target_schema="analytics",
        staging_schema="staging",
        runtime_image="example/runtime@sha256:" + "a" * 64,
        state={
            "type": "mssql",
            "connection_ref": "mssql_state",
            "partition_checkpoint_table": {"schema": "state", "name": "checkpoints"},
        },
    )
    intent = DbtPublishIntent(
        enabled=True,
        profile=profile.name,
        workflow="synthetic_events",
        strategy_mode="partition_replace",
        partition_key="event_date",
        window_days=window_days,
    )
    compiled = DbtModelToWorkloadCompiler(planner=DbtPublishPlanner()).compile(
        model,
        intent,
        profile,
        DbtPublishStrategyPolicy(allowed_strategies=("partition_replace",)),
        supported_strategies=("partition_replace",),
    )
    assert not compiled.warnings
    process = ETLProcessConfig.from_dict(dict(compiled.manifest), base_path=tmp_path, metadata_only=True)
    interval = RunInterval(interval_start="2026-09-01T00:00:00Z", interval_end="2026-09-02T00:00:00Z")

    load_config = IntervalContextService(interval).apply(process.load_config)

    assert load_config.options["source_custom_predicate"] == (
        f"[event_date] >= DATEADD(day, -{lookback_days}, "
        "CONVERT(datetime2, '2026-09-01T00:00:00Z', 127)) "
        "AND [event_date] < CONVERT(datetime2, '2026-09-02T00:00:00Z', 127)"
    )
    assert compiled.strategy["window_days"] == window_days
    assert load_config.source_database == "warehouse"
    assert load_config.source_schema == "warehouse.mart"
    assert load_config.target_schema == "analytics"
