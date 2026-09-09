from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.ports import LineageProjectionResult, StagedLoadHandle
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sources.base import ExtractResult

ROOT = Path(__file__).resolve().parents[1]


class _Logger:
    def log_etl_start(self, payload):
        del payload

    def log_etl_progress(self, event, payload):
        del event, payload

    def log_etl_error(self, message, payload):
        del message, payload

    def log_etl_end(self, payload):
        del payload

    def info(self, message):
        del message


@dataclass
class _SqlConnector:
    events: list[str]
    fail_on: str | None = None

    def execute(self, sql: str, *, autocommit: bool = False) -> None:
        self.events.append(f"sql:{sql}:{autocommit}")
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("procedure failed")


class _Source:
    def __init__(self, events: list[str], *, connector: _SqlConnector) -> None:
        self.events = events
        self.connector = connector
        self.saved = False

    def get_incremental_state(self, load_config):
        del load_config
        self.events.append("state")
        return None

    def extract(self, load_config, last_state):
        del load_config, last_state
        self.events.append("extract")
        return ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
            state={"cursor": 1},
        )

    def save_state(self, load_config, saved_state):
        del load_config, saved_state
        self.saved = True
        self.events.append("save_state")


class _Sink:
    def __init__(self, events: list[str], *, connector: _SqlConnector) -> None:
        self.events = events
        self.connector = connector

    def load(self, load_config, payload):
        del load_config
        self.events.append("load")
        return LoadResult(
            inserted_rows=len(payload.artifact._rows),
            updated_rows=0,
            total_rows=len(payload.artifact._rows),
            staging_rows=len(payload.artifact._rows),
        )


def _cfg(**hooks) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
        options={"lineage": False, "hooks": hooks},
    )


def test_etl_processor_runs_source_pre_hook_before_extract_and_post_hook_after_load() -> None:
    events: list[str] = []
    source_connector = _SqlConnector(events)
    sink_connector = _SqlConnector(events)
    source = _Source(events, connector=source_connector)
    sink = _Sink(events, connector=sink_connector)

    result = ETLProcessor(source, sink, etl_logger=_Logger()).run(
        _cfg(
            pre_hook=[
                {
                    "id": "refresh_orders",
                    "kind": "source_refresh",
                    "type": "sql",
                    "connector": "source",
                    "sql": "EXEC [dbo].[p_refresh_orders]",
                    "mutates_source": True,
                    "autocommit": True,
                }
            ],
            post_hook=[
                {
                    "id": "target_stats",
                    "kind": "target_maintenance",
                    "type": "sql",
                    "connector": "sink",
                    "sql": "OPTIMIZE TABLE raw.orders FINAL",
                    "mutates_source": False,
                }
            ],
        )
    )

    assert result["status"] == "success"
    assert result["run_throughput"]["schema_version"] == "dpone.runtime.throughput.v1"
    assert result["run_throughput"]["row_count"] == 1
    assert result["run_throughput"]["row_count_source"] == "loaded_rows"
    assert result["run_throughput"]["rows_per_second"] > 0
    assert events == [
        "sql:EXEC [dbo].[p_refresh_orders]:True",
        "state",
        "extract",
        "load",
        "sql:OPTIMIZE TABLE raw.orders FINAL:False",
    ]


def test_etl_processor_blocks_transfer_when_required_pre_hook_fails() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events, fail_on="p_refresh_orders"))
    sink = _Sink(events, connector=_SqlConnector(events))

    with pytest.raises(RuntimeError, match="procedure failed"):
        ETLProcessor(source, sink, etl_logger=_Logger()).run(
            _cfg(
                pre_hook=[
                    {
                        "id": "refresh_orders",
                        "kind": "source_refresh",
                        "type": "sql",
                        "connector": "source",
                        "sql": "EXEC [dbo].[p_refresh_orders]",
                        "mutates_source": True,
                    }
                ]
            )
        )

    assert events == ["sql:EXEC [dbo].[p_refresh_orders]:False"]
    assert source.saved is False


def test_etl_processor_blocks_success_when_quality_gate_fails() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    cfg = _cfg(
        pre_hook=[],
        post_hook=[],
    )
    cfg.options["quality"] = {
        "gates": [
            {
                "id": "row_count_reconciliation",
                "type": "row_count_reconciliation",
                "severity": "error",
                "tolerance": {"mode": "absolute", "value": 0},
            }
        ]
    }

    class BadSink(_Sink):
        def load(self, load_config, payload):
            super().load(load_config, payload)
            return LoadResult(inserted_rows=2, updated_rows=0, total_rows=2, staging_rows=2)

    with pytest.raises(RuntimeError, match="quality gates failed"):
        ETLProcessor(source, BadSink(events, connector=_SqlConnector(events)), etl_logger=_Logger()).run(cfg)


def test_etl_processor_marks_legacy_post_finalize_for_non_staged_sinks() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    result = ETLProcessor(source, _Sink(events, connector=_SqlConnector(events)), etl_logger=_Logger()).run(
        _cfg(pre_hook=[], post_hook=[])
    )

    assert result["reconciliation_metrics"]["governance_finalization"] == "legacy_post_finalize"


def test_etl_processor_uses_staged_governance_before_finalization() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    cfg = _cfg(pre_hook=[], post_hook=[])
    cfg.options["lineage"] = {"enabled": True, "preset": "bulk_standard"}
    cfg.options["quality"] = {"gates": [_strict_count_gate()]}

    result = ETLProcessor(source, _GovernedSink(events), etl_logger=_Logger()).run(cfg)

    assert result["status"] == "success"
    assert events == ["state", "extract", "stage", "project", "finalize"]


def test_etl_processor_aborts_staged_governance_when_pre_finalize_quality_fails() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    cfg = _cfg(pre_hook=[], post_hook=[])
    cfg.options["lineage"] = {"enabled": True, "preset": "bulk_standard"}
    cfg.options["quality"] = {"gates": [_strict_count_gate()]}

    with pytest.raises(RuntimeError, match="quality gates failed"):
        ETLProcessor(source, _GovernedSink(events, staged_rows=2), etl_logger=_Logger()).run(cfg)

    assert events == ["state", "extract", "stage", "project", "abort"]
    assert source.saved is False


def test_etl_processor_configures_clickhouse_load_governance_audit_from_manifest() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    sink = _ClickHouseGovernedSink(events)
    cfg = _cfg(pre_hook=[], post_hook=[])
    cfg.options["lineage"] = {"enabled": True, "preset": "bulk_standard"}
    cfg.options["quality"] = {"gates": [_strict_count_gate()]}
    cfg.options["load_governance"] = {
        "audit": {"enabled": True, "state_schema": "state"},
    }

    ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    rendered = "\n".join(query for query, _ in sink.connector.queries)
    assert "CREATE TABLE IF NOT EXISTS `state`.`__dpone__loads`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `state`.`__dpone__load_steps`" in rendered
    assert "INSERT INTO `state`.`__dpone__loads`" in rendered
    assert "INSERT INTO `state`.`__dpone__load_steps`" in rendered


def test_etl_processor_enables_clickhouse_load_governance_audit_by_default() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    sink = _ClickHouseGovernedSink(events)
    cfg = _cfg(pre_hook=[], post_hook=[])
    cfg.options["lineage"] = {"enabled": True, "preset": "bulk_standard"}
    cfg.options["quality"] = {"gates": [_strict_count_gate()]}

    ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    rendered = "\n".join(query for query, _ in sink.connector.queries)
    assert "CREATE TABLE IF NOT EXISTS `etl_state`.`__dpone__loads`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `etl_state`.`__dpone__load_steps`" in rendered
    assert "INSERT INTO `etl_state`.`__dpone__loads`" in rendered
    assert "INSERT INTO `etl_state`.`__dpone__load_steps`" in rendered


def test_etl_processor_respects_explicit_clickhouse_load_governance_audit_opt_out() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    sink = _ClickHouseGovernedSink(events)
    cfg = _cfg(pre_hook=[], post_hook=[])
    cfg.options["lineage"] = {"enabled": True, "preset": "bulk_standard"}
    cfg.options["quality"] = {"gates": [_strict_count_gate()]}
    cfg.options["load_governance"] = {"audit": {"enabled": False}}

    ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    rendered = "\n".join(query for query, _ in sink.connector.queries)
    assert "__dpone__loads" not in rendered
    assert "__dpone__load_steps" not in rendered


def test_etl_processor_respects_clickhouse_load_governance_audit_mode_off() -> None:
    events: list[str] = []
    source = _Source(events, connector=_SqlConnector(events))
    sink = _ClickHouseGovernedSink(events)
    cfg = _cfg(pre_hook=[], post_hook=[])
    cfg.options["lineage"] = {"enabled": True, "preset": "bulk_standard"}
    cfg.options["quality"] = {"gates": [_strict_count_gate()]}
    cfg.options["load_governance"] = {"audit": {"mode": "off"}}

    ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    rendered = "\n".join(query for query, _ in sink.connector.queries)
    assert "__dpone__loads" not in rendered
    assert "__dpone__load_steps" not in rendered


def test_manifest_schemas_expose_industrial_load_governance_audit_fields() -> None:
    for relative in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        audit = _find_load_governance_audit_properties(schema)

        assert audit["enabled"]["default"] is True
        assert audit["mode"]["enum"] == ["off", "standard", "debug", "forensic"]
        assert audit["resource_metrics"]["default"] is True
        assert audit["step_metrics"]["default"] is True
        assert audit["query_log_correlation"]["default"] is True
        assert audit["retention_days"]["default"] == 180


def _find_load_governance_audit_properties(schema: dict[str, object]) -> dict[str, object]:
    stack = [schema]
    while stack:
        current = stack.pop()
        if not isinstance(current, dict):
            continue
        properties = current.get("properties")
        if isinstance(properties, dict) and "load_governance" in properties:
            governance = properties["load_governance"]
            if isinstance(governance, dict):
                audit = governance.get("properties", {}).get("audit", {})
                if isinstance(audit, dict):
                    return audit.get("properties", {})
        stack.extend(value for value in current.values() if isinstance(value, dict))
        stack.extend(
            item for value in current.values() if isinstance(value, list) for item in value if isinstance(item, dict)
        )
    raise AssertionError("load_governance.audit schema not found")


def _strict_count_gate() -> dict[str, object]:
    return {
        "id": "row_count_reconciliation",
        "type": "row_count_reconciliation",
        "severity": "error",
        "tolerance": {"mode": "absolute", "value": 0},
    }


class _GovernedSink:
    def __init__(self, events: list[str], *, staged_rows: int = 1) -> None:
        self.events = events
        self.staged_rows = staged_rows
        self.connector = _SqlConnector(events)
        self.lineage_projector = _GovernedProjector(events)

    def stage_payload(self, load_config, payload):  # noqa: ANN001
        del load_config
        self.events.append("stage")
        return StagedLoadHandle(
            staging_config=object(),
            payload_schema=tuple(payload.schema),
            staged_rows=self.staged_rows,
        )

    def finalize_staged_load(self, load_config, handle):  # noqa: ANN001
        del load_config
        self.events.append("finalize")
        return LoadResult(
            inserted_rows=handle.staged_rows,
            updated_rows=0,
            total_rows=handle.staged_rows,
            staging_rows=handle.staged_rows,
        )

    def abort_staged_load(self, handle):  # noqa: ANN001
        del handle
        self.events.append("abort")


class ClickHouseConnector:
    def __init__(self) -> None:
        self.queries: list[tuple[str, object | None]] = []

    def execute_query(self, query, params=None):  # noqa: ANN001
        self.queries.append((str(query), params))
        return 0


class _ClickHouseGovernedSink(_GovernedSink):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.connector = ClickHouseConnector()


class _GovernedProjector:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def project(self, *, load_config, handle, lineage_options, load_record):  # noqa: ANN001
        del load_config, lineage_options, load_record
        self.events.append("project")
        return LineageProjectionResult(
            handle=handle,
            projected=True,
            columns=("__dpone__run_id", "__dpone__load_id"),
        )
