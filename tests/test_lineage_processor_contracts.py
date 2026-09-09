from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.load_config_runtime import LoadConfigRuntimeService
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.kafka.offsets import KafkaOffsetState
from dpone.runtime.lineage import LineageIdentityService
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sources.base import ExtractResult


def _cfg(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options=options,
    )


@dataclass
class _Source:
    events: list[str]

    def get_incremental_state(self, load_config):
        self.events.append("get_state")
        return None

    def extract(self, load_config, last_state):
        self.events.append("extract")
        return ExtractResult(
            artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
            schema=[("id", "bigint"), ("name", "text")],
            state=KafkaOffsetState(
                topic="orders",
                group_id="dpone.orders",
                partition_offsets={0: 1},
                high_watermarks={0: 1},
                read_mode="offsets",
            ),
        )

    def save_state(self, load_config, saved_state):
        self.events.append("save_state")


@dataclass
class _Sink:
    events: list[str]
    rows: list[dict] | None = None

    def load(self, load_config, payload):
        self.events.append("load")
        self.rows = list(payload.artifact._rows)
        assert ("__dpone__load_id", "varchar(26)") in payload.schema
        assert ("__dpone__row_id", "varchar(64)") in payload.schema
        return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)


class _MemoryLoadAuditStorage:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def record_load_started(self, record):
        self.events.append(("started", record.load_id))

    def record_load_staged(self, record):
        self.events.append(("staged", record.load_id))

    def record_load_committed(self, record):
        self.events.append(("committed", record.load_id))

    def record_load_failed(self, record):
        self.events.append(("failed", record.load_id))


def test_etl_processor_enriches_rows_and_commits_load_before_source_state() -> None:
    runtime_events: list[str] = []
    source = _Source(runtime_events)
    sink = _Sink(runtime_events)
    audit_storage = _MemoryLoadAuditStorage()
    identity = LineageIdentityService()
    load_identity = LoadIdentityService(identity_service=identity, audit_storage=audit_storage)

    result = ETLProcessor(source, sink, load_identity_service=load_identity).run(_cfg())

    assert result["status"] == "success"
    assert sink.rows is not None
    assert len(sink.rows[0]["__dpone__load_id"]) == 26
    assert len(sink.rows[0]["__dpone__row_id"]) == 64
    assert sink.rows[0]["__dpone__extracted_at"]
    assert sink.rows[0]["__dpone__loaded_at"]
    assert [event for event, _ in audit_storage.events] == ["started", "staged", "committed"]
    assert runtime_events == ["get_state", "extract", "load", "save_state"]


@pytest.mark.parametrize(
    "strategy",
    [
        LoadStrategy.SNAPSHOT_DIFF,
        LoadStrategy.SCD2,
        LoadStrategy.CDC_APPLY,
        LoadStrategy.BACKFILL,
    ],
)
def test_runtime_service_persists_state_for_new_production_strategies(strategy: LoadStrategy) -> None:
    cfg = _cfg(lineage=False)
    cfg.load_strategy = strategy
    extract_result = ExtractResult(artifact=InMemoryRowsArtifact([]), schema=[], state=object())

    assert LoadConfigRuntimeService().should_persist_state(cfg, cfg, extract_result) is True
