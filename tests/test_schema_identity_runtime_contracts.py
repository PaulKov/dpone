from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.base import LoadResult
from dpone.type_system.source_sink.provenance import SourceColumnProvenance


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

    def warning(self, message):
        del message


class _Source:
    def __init__(self, rows, schema):
        self.connector = object()
        self._result = SimpleNamespace(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
            state=None,
            force_full_refresh=False,
        )

    def get_incremental_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, state):
        del load_config, state
        return self._result


class _Sink:
    def __init__(self, *, nullable: bool = True) -> None:
        self.received_payload = None
        self.nullable = nullable

    def get_target_columns(self, load_config):
        del load_config
        return [("customer_id", "bigint", self.nullable)]

    def load(self, load_config, payload):
        del load_config
        self.received_payload = payload
        return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)


def test_runtime_projects_source_alias_to_canonical_before_schema_evolution() -> None:
    source = _Source(rows=[{"client_id": 42}], schema=[("client_id", "bigint")])
    sink = _Sink()

    ETLProcessor(source, sink, etl_logger=_Logger()).run(_load_config())

    assert sink.received_payload.schema == [("customer_id", "bigint")]
    assert sink.received_payload.artifact._rows == [{"customer_id": 42}]


def test_runtime_blocks_alias_conflict_before_sink_load() -> None:
    source = _Source(
        rows=[{"client_id": 42, "customer_id": 43}],
        schema=[("client_id", "bigint"), ("customer_id", "bigint")],
    )
    sink = _Sink()

    with pytest.raises(RuntimeError, match="schema identity projection blocked"):
        ETLProcessor(source, sink, etl_logger=_Logger()).run(_load_config())

    assert sink.received_payload is None


def test_runtime_projects_alias_relation_schema_and_metadata_in_lockstep() -> None:
    source = _Source(rows=[{"client_id": 42}], schema=[("client_id", "bigint")])
    source._result.relation_schema = (("client_id", "bigint"),)
    source._result.relation_metadata = (
        SourceColumnProvenance(
            name="client_id",
            declared_type="bigint",
            nullable=False,
            type_schema="pg_catalog",
            type_name="int8",
        ),
    )
    sink = _Sink(nullable=False)

    ETLProcessor(source, sink, etl_logger=_Logger()).run(_load_config())

    assert sink.received_payload.relation_schema == (("customer_id", "bigint"),)
    assert sink.received_payload.relation_metadata == (
        SourceColumnProvenance(
            name="customer_id",
            declared_type="bigint",
            nullable=False,
            type_schema="pg_catalog",
            type_name="int8",
        ),
    )


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "lineage": False,
            "schema_identity": {
                "enabled": True,
                "columns": {
                    "customer_id": {
                        "id": "orders.customer_id",
                        "aliases": [{"name": "client_id"}],
                    }
                },
            },
        },
    )
