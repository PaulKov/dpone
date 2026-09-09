"""Canonical MSSQL strategy-metadata shape across target and artifact paths."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.etl.mssql_fresh_target_preplan import resolve_mssql_target_columns
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import (
    resolve_mssql_native_schema,
)
from dpone.runtime.sinks.strategies.mssql.mssql_strategy_metadata import (
    resolve_mssql_strategy_metadata_columns,
)
from dpone.runtime.streaming_rows import StreamingRowsArtifact

_EFFECTIVE_AT = datetime(2026, 8, 16, 12, 34, 56, 123456, tzinfo=UTC)


@pytest.mark.parametrize(
    ("strategy", "expected"),
    (
        (
            LoadStrategy.SNAPSHOT_DIFF,
            (
                ("__dpone__row_hash", "varchar(64)", False),
                ("__dpone__deleted_at", "datetime2(7)", True),
            ),
        ),
        (
            LoadStrategy.SCD2,
            (
                ("__dpone__row_hash", "varchar(64)", False),
                ("__dpone__valid_from_at", "datetime2(7)", False),
                ("__dpone__valid_to_at", "datetime2(7)", True),
                ("__dpone__is_current", "bit", False),
            ),
        ),
    ),
)
def test_mssql_strategy_metadata_is_identical_for_row_file_and_stream(
    tmp_path: Path,
    strategy: LoadStrategy,
    expected: tuple[tuple[str, str, bool], ...],
) -> None:
    """Artifact transport cannot change framework physical types/nullability."""

    config = _config(strategy)
    observations: dict[str, tuple[tuple[str, str, bool], ...]] = {}
    for transport in ("python_rows", "file_stream", "postgres_copy_mssql_bcp"):
        payload = _payload(tmp_path, transport)
        enriched = StrategyMetadataEnricher().enrich_payload(
            payload,
            load_config=config,
            effective_at=_EFFECTIVE_AT,
        )
        _consume_lazy_artifact(enriched.artifact)
        resolved = resolve_mssql_native_schema(
            config,
            enriched.schema,
            relation_schema=None,
            relation_dialect=None,
            target_projection=None,
        )
        observations[transport] = tuple(
            (name, resolved.types[name], resolved.nullability[name]) for name, _target_type, _nullable in expected
        )

    assert resolve_mssql_strategy_metadata_columns(config) == expected
    assert set(observations) == {
        "python_rows",
        "file_stream",
        "postgres_copy_mssql_bcp",
    }
    assert len(set(observations.values())) == 1
    assert next(iter(observations.values())) == expected


@pytest.mark.parametrize("strategy", (LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2))
def test_fresh_target_preplan_uses_the_same_strategy_metadata_authority(
    strategy: LoadStrategy,
) -> None:
    """Pre-source target identity and post-source native schema must not drift."""

    config = _config(strategy)
    planned = {
        column.name: (column.dtype, column.nullable)
        for column in resolve_mssql_target_columns(
            config,
            [ColumnDef("id", "int", nullable=False), ColumnDef("value", "nvarchar(max)")],
        )
    }

    assert {
        name: planned[name] for name, _target_type, _nullable in resolve_mssql_strategy_metadata_columns(config)
    } == {
        name: (target_type, nullable) for name, target_type, nullable in resolve_mssql_strategy_metadata_columns(config)
    }


def test_xmin_initial_generates_snapshot_metadata_in_native_staging_without_rewriting_copy_file(
    tmp_path: Path,
) -> None:
    """Initial and incremental phases share one exact target without local hashing."""

    config = _xmin_initial_config()
    payload = _payload(tmp_path, "postgres_copy_mssql_bcp")
    path = Path(payload.artifact.file_path)
    original = path.read_bytes()

    enriched = StrategyMetadataEnricher().enrich_payload(
        payload,
        load_config=config,
        effective_at=_EFFECTIVE_AT,
    )
    resolved = resolve_mssql_native_schema(
        config,
        enriched.schema,
        relation_schema=None,
        relation_dialect=None,
        target_projection=None,
    )
    planned = {
        column.name: (column.dtype, column.nullable)
        for column in resolve_mssql_target_columns(
            config,
            [ColumnDef("id", "int", nullable=False), ColumnDef("value", "nvarchar(max)")],
        )
    }

    assert enriched is payload
    assert path.read_bytes() == original
    assert resolve_mssql_strategy_metadata_columns(config) == (
        ("__dpone__row_hash", "varchar(64)", False),
        ("__dpone__deleted_at", "datetime2(7)", True),
    )
    assert planned["__dpone__row_hash"] == ("varchar(64)", False)
    assert planned["__dpone__deleted_at"] == ("datetime2(7)", True)
    assert tuple(column.name for column in resolved.generated_columns) == (
        "__dpone__row_hash",
        "__dpone__deleted_at",
    )
    assert resolved.types["__dpone__row_hash"] == "varchar(64)"
    assert resolved.nullability["__dpone__row_hash"] is False
    assert resolved.nullability["__dpone__deleted_at"] is True
    assert "REPLICATE('0', 64)" in resolved.generated_columns[0].placeholder_sql


def _config(strategy: LoadStrategy) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_schema="dbo",
        target_table="events",
        staging_schema="staging",
        load_strategy=strategy,
        unique_key=["id"],
        options={"source_type": "postgres", "sink_type": "mssql", "lineage": False},
    )


def _xmin_initial_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_schema="dbo",
        target_table="events",
        staging_schema="staging",
        load_strategy=LoadStrategy.BACKFILL,
        unique_key=["id"],
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "initial", "handoff_id": "events_v1"},
            "backfill": {
                "inner_mode": "incremental_merge",
                "parallel_workers": 4,
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
                "chunk": {"column": "id", "kind": "uuid", "buckets": 512},
            },
            "lineage": False,
        },
    )


def _payload(tmp_path: Path, transport: str) -> LoadPayload:
    row = {"id": 1, "value": "alpha"}
    schema = [("id", "int"), ("value", "text")]
    if transport == "python_rows":
        artifact: object = InMemoryRowsArtifact([row])
    elif transport == "file_stream":
        artifact = StreamingRowsArtifact(iter((row,)))
    else:
        path = tmp_path / f"{transport}.tsv"
        path.write_text("1\talpha\n", encoding="utf-8")
        artifact = FileExportArtifact(
            file_path=str(path),
            columns=["id", "value"],
            format="mssql-delimited",
            rows_exported=1,
        )
    return LoadPayload(artifact=artifact, schema=schema)


def _consume_lazy_artifact(artifact: object) -> None:
    if isinstance(artifact, InMemoryRowsArtifact):
        assert len(list(artifact._rows)) == 1
    elif isinstance(artifact, StreamingRowsArtifact):
        assert len(list(artifact._iterator)) == 1
    else:
        assert isinstance(artifact, FileExportArtifact)
        assert Path(artifact.file_path).is_file()
