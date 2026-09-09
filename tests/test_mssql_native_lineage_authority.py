"""Framework authority contracts for row and native MSSQL lineage."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.lineage import LineageOptions, RowLineageEnricher
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.strategies.mssql import mssql_native_staging
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import (
    MssqlNativeLineageProjection,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import (
    ResolvedMssqlNativeSchema,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import (
    MssqlNativeStagingNormalizer,
)
from dpone.runtime.streaming_rows import StreamingRowsArtifact

_RUN_ID = "R" * 26
_LOAD_ID = "L" * 26
_EXTRACTED_AT = datetime(2026, 8, 16, 9, 30, tzinfo=UTC)


def test_row_lineage_populates_run_identity_for_rows_and_streams_only_when_enabled() -> None:
    """Row-backed artifacts must override a same-named untrusted source value."""

    enabled = LineageOptions.from_config({"preset": "standard", "features": {"run_identity": True}})
    disabled = LineageOptions.from_config({"preset": "standard"})
    enricher = RowLineageEnricher()

    for artifact in (
        InMemoryRowsArtifact([{"id": 1, "__dpone__run_id": "forged"}]),
        StreamingRowsArtifact(iter(({"id": 1, "__dpone__run_id": None},))),
    ):
        payload = LoadPayload(
            artifact=artifact,
            schema=(("id", "int"), ("__dpone__run_id", "varchar(26)")),
        )
        enriched = enricher.enrich_payload(
            payload,
            options=enabled,
            run_id=_RUN_ID,
            load_id=_LOAD_ID,
            source_type="postgres",
            source_schema="public",
            source_table="events",
            unique_key=("id",),
            extracted_at=_EXTRACTED_AT,
            loaded_at=_EXTRACTED_AT,
        )

        rows = (
            list(enriched.artifact._rows)
            if isinstance(enriched.artifact, InMemoryRowsArtifact)
            else list(enriched.artifact._iterator)
        )
        assert rows[0]["__dpone__run_id"] == _RUN_ID

    untouched = list(
        enricher.enrich_rows(
            ({"id": 1},),
            options=disabled,
            run_id=_RUN_ID,
            load_id=_LOAD_ID,
            source_type="postgres",
            source_schema="public",
            source_table="events",
            unique_key=("id",),
            extracted_at=_EXTRACTED_AT,
            loaded_at=_EXTRACTED_AT,
        )
    )
    assert "__dpone__run_id" not in untouched[0]


def test_native_lineage_is_framework_owned_for_row_file_and_stream_wire_shapes(
    monkeypatch: Any,
) -> None:
    """Forged, null, and omitted wire lineage must yield one native contract."""

    monkeypatch.setattr(
        mssql_native_staging,
        "validate_native_conversions",
        lambda *_args, **_kwargs: None,
    )
    config = _lineage_config()
    lineage = MssqlNativeLineageProjection.resolve(
        config,
        ExtractionLifecycleReceipt(
            extraction_started_at=_EXTRACTED_AT,
            extraction_completed_at=_EXTRACTED_AT,
            clock_authority="dpone.test.utc",
        ),
    )
    generated_names = tuple(column.name for column in lineage.columns)

    observations: dict[str, tuple[tuple[str, str], ...]] = {}
    for transport, source_value in (
        ("python_rows", "forged"),
        ("file_stream", None),
        ("postgres_copy_mssql_bcp", _Omitted),
    ):
        connector = _RecordingConnector()
        staging = _RecordingStaging()
        strategy = SimpleNamespace(
            connector=connector,
            staging_manager=staging,
            _staging_name=lambda artifact: f"[{artifact.schema}].[{artifact.table}]",
        )
        normalizer = MssqlNativeStagingNormalizer(strategy)
        source_columns = [("id", "int")]
        if source_value is not _Omitted:
            source_columns.extend((column.name, column.target_type) for column in lineage.columns)
        resolved = _resolved(source_columns).with_lineage(lineage)
        raw = StagingTableArtifact(
            schema="staging",
            table=f"raw_{transport}",
            columns=[name for name, _dtype in source_columns],
            staging_manager=staging,
            row_count=1,
            column_types=dict(source_columns),
            target_column_types=dict(source_columns),
        )

        normalizer.normalize(
            config,
            raw,
            source_columns,
            resolved,
            lineage=lineage,
        )

        insert = next(query for query in connector.queries if "INSERT INTO" in query)
        assert not any(query.startswith("UPDATE n SET") for query in connector.queries)
        evidence = {str(item["target_name"]): item for item in staging.evidence}
        for column in lineage.columns:
            assert f"AS [{column.name}]" in insert, transport
            assert f"r.[{column.name}]" not in insert, transport
            assert evidence[column.name]["wire_name"] == f"framework:{column.name}"
            assert evidence[column.name]["generation_contract"] == column.generation_contract
        assert "CONVERT(int, r.[id]) AS [id]" in insert
        assert "N'RRRR" in insert
        assert "HASHBYTES('SHA2_256'" in insert
        assert "SYSUTCDATETIME" not in "\n".join(connector.queries)
        observations[transport] = tuple(
            (
                str(evidence[name]["wire_name"]),
                str(evidence[name]["generation_contract"]),
            )
            for name in generated_names
        )

    assert len(set(observations.values())) == 1


def test_xmin_initial_generates_and_recomputes_snapshot_hash_inside_native_staging(
    monkeypatch: Any,
) -> None:
    """The immutable COPY wire must not carry target-only snapshot metadata."""

    monkeypatch.setattr(
        mssql_native_staging,
        "validate_native_conversions",
        lambda *_args, **_kwargs: None,
    )
    config = LoadConfig(
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
        },
    )
    connector = _RecordingConnector()
    staging = _RecordingStaging()
    strategy = SimpleNamespace(
        connector=connector,
        staging_manager=staging,
        _staging_name=lambda artifact: f"[{artifact.schema}].[{artifact.table}]",
    )
    normalizer = MssqlNativeStagingNormalizer(strategy)
    source_schema = [("id", "int")]
    resolved = normalizer.resolve_schema(
        config,
        source_schema,
        relation_schema=None,
        relation_metadata=None,
        relation_dialect=None,
        target_projection=None,
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="raw_xmin_initial",
        columns=["id"],
        staging_manager=staging,
        row_count=1,
        column_types={"id": "nvarchar(max)"},
        target_column_types={"id": "int"},
    )

    normalizer.normalize(
        config,
        raw,
        source_schema,
        resolved,
        lineage=SimpleNamespace(columns=()),
    )

    insert = next(query for query in connector.queries if "INSERT INTO" in query)
    evidence = {str(item["target_name"]): item for item in staging.evidence}

    assert "HASHBYTES('SHA2_256'" in insert
    assert "CONVERT(datetime2(7), NULL) AS [__dpone__deleted_at]" in insert
    assert not any(query.startswith("UPDATE n SET [__dpone__row_hash]") for query in connector.queries)
    assert "r.[__dpone__row_hash]" not in insert
    assert evidence["__dpone__row_hash"]["wire_name"] == "framework:__dpone__row_hash"
    assert evidence["__dpone__deleted_at"]["wire_name"] == "framework:__dpone__deleted_at"


class _Omitted:
    """Sentinel for a file artifact without source-side lineage columns."""


class _RecordingConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def execute_query(self, query: str, *_args: Any, **_kwargs: Any) -> int:
        self.queries.append(str(query))
        return 1

    def get_records(self, query: str, *_args: Any, **_kwargs: Any) -> list[tuple[int]]:
        rendered = str(query)
        if rendered.startswith("SELECT COUNT_BIG(*) FROM"):
            return [(1,)]
        return []


class _RecordingStaging:
    def __init__(self) -> None:
        self.evidence: tuple[dict[str, object], ...] = ()

    def create(
        self,
        config: LoadConfig,
        schema: tuple[tuple[str, str], ...],
    ) -> StagingTableArtifact:
        types = dict(schema)
        return StagingTableArtifact(
            schema=config.staging_schema,
            table=str(config.staging_table),
            columns=[name for name, _dtype in schema],
            staging_manager=self,
            column_types=types,
            target_column_types=types,
            target_column_nullability={
                name: name not in config.options["__dpone_mssql_native_not_null_columns"] for name, _dtype in schema
            },
        )

    def finalize_native_evidence(
        self,
        _raw: StagingTableArtifact,
        _native: StagingTableArtifact,
        *,
        columns: tuple[dict[str, object], ...],
    ) -> None:
        self.evidence = tuple(dict(column) for column in columns)

    @contextmanager
    def database_authority_scope(self, _artifact: StagingTableArtifact):
        """Model the already-verified staging database lease in this pure unit."""

        yield

    def drop(self, _artifact: StagingTableArtifact) -> None:
        return None


def _lineage_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_schema="dbo",
        target_table="events",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "source_type": "postgres",
            "lineage": {
                "preset": "hierarchical",
                "features": {
                    "run_identity": True,
                    "operations": True,
                    "diagnostics": True,
                },
            },
            "__dpone_load_identity": {"run_id": _RUN_ID, "load_id": _LOAD_ID},
        },
    )


def _resolved(
    source_columns: list[tuple[str, str]],
) -> ResolvedMssqlNativeSchema:
    types = dict(source_columns)
    return ResolvedMssqlNativeSchema(
        types=types,
        source_types=dict(types),
        nullability={name: True for name in types},
        collations={},
        wire_to_target={name: name for name in types},
    )
