"""MSSQL native lineage must not be serialized on the source wire first."""

from __future__ import annotations

from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.etl.extracted_payload_load import ExtractedPayloadLoadService
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.streaming_rows import StreamingRowsArtifact


def test_mssql_native_lineage_capability_bypasses_python_row_enrichment() -> None:
    captured: list[object] = []

    class _NativeLineageSink:
        @staticmethod
        def native_lineage_projection_capability() -> str:
            return "mssql_native_lineage_v1"

    class _ForbiddenRowEnricher:
        @staticmethod
        def enrich_payload(*_args, **_kwargs):
            raise AssertionError("MSSQL owns authoritative lineage after typed native projection")

    class _PayloadLoader:
        @staticmethod
        def load_single_payload(_load_config, payload, *_args, **_kwargs):
            captured.append(payload)
            return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)

    class _Logger:
        @staticmethod
        def log_etl_progress(*_args, **_kwargs) -> None:
            return None

    service = ExtractedPayloadLoadService(
        sink=_NativeLineageSink(),
        logger=_Logger(),
        load_identity_service=object(),
        row_lineage_enricher=_ForbiddenRowEnricher(),
        payload_load_service=_PayloadLoader(),
    )
    artifact = StreamingRowsArtifact(iter(({"id": 1},)))
    extract_result = SimpleNamespace(
        artifact=artifact,
        schema=(("id", "Int64"),),
        relation_schema=None,
        relation_metadata=None,
        relation_dialect="clickhouse",
        target_projection=None,
        extraction_lifecycle=None,
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="marketing",
        source_table="events",
        target_schema="ch",
        target_table="marketing__events",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"source_type": "clickhouse", "sink_type": "mssql", "lineage": {"preset": "standard"}},
    )

    result, _metrics = service.load_extracted_payload(
        load_config=config,
        extract_result=extract_result,
        load_record=SimpleNamespace(run_id="01RUN", load_id="01LOAD"),
        reconciliation_service=SimpleNamespace(run_if_enabled=lambda *_args, **_kwargs: None),
    )

    assert result.inserted_rows == 1
    assert len(captured) == 1
    payload = captured[0]
    assert payload.artifact is artifact
    assert tuple(payload.schema) == (("id", "Int64"),)
    assert next(artifact._iterator) == {"id": 1}
