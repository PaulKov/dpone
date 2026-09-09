"""SS-56 nested package staged abort, mutation port, and fail-closed gate."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.governance.hooks import InMemoryLoadStepAuditStorage
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.extracted_payload_load import ExtractedPayloadLoadService
from dpone.runtime.etl.nested_load import NestedLoadService
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.lineage import LineageOptions
from dpone.runtime.normalization.load_package import (
    NESTED_PACKAGE_ABORT_FAILED,
    NESTED_PACKAGE_MUTATION_PORT_REQUIRED,
    NESTED_PACKAGE_PARTIAL_FINALIZE,
    NESTED_PACKAGE_STAGED_ABORT_REQUIRED,
    NestedPackagePartialFinalizeError,
)
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.base import ExtractResult
from tests.nested_package_test_support import (
    RecordingStagedSink,
    identity_evaluate,
    identity_prepare,
)


class _Source:
    def get_incremental_state(self, _load_config):
        return None

    def extract(self, _load_config, _last_state):
        return ExtractResult(
            artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}, {"sku": "B"}]}]),
            schema=[("order_id", "bigint"), ("items", "json")],
            state=SimpleNamespace(cursor="next"),
        )


class _LegacySink:
    def __init__(self) -> None:
        self.loaded_tables: list[str] = []

    def load(self, load_config, payload):
        from tests.nested_package_test_support import artifact_rows

        rows = artifact_rows(payload.artifact)
        self.loaded_tables.append(load_config.target_table)
        from dpone.runtime.sinks.base import LoadResult

        return LoadResult(inserted_rows=len(rows), updated_rows=0, total_rows=len(rows), staging_rows=len(rows))


def _nested_config(
    tmp_path: Path | None = None,
    *,
    min_rows: int = 1,
    materialization: str = "spill_to_disk",
) -> LoadConfig:
    nested: dict[str, object] = {"enabled": True, "materialization": materialization}
    if materialization == "spill_to_disk":
        assert tmp_path is not None
        nested["spill_output_dir"] = str(tmp_path)
        nested["spill_output_format"] = "jsonl"
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={
            "schema_evolution": {"enabled": False},
            "lineage": {"preset": "hierarchical"},
            "normalization": {"nested": nested},
            "quality": {
                "gates": [
                    {
                        "id": "aggregate_rows",
                        "type": "min_rows",
                        "side": "target",
                        "threshold": min_rows,
                    }
                ]
            },
        },
    )


def test_nested_package_fails_closed_without_staged_abort(tmp_path: Path) -> None:
    sink = _LegacySink()

    with pytest.raises(RuntimeError, match=NESTED_PACKAGE_STAGED_ABORT_REQUIRED):
        ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(_nested_config(tmp_path))

    assert sink.loaded_tables == []


def test_load_nested_table_payload_compatibility_api_fails_closed() -> None:
    service = ExtractedPayloadLoadService(
        sink=object(),
        logger=SimpleNamespace(log_etl_progress=lambda *_a, **_k: None),
        load_identity_service=SimpleNamespace(),
    )
    with pytest.raises(RuntimeError, match=NESTED_PACKAGE_MUTATION_PORT_REQUIRED):
        service.load_nested_table_payload(
            load_config=object(),
            payload=object(),
            extract_result=object(),
            load_record=object(),
        )


def test_nested_package_aborts_already_staged_member_when_later_stage_fails() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _Identity:
        def mark_staged(self, *_args, **_kwargs) -> None:
            return None

        def mark_failed(self, *_args, **_kwargs) -> None:
            return None

    sink = RecordingStagedSink(fail_stage_table="orders")
    with pytest.raises(RuntimeError, match="child load failed"):
        NestedLoadService(load_identity_service=_Identity()).load(
            load_config=load_config,
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=NestedNormalizationOptions.from_config(
                {
                    "enabled": True,
                    "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
                }
            ),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert "order_lines" in sink.staged_tables
    assert sink.loaded_tables == []
    assert sink.aborted_tables == ["order_lines"]


def test_nested_load_service_requires_package_mutation_port() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    with pytest.raises(RuntimeError, match=NESTED_PACKAGE_MUTATION_PORT_REQUIRED):
        NestedLoadService().load(
            load_config=load_config,
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=NestedNormalizationOptions.from_config({"enabled": True}),
            package_mutation=None,  # type: ignore[arg-type]
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )


def test_nested_package_aborts_staged_members_when_quality_fails(tmp_path: Path) -> None:
    sink = RecordingStagedSink(zero_staged_rows=True)

    with pytest.raises(Exception):
        ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(
            _nested_config(tmp_path, min_rows=1)
        )

    assert sink.loaded_tables == []
    assert sink.staged_tables
    assert set(sink.aborted_tables) == set(sink.staged_tables)


def test_nested_package_finalizes_only_after_quality_passes(tmp_path: Path) -> None:
    sink = RecordingStagedSink()

    result = ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(
        _nested_config(tmp_path, min_rows=1)
    )

    assert result["status"] == "success"
    assert sink.validated_tables == ["orders__items", "orders"]
    assert sink.loaded_tables == ["orders__items", "orders"]
    assert sink.aborted_tables == []


def test_nested_package_validates_every_member_before_any_target_finalize(tmp_path: Path) -> None:
    sink = RecordingStagedSink(fail_validate_table="orders")

    with pytest.raises(RuntimeError, match="staged validation failed"):
        ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(
            _nested_config(tmp_path, min_rows=1)
        )

    assert sink.validated_tables == ["orders__items", "orders"]
    assert sink.loaded_tables == []
    assert set(sink.aborted_tables) == set(sink.staged_tables)


def test_nested_package_in_memory_aborts_when_quality_fails() -> None:
    sink = RecordingStagedSink(zero_staged_rows=True)

    with pytest.raises(Exception):
        ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(
            _nested_config(materialization="memory", min_rows=1)
        )

    assert sink.loaded_tables == []
    assert sink.staged_tables
    assert set(sink.aborted_tables) == set(sink.staged_tables)


def test_nested_package_partial_finalize_retains_unconfirmed_staging(tmp_path: Path) -> None:
    sink = RecordingStagedSink(fail_finalize_table="orders")
    audit = InMemoryLoadStepAuditStorage()

    with pytest.raises(RuntimeError, match=NESTED_PACKAGE_PARTIAL_FINALIZE) as raised:
        ETLProcessor(
            _Source(),
            sink,
            load_governance_service=LoadGovernanceService(audit_storage=audit),
        ).run(_nested_config(tmp_path, min_rows=1))

    assert "orders__items" in sink.loaded_tables
    assert "orders" not in sink.loaded_tables
    assert sink.aborted_tables == []
    failure = next(record for record in audit.records if record.kind == "nested_package_finalization")
    assert failure.details == raised.value.details
    assert failure.details["finalized_tables"] == ["orders__items"]
    assert failure.details["operation_tables"] == {
        "landing.orders:staging": "landing.orders",
    }
    assert failure.details["cleanup_attempted"] is False
    assert failure.details["cleanup_status"] == "retained_for_reconciliation"
    assert failure.details["safe_to_retry"] is False


def test_nested_package_first_target_mutate_then_raise_is_partial_and_retained(tmp_path: Path) -> None:
    sink = RecordingStagedSink(mutate_then_fail_finalize_table="orders__items")

    with pytest.raises(RuntimeError, match=NESTED_PACKAGE_PARTIAL_FINALIZE):
        ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(
            _nested_config(tmp_path, min_rows=1)
        )

    assert sink.loaded_tables == ["orders__items"]
    assert sink.aborted_tables == []


def test_nested_package_interrupt_during_target_finalize_is_commit_unknown() -> None:
    class InterruptingSink(RecordingStagedSink):
        def finalize_staged_load(self, load_config, receipt):
            del load_config, receipt
            raise KeyboardInterrupt

    sink = InterruptingSink()
    mutation = sink.mutation()
    load_config = _nested_config(materialization="memory")
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1}]),
        schema=[("order_id", "integer")],
    )
    mutation.stage(load_config, payload)

    with pytest.raises(NestedPackagePartialFinalizeError) as raised:
        mutation.finalize_all()

    assert mutation.retention_required is True
    assert mutation.target_outcome_unknown is True
    assert len(mutation.entries) == 1
    assert sink.aborted_tables == []
    assert raised.value.details["target_outcome"] == "commit_unknown"
    assert raised.value.details["cleanup_attempted"] is False
    assert raised.value.details["safe_to_retry"] is False


def test_nested_prevalidation_preserves_primary_when_abort_fails(tmp_path: Path) -> None:
    sink = RecordingStagedSink(fail_validate_table="orders", fail_abort=True)

    with pytest.raises(RuntimeError, match="staged validation failed") as raised:
        ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(
            _nested_config(tmp_path, min_rows=1)
        )

    assert any("nested package abort failed" in note for note in raised.value.__notes__)


def test_nested_package_abort_failure_is_not_swallowed(tmp_path: Path) -> None:
    sink = RecordingStagedSink(zero_staged_rows=True, fail_abort=True)

    with pytest.raises(RuntimeError, match=NESTED_PACKAGE_ABORT_FAILED):
        ETLProcessor(_Source(), sink, load_governance_service=LoadGovernanceService()).run(
            _nested_config(tmp_path, min_rows=1)
        )

    assert sink.loaded_tables == []
