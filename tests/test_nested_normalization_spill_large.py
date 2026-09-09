from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.nested_load import NestedLoadService
from dpone.runtime.lineage import LineageOptions
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.sinks import LoadPayload
from tests.nested_package_test_support import RecordingStagedSink, identity_evaluate, identity_prepare


class _Audit:
    run_id = "01JZ0000000000000000000000"
    load_id = "01JZ0000000000000000000001"


class _RetryAudit:
    run_id = "01JZ0000000000000000000002"
    load_id = "01JZ0000000000000000000003"


class _ExactIdentity:
    def __init__(self) -> None:
        self.failures: list[Exception] = []

    def mark_staged(self, _record, *, extracted_rows: int) -> None:
        assert extracted_rows == 129

    def mark_committed(self, _record, _aggregate) -> None:
        return None

    def mark_failed(self, _record, exc: Exception) -> None:
        self.failures.append(exc)


def test_spilled_child_retry_never_repeats_root_mutation(tmp_path: Path) -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": f"sku-{index}"} for index in range(128)]}]),
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
        options={"sink_type": "portable"},
    )
    nested_options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "materialization": "spill_to_disk",
            "spill_output_dir": str(tmp_path),
            "spill_output_format": "jsonl",
        }
    )
    identity = _ExactIdentity()
    fail_child = True

    class _OnceFailSink(RecordingStagedSink):
        def stage_payload(self, load_config, payload):
            nonlocal fail_child
            if load_config.target_table != "orders" and fail_child:
                fail_child = False
                raise RuntimeError("child mutation failed")
            return super().stage_payload(load_config, payload)

    sink = _OnceFailSink()
    service = NestedLoadService(load_identity_service=identity)
    with pytest.raises(RuntimeError, match="child mutation failed") as failure:
        service.load(
            load_config=load_config,
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=nested_options,
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )
    sink = _OnceFailSink()
    result = service.load(
        load_config=load_config,
        payload=payload,
        extract_result=SimpleNamespace(),
        load_record=_RetryAudit(),
        lineage_options=LineageOptions(enabled=True),
        nested_options=nested_options,
        package_mutation=sink.mutation(),
        prepare_member=identity_prepare,
        evaluate_package=identity_evaluate,
    )

    assert identity.failures == [failure.value]
    assert sink.staged_tables.count("orders") == 1
    assert result.total_rows == 129
    assert Path(result.reconciliation_metrics["nested_normalization"]["spill_output_dir"]).name == _RetryAudit.load_id
