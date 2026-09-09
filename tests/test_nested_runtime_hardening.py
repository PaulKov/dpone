from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.nested_load import NestedLoadService
from dpone.runtime.etl.nested_load_config import table_load_config
from dpone.runtime.lineage import LineageOptions
from dpone.runtime.normalization import certification_checks
from dpone.runtime.normalization.child_quality import ChildQualityViolationError
from dpone.runtime.normalization.hierarchy_builder import HierarchyBuilderService
from dpone.runtime.normalization.models import NormalizationResult, NormalizedTable
from dpone.runtime.normalization.normalizer import NestedNormalizationService
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.spill import SpillToDiskNormalizationService
from dpone.runtime.sinks import LoadPayload
from tests.nested_package_test_support import (
    RecordingStagedSink,
    identity_evaluate,
    identity_prepare,
)


class _Audit:
    run_id = "01JZ0000000000000000000000"
    load_id = "01JZ0000000000000000000001"


class _RecordingIdentity:
    def __init__(self) -> None:
        self.events: list[str] = []

    def mark_staged(self, *_args: object, **_kwargs: object) -> None:
        self.events.append("staged")

    def mark_committed(self, *_args: object, **_kwargs: object) -> None:
        self.events.append("committed")

    def mark_failed(self, *_args: object, **_kwargs: object) -> None:
        self.events.append("failed")


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={"sink_type": "mssql"},
    )


def _nested_options(*, materialization: str = "memory", spill_output_dir: Path | None = None):
    return NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "materialization": materialization,
            "spill_output_dir": str(spill_output_dir) if spill_output_dir else None,
            "split_paths": [
                {
                    "path": "$.items[*]",
                    "table": "order_lines",
                    "unique_key": ["order_id", "sku"],
                }
            ],
            "child_quality": {
                "duplicate_child_key": "fail",
                "orphan_child_rows": "fail",
            },
        }
    )


def test_empty_containers_survive_round_trip_without_fake_child_rows() -> None:
    source = [{"order_id": 1, "empty_items": [], "empty_attributes": {}}]

    result = NestedNormalizationService().normalize_rows(
        source,
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        unique_key=["order_id"],
    )

    assert [table.name for table in result.tables] == ["orders"]
    assert result.table("orders").rows[0]["empty_items"] == []
    assert result.table("orders").rows[0]["empty_attributes"] == {}
    assert HierarchyBuilderService().rebuild(result, root_table="orders") == source


def test_ascii_generated_table_spelling_remains_stable() -> None:
    result = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "OrderItems": [{"sku": "A"}]}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
    )

    assert "orders__OrderItems" in result.as_mapping()


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"a-b": [{"value": 1}], "a_b": [{"value": 2}]}, "a-b"),
        ({"Items": [{"value": 1}], "items": [{"value": 2}]}, "Items"),
    ],
)
def test_generated_table_collision_fails_with_both_source_paths(
    row: dict[str, object],
    expected: str,
) -> None:
    with pytest.raises(ValueError, match="generated table collision") as raised:
        NestedNormalizationService().normalize_rows(
            [{"order_id": 1, **row}],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
        )

    assert expected in str(raised.value)


def test_non_ascii_generated_physical_identifier_fails_with_mapping_guidance() -> None:
    with pytest.raises(ValueError, match="non-ASCII.*explicit.*mapping"):
        NestedNormalizationService().normalize_rows(
            [{"order_id": 1, "café": [{"value": 1}]}],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
        )


def test_default_generated_table_retains_configured_child_unique_key() -> None:
    result = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}]}],
        root_table="orders",
        options=NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "split_paths": [{"path": "$.items[*]", "unique_key": ["sku"]}],
            }
        ),
    )

    assert result.table_keys() == {"orders__items": ("sku",)}
    assert result.table_parents() == {"orders__items": "orders"}


def test_spill_registry_rejects_cross_row_case_collision(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="generated table collision"):
        SpillToDiskNormalizationService().spill_rows(
            [
                {"order_id": 1, "Items": [{"value": 1}]},
                {"order_id": 2, "items": [{"value": 2}]},
            ],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
            output_dir=tmp_path,
        )


def test_runtime_collision_fails_before_audit_or_sink_mutation() -> None:
    identity = _RecordingIdentity()
    sink = RecordingStagedSink()

    with pytest.raises(ValueError, match="generated table collision"):
        NestedLoadService(load_identity_service=identity).load(
            load_config=_load_config(),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"order_id": 1, "a-b": [{"value": 1}], "a_b": [{"value": 2}]}]),
                schema=[("order_id", "integer")],
            ),
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=NestedNormalizationOptions(enabled=True),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert identity.events == []
    assert sink.staged_tables == []


@pytest.mark.parametrize("materialization", ["memory", "spill_to_disk"])
def test_repeated_unkeyed_children_under_distinct_roots_do_not_conflict(
    materialization: str,
    tmp_path: Path,
) -> None:
    identity = _RecordingIdentity()
    sink = RecordingStagedSink()
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact(
            [
                {"order_id": 1, "items": [{"sku": "A"}]},
                {"order_id": 2, "items": [{"sku": "A"}]},
            ]
        ),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "materialization": materialization,
            "spill_output_dir": str(tmp_path / "spill"),
            "child_quality": {
                "duplicate_child_key": "skip",
                "orphan_child_rows": "skip",
            },
        }
    )

    result = NestedLoadService(load_identity_service=identity).load(
        load_config=_load_config(),
        payload=payload,
        extract_result=SimpleNamespace(),
        load_record=_Audit(),
        lineage_options=LineageOptions(enabled=True),
        nested_options=options,
        package_mutation=sink.mutation(),
        prepare_member=identity_prepare,
        evaluate_package=identity_evaluate,
    )

    assert identity.events == ["staged", "committed"]
    assert set(sink.staged_tables) == {"orders", "orders__items"}
    quality = result.reconciliation_metrics["nested_normalization"]["child_quality"]
    assert quality[0]["duplicate_row_ids"] == 0
    assert quality[0]["row_identity_status"] == "passed"


def test_configured_duplicate_key_warns_without_triggering_unkeyed_identity_failure() -> None:
    class _KeyRecordingSink(RecordingStagedSink):
        def __init__(self) -> None:
            super().__init__()
            self.keys: dict[str, list[str]] = {}

        def stage_payload(self, load_config: LoadConfig, payload: object):
            self.keys[load_config.target_table] = list(load_config.unique_key or ())
            return super().stage_payload(load_config, payload)

    identity = _RecordingIdentity()
    sink = _KeyRecordingSink()
    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "split_paths": [{"path": "$.items[*]", "unique_key": ["sku"]}],
            "child_quality": {
                "duplicate_child_key": "warn",
                "orphan_child_rows": "fail",
            },
        }
    )
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}, {"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )

    with pytest.warns(UserWarning, match="duplicate_child_key=1"):
        result = NestedLoadService(load_identity_service=identity).load(
            load_config=_load_config(),
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=options,
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert sink.keys["orders__items"] == ["sku"]
    assert identity.events == ["staged", "committed"]
    quality = result.reconciliation_metrics["nested_normalization"]["child_quality"]
    assert quality[0]["status"] == "warning"
    assert quality[0]["row_identity_status"] == "passed"


def test_table_load_config_uses_resolved_default_child_key() -> None:
    child_config = table_load_config(
        _load_config(),
        "orders__items",
        NestedNormalizationOptions(enabled=True),
        resolved_child_unique_key=("sku",),
    )

    assert child_config.unique_key == ["sku"]


@pytest.mark.parametrize("materialization", ["memory", "spill_to_disk"])
@pytest.mark.parametrize("sku", ["A", None])
def test_duplicate_child_keys_fail_before_snapshot_audit_or_sink_mutation(
    materialization: str,
    sku: str | None,
    tmp_path: Path,
) -> None:
    identity = _RecordingIdentity()
    sink = RecordingStagedSink()
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact(
            [
                {"order_id": 1, "items": [{"sku": sku}]},
                {"order_id": 1, "items": [{"sku": sku}]},
            ]
        ),
        schema=[("order_id", "integer"), ("items", "json")],
    )

    with pytest.raises(ChildQualityViolationError, match="duplicate_child_key"):
        NestedLoadService(load_identity_service=identity).load(
            load_config=_load_config(),
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=_nested_options(
                materialization=materialization,
                spill_output_dir=tmp_path / "spill",
            ),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert identity.events == []
    assert sink.staged_tables == []


def test_orphan_child_fails_before_snapshot_audit_or_sink_mutation() -> None:
    root = {
        "__dpone__row_id": "root-1",
        "__dpone__root_row_id": "root-1",
        "__dpone__parent_row_id": None,
    }
    orphan = {
        "sku": "A",
        "__dpone__row_id": "line-1",
        "__dpone__root_row_id": "root-1",
        "__dpone__parent_row_id": "missing-parent",
    }
    result = NormalizationResult(
        (
            NormalizedTable("orders", (), (root,)),
            NormalizedTable("order_lines", (), (orphan,)),
        ),
        child_unique_keys=(("order_lines", ("sku",)),),
    )

    class _InvalidNormalizer:
        def normalize_payload(self, *_args: object, **_kwargs: object) -> NormalizationResult:
            return result

    identity = _RecordingIdentity()
    sink = RecordingStagedSink()
    with pytest.raises(ChildQualityViolationError, match="orphan_child_rows"):
        NestedLoadService(
            normalization_service=_InvalidNormalizer(),  # type: ignore[arg-type]
            load_identity_service=identity,
        ).load(
            load_config=_load_config(),
            payload=LoadPayload(
                artifact=InMemoryRowsArtifact([{"order_id": 1}]),
                schema=[("order_id", "integer")],
            ),
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=_nested_options(),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert identity.events == []
    assert sink.staged_tables == []


def test_certification_reverse_readback_compares_complete_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _IncompleteBuilder:
        def rebuild(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            return [{"order_id": 1, "items": [{"sku": "WRONG"}]}]

    monkeypatch.setattr(
        certification_checks,
        "HierarchyBuilderService",
        _IncompleteBuilder,
    )

    assert (
        certification_checks.reverse_readback_passes(
            [{"order_id": 1, "items": [{"sku": "A"}]}],
            root_table="orders",
        )
        is False
    )
