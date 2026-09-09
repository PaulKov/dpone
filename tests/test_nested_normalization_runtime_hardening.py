from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.runtime.normalization.child_quality import (
    ChildQualityService,
    ChildQualityViolationError,
)
from dpone.runtime.normalization.fast_path import NestedSpillFastPathPlanner
from dpone.runtime.normalization.finalizers import ChildDeleteFinalizerService
from dpone.runtime.normalization.options import NestedChildQualityOptions, NestedNormalizationOptions
from dpone.runtime.normalization.snapshot_factory import ChildSnapshotStoreFactory
from dpone.runtime.normalization.snapshot_sql import SqlChildSnapshotStore
from dpone.runtime.normalization.snapshot_store import JsonFileChildSnapshotStore
from dpone.runtime.normalization.spill import SpillToDiskNormalizationService


def test_runtime_hardening_services_are_public_normalization_exports() -> None:
    import dpone.runtime.normalization as normalization

    assert normalization.ChildDeleteFinalizerService is ChildDeleteFinalizerService
    assert normalization.JsonFileChildSnapshotStore is JsonFileChildSnapshotStore
    assert normalization.SqlChildSnapshotStore is SqlChildSnapshotStore
    assert normalization.ChildSnapshotStoreFactory is ChildSnapshotStoreFactory
    assert normalization.NestedSpillFastPathPlanner is NestedSpillFastPathPlanner
    assert normalization.SpillToDiskNormalizationService is SpillToDiskNormalizationService
    assert normalization.ChildQualityViolationError.__name__ == "ChildQualityViolationError"


def test_mssql_child_delete_finalizer_uses_staging_join_sql() -> None:
    plan = ChildDeleteFinalizerService().plan_delete(
        dialect="mssql",
        target_table="landing.order_lines",
        deleted_keys_staging_table="staging.order_lines_deleted_keys",
        unique_key=["order_id", "sku"],
    )

    assert plan.operation == "child_delete"
    assert plan.dialect == "mssql"
    assert plan.statements == (
        "DELETE target\n"
        "FROM landing.order_lines AS target\n"
        "INNER JOIN staging.order_lines_deleted_keys AS deleted_keys\n"
        "  ON target.[order_id] = deleted_keys.[order_id] AND target.[sku] = deleted_keys.[sku];",
    )
    assert plan.requires_staging is True


def test_clickhouse_child_delete_finalizer_defaults_to_lightweight_delete() -> None:
    plan = ChildDeleteFinalizerService().plan_delete(
        dialect="clickhouse",
        target_table="landing.order_lines",
        deleted_keys_staging_table="staging.order_lines_deleted_keys",
        unique_key=["order_id", "sku"],
    )

    assert "DELETE FROM landing.order_lines" in plan.statements[0]
    assert "tuple(order_id, sku) IN" in plan.statements[0]
    assert plan.warnings == ("ClickHouse lightweight deletes are asynchronous until background cleanup completes.",)


def test_kafka_child_delete_finalizer_emits_keyed_delete_events_contract() -> None:
    plan = ChildDeleteFinalizerService().plan_delete(
        dialect="kafka",
        target_table="orders.order_lines",
        deleted_keys_staging_table="staging.order_lines_deleted_keys",
        unique_key=["order_id", "sku"],
    )

    assert plan.operation == "child_delete_events"
    assert plan.statements == ()
    assert plan.event_contract == {
        "op": "delete",
        "key_columns": ["order_id", "sku"],
        "source": "staging.order_lines_deleted_keys",
        "target": "orders.order_lines",
    }


def test_json_child_snapshot_store_advances_only_after_commit(tmp_path: Path) -> None:
    store = JsonFileChildSnapshotStore(tmp_path / "child_snapshots.json")
    old_keys = [{"order_id": 1, "sku": "A"}, {"order_id": 1, "sku": "B"}]
    new_keys = [{"order_id": 1, "sku": "A"}]

    store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=old_keys,
        load_id="01JZ0000000000000000000001",
    )
    assert store.load_committed(root_table="orders", child_table="order_lines") == []
    store.commit_snapshot(root_table="orders", child_table="order_lines", load_id="01JZ0000000000000000000001")
    assert store.load_committed(root_table="orders", child_table="order_lines") == old_keys

    store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=new_keys,
        load_id="01JZ0000000000000000000002",
    )
    assert store.load_committed(root_table="orders", child_table="order_lines") == old_keys
    store.rollback_staged(root_table="orders", child_table="order_lines", load_id="01JZ0000000000000000000002")
    assert store.load_committed(root_table="orders", child_table="order_lines") == old_keys


def test_spill_to_disk_supports_tsv_fast_path_metadata(tmp_path: Path) -> None:
    result = SpillToDiskNormalizationService().spill_rows(
        [{"order_id": 1, "items": [{"sku": "A", "qty": 2}]}],
        root_table="orders",
        options=NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
            }
        ),
        output_dir=tmp_path,
        output_format="tsv",
    )

    assert result.formats["orders"] == "tsv"
    assert result.files["orders"].suffix == ".tsv"
    assert result.files["order_lines"].suffix == ".tsv"
    assert result.files["order_lines"].read_text(encoding="utf-8").splitlines()[0].startswith("sku\tqty\torder_id")


def test_spill_to_disk_supports_clickhouse_json_each_row_fast_path(tmp_path: Path) -> None:
    result = SpillToDiskNormalizationService().spill_rows(
        [{"order_id": 1, "items": [{"sku": "A"}]}],
        root_table="orders",
        options=NestedNormalizationOptions.from_config({"enabled": True}),
        output_dir=tmp_path,
        output_format="json_each_row",
    )

    assert result.formats["orders"] == "json_each_row"
    row = json.loads(result.files["orders"].read_text(encoding="utf-8").splitlines()[0])
    assert row["order_id"] == 1


def test_spill_to_disk_rejects_unsafe_tsv_values(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsafe delimiter"):
        SpillToDiskNormalizationService().spill_rows(
            [{"order_id": 1, "note": "bad\tvalue"}],
            root_table="orders",
            options=NestedNormalizationOptions.from_config({"enabled": True}),
            output_dir=tmp_path,
            output_format="tsv",
        )


def test_child_quality_uses_all_hierarchy_row_ids_for_intermediate_parents() -> None:
    rows_by_table = {
        "orders": [
            {
                "__dpone__row_id": "root-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": None,
            }
        ],
        "orders__items": [
            {
                "sku": "A",
                "__dpone__row_id": "item-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "root-1",
            }
        ],
        "orders__items__discounts": [
            {
                "code": "VIP",
                "__dpone__row_id": "discount-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "item-1",
            }
        ],
    }

    results = ChildQualityService().evaluate_package(
        root_table="orders",
        table_rows=rows_by_table,
        table_keys={},
        options=NestedChildQualityOptions(),
    )

    assert [result.orphan_child_rows for result in results] == [0, 0]
    assert [result.orphan_child_rows_status for result in results] == ["passed", "passed"]


@pytest.mark.parametrize(
    ("parent_id", "child_root_id"),
    [
        ("payment-1", "root-1"),
        ("item-2", "root-1"),
    ],
)
def test_child_quality_rejects_wrong_parent_table_or_root(
    parent_id: str,
    child_root_id: str,
) -> None:
    rows_by_table = {
        "orders": [
            {
                "__dpone__row_id": "root-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": None,
            },
            {
                "__dpone__row_id": "root-2",
                "__dpone__root_row_id": "root-2",
                "__dpone__parent_row_id": None,
            },
        ],
        "orders__items": [
            {
                "__dpone__row_id": "item-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "root-1",
            },
            {
                "__dpone__row_id": "item-2",
                "__dpone__root_row_id": "root-2",
                "__dpone__parent_row_id": "root-2",
            },
        ],
        "orders__payments": [
            {
                "__dpone__row_id": "payment-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "root-1",
            }
        ],
        "orders__items__discounts": [
            {
                "__dpone__row_id": "discount-1",
                "__dpone__root_row_id": child_root_id,
                "__dpone__parent_row_id": parent_id,
            }
        ],
    }

    with pytest.raises(ChildQualityViolationError, match="orphan_child_rows"):
        ChildQualityService().evaluate_package(
            root_table="orders",
            table_rows=rows_by_table,
            table_keys={},
            table_parents={
                "orders__items": "orders",
                "orders__payments": "orders",
                "orders__items__discounts": "orders__items",
            },
            options=NestedChildQualityOptions(
                duplicate_child_key="skip",
                orphan_child_rows="fail",
            ),
        )


def test_child_quality_warn_and_skip_are_explicit_non_pass_statuses() -> None:
    rows_by_table = {
        "orders": [
            {
                "__dpone__row_id": "root-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": None,
            }
        ],
        "order_lines": [
            {
                "sku": "A",
                "__dpone__row_id": "line-1",
                "__dpone__root_row_id": "missing-root",
                "__dpone__parent_row_id": "missing-parent",
            },
            {
                "sku": "A",
                "__dpone__row_id": "line-2",
                "__dpone__root_row_id": "missing-root",
                "__dpone__parent_row_id": "missing-parent",
            },
        ],
    }

    with pytest.warns(UserWarning, match="child-quality warning"):
        (warning_result,) = ChildQualityService().evaluate_package(
            root_table="orders",
            table_rows=rows_by_table,
            table_keys={"order_lines": ("sku",)},
            options=NestedChildQualityOptions(
                duplicate_child_key="warn",
                orphan_child_rows="warn",
            ),
        )
    (skipped_result,) = ChildQualityService().evaluate_package(
        root_table="orders",
        table_rows=rows_by_table,
        table_keys={"order_lines": ("sku",)},
        options=NestedChildQualityOptions(
            duplicate_child_key="skip",
            orphan_child_rows="skip",
        ),
    )

    assert warning_result.status == "warning"
    assert warning_result.duplicate_child_key_status == "warning"
    assert warning_result.orphan_child_rows_status == "warning"
    assert skipped_result.status == "partial"
    assert skipped_result.duplicate_child_keys is None
    assert skipped_result.orphan_child_rows is None
    assert skipped_result.row_identity_status == "passed"


def test_child_quality_rejects_unknown_policy_at_python_boundary() -> None:
    with pytest.raises(ValueError, match="expected fail, warn, or skip"):
        NestedChildQualityOptions(duplicate_child_key="continue")


def test_child_quality_requires_row_id_even_with_configured_business_key() -> None:
    with pytest.raises(ChildQualityViolationError, match="missing_row_id=1"):
        ChildQualityService().evaluate_package(
            root_table="orders",
            table_rows={
                "orders": [
                    {
                        "__dpone__row_id": "root-1",
                        "__dpone__root_row_id": "root-1",
                        "__dpone__parent_row_id": None,
                    }
                ],
                "order_lines": [
                    {
                        "sku": "A",
                        "__dpone__root_row_id": "root-1",
                        "__dpone__parent_row_id": "root-1",
                    }
                ],
            },
            table_keys={"order_lines": ("sku",)},
            options=NestedChildQualityOptions(
                duplicate_child_key="skip",
                orphan_child_rows="skip",
            ),
        )


def test_child_quality_enforces_identity_for_ignored_raw_landing_table() -> None:
    with pytest.raises(ChildQualityViolationError, match="duplicate_row_id=1"):
        ChildQualityService().evaluate_package(
            root_table="orders",
            table_rows={
                "orders": [
                    {
                        "__dpone__row_id": "root-1",
                        "__dpone__root_row_id": "root-1",
                        "__dpone__parent_row_id": None,
                    }
                ],
                "orders__raw": [
                    {
                        "__dpone__row_id": "raw-1",
                        "__dpone__root_row_id": "root-1",
                        "__dpone__parent_row_id": "root-1",
                    },
                    {
                        "__dpone__row_id": "raw-1",
                        "__dpone__root_row_id": "root-1",
                        "__dpone__parent_row_id": "root-1",
                    },
                ],
            },
            table_keys={},
            ignored_hierarchy_tables=("orders__raw",),
            options=NestedChildQualityOptions(
                duplicate_child_key="skip",
                orphan_child_rows="skip",
            ),
        )
