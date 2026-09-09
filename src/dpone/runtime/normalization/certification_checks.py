"""Runtime-owned nested normalization certification checks."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from dpone.runtime.normalization.child_quality import (
    ChildQualityService,
    ChildQualityViolationError,
)
from dpone.runtime.normalization.fast_path import NestedSpillFastPathPlanner
from dpone.runtime.normalization.finalizers import ChildDeleteFinalizerService
from dpone.runtime.normalization.hierarchy_builder import HierarchyBuilderService
from dpone.runtime.normalization.normalizer import NestedNormalizationService
from dpone.runtime.normalization.options import NestedChildQualityOptions, NestedNormalizationOptions
from dpone.runtime.normalization.reconciliation import ChildTableReconciliationService
from dpone.runtime.normalization.snapshot_sql import SqlChildSnapshotStore
from dpone.runtime.normalization.snapshot_store import JsonFileChildSnapshotStore
from dpone.runtime.normalization.spill import SpillToDiskNormalizationService


def child_options() -> NestedNormalizationOptions:
    return NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "split_paths": [
                {
                    "path": "$.items[*]",
                    "table": "order_lines",
                    "unique_key": ["order_id", "sku"],
                    "delete_policy": "replace_parent_children",
                }
            ],
            "guardrails": {"max_array_length": 16},
        }
    )


def child_identity_is_stable(*, root_table: str) -> bool:
    options = child_options()
    normalizer = NestedNormalizationService()
    first = normalizer.normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "B"}]}],
        root_table=root_table,
        options=options,
        unique_key=["order_id"],
    )
    second = normalizer.normalize_rows(
        [{"order_id": 1, "items": [{"sku": "B"}, {"sku": "A"}]}],
        root_table=root_table,
        options=options,
        unique_key=["order_id"],
    )
    first_rows = {row["sku"]: row["__dpone__row_id"] for row in first.table("order_lines").rows}
    second_rows = {row["sku"]: row["__dpone__row_id"] for row in second.table("order_lines").rows}
    type_distinct = normalizer.normalize_rows(
        [
            {"order_id": 2, "items": [{"sku": date(2026, 7, 30)}]},
            {"order_id": 2, "items": [{"sku": "2026-07-30"}]},
        ],
        root_table=root_table,
        options=options,
        unique_key=["order_id"],
    )
    typed_ids = {row["__dpone__row_id"] for row in type_distinct.table("order_lines").rows}
    return first_rows == second_rows and len(typed_ids) == 2


def child_delete_diff_passes(*, root_table: str) -> bool:
    options = child_options()
    normalizer = NestedNormalizationService()
    previous = normalizer.normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "B"}, {"sku": "C"}]}],
        root_table=root_table,
        options=options,
        unique_key=["order_id"],
    )
    current = normalizer.normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "C"}]}],
        root_table=root_table,
        options=options,
        unique_key=["order_id"],
    )
    plan = ChildTableReconciliationService().diff_results(previous, current, options=options)
    return plan.tables["order_lines"].deleted_keys == ({"order_id": 1, "sku": "B"},)


def child_finalizers_pass(*, root_table: str) -> bool:
    service = ChildDeleteFinalizerService()
    mssql = service.plan_delete(
        dialect="mssql",
        target_table=f"landing.{root_table}__order_lines",
        deleted_keys_staging_table=f"staging.{root_table}__order_lines_deleted_keys",
        unique_key=["order_id", "sku"],
    )
    clickhouse = service.plan_delete(
        dialect="clickhouse",
        target_table=f"landing.{root_table}__order_lines",
        deleted_keys_staging_table=f"staging.{root_table}__order_lines_deleted_keys",
        unique_key=["order_id", "sku"],
    )
    kafka = service.plan_delete(
        dialect="kafka",
        target_table=f"{root_table}.order_lines",
        deleted_keys_staging_table=f"staging.{root_table}__order_lines_deleted_keys",
        unique_key=["order_id", "sku"],
    )
    return (
        "DELETE target" in "\n".join(mssql.statements)
        and "tuple(order_id, sku) IN" in "\n".join(clickhouse.statements)
        and kafka.event_contract
        == {
            "op": "delete",
            "key_columns": ["order_id", "sku"],
            "source": f"staging.{root_table}__order_lines_deleted_keys",
            "target": f"{root_table}.order_lines",
        }
    )


def snapshot_store_passes(*, output_dir: Path) -> bool:
    store = JsonFileChildSnapshotStore(output_dir / "child_snapshots.json")
    first = ({"order_id": 1, "sku": "A"},)
    second = ({"order_id": 1, "sku": "B"},)
    store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=first,
        load_id="load-1",
    )
    if store.load_committed(root_table="orders", child_table="order_lines"):
        return False
    store.commit_snapshot(root_table="orders", child_table="order_lines", load_id="load-1")
    store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=second,
        load_id="load-2",
    )
    store.rollback_staged(root_table="orders", child_table="order_lines", load_id="load-2")
    committed = store.load_committed(root_table="orders", child_table="order_lines")
    return committed == list(first)


def sql_snapshot_store_passes() -> bool:
    class _Executor:
        def __init__(self) -> None:
            self.statements: list[tuple[str, dict[str, object]]] = []

        def execute(self, statement: str, params: dict[str, object]) -> None:
            self.statements.append((statement, params))

        def fetch_value(self, statement: str, params: dict[str, object]) -> object:
            self.statements.append((statement, params))
            return '[{"order_id": 1, "sku": "A"}]'

    executor = _Executor()
    store = SqlChildSnapshotStore(dialect="postgres", table="etl_state.__dpone__child_snapshots", executor=executor)
    store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=({"order_id": 1, "sku": "A"},),
        load_id="load-1",
    )
    committed = store.load_committed(root_table="orders", child_table="order_lines")
    store.commit_snapshot(root_table="orders", child_table="order_lines", load_id="load-1")
    rendered = "\n".join(statement for statement, _params in executor.statements)
    return committed == [{"order_id": 1, "sku": "A"}] and "INSERT INTO etl_state.__dpone__child_snapshots" in rendered


def native_spill_passes(*, output_dir: Path, root_table: str) -> bool:
    result = SpillToDiskNormalizationService().spill_rows(
        [{"order_id": 1, "items": [{"sku": "A", "qty": 1}]}],
        root_table=root_table,
        options=child_options(),
        output_dir=output_dir / "native_spill",
        unique_key=["order_id"],
        output_format="tsv",
    )
    return result.formats.get(root_table) == "tsv" and result.files[root_table].suffix == ".tsv"


def native_fast_path_passes() -> bool:
    planner = NestedSpillFastPathPlanner()
    return all(
        planner.plan(sink_type=sink_type, spill_format=spill_format).artifact_kind == "streaming"
        for sink_type, spill_format in (
            ("mssql", "tsv"),
            ("postgres", "tsv"),
            ("clickhouse", "json_each_row"),
            ("clickhouse", "tsv"),
            ("bigquery", "jsonl"),
            ("kafka", "ndjson"),
        )
    )


def child_quality_passes(*, root_table: str) -> bool:
    service = ChildQualityService()
    options = NestedChildQualityOptions()
    valid_tables: dict[str, list[dict[str, object]]] = {
        root_table: [
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
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "root-1",
            }
        ],
    }
    (valid,) = service.evaluate_package(
        root_table=root_table,
        table_rows=valid_tables,
        table_keys={"order_lines": ("sku",)},
        table_parents={"order_lines": root_table},
        options=options,
    )
    duplicate_identity_tables: dict[str, list[dict[str, object]]] = {
        **valid_tables,
        "order_lines": [
            {
                **valid_tables["order_lines"][0],
                "sku": "A",
            },
            {
                **valid_tables["order_lines"][0],
                "sku": "B",
            },
        ],
    }
    try:
        service.evaluate_package(
            root_table=root_table,
            table_rows=duplicate_identity_tables,
            table_keys={},
            table_parents={"order_lines": root_table},
            options=options,
        )
    except ChildQualityViolationError:
        duplicate_identity_rejected = True
    else:
        duplicate_identity_rejected = False
    orphan_tables: dict[str, list[dict[str, object]]] = {
        **valid_tables,
        "order_lines": [
            {
                **valid_tables["order_lines"][0],
                "__dpone__parent_row_id": "missing-parent",
            }
        ],
    }
    try:
        service.evaluate_package(
            root_table=root_table,
            table_rows=orphan_tables,
            table_keys={"order_lines": ("sku",)},
            table_parents={"order_lines": root_table},
            options=options,
        )
    except ChildQualityViolationError:
        orphan_rejected = True
    else:
        orphan_rejected = False
    return valid.status == "passed" and duplicate_identity_rejected and orphan_rejected


def child_schema_evolution_contract() -> dict[str, object]:
    return {
        "status": "unverified",
        "reason": (
            "contract metadata is documented, but this local certification "
            "does not execute destination schema evolution"
        ),
        "safe_changes": ["add_nullable_column", "widen_type", "framework_columns"],
        "breaking_changes": ["drop", "rename", "narrowing", "nullable_to_not_null_without_default"],
        "type_conflict_policy": "__dpone__nc__<column>",
        "scope": "root_and_generated_child_tables",
    }


def native_fast_path_execution_contract() -> dict[str, object]:
    return {
        "status": "unverified",
        "reason": ("sink-native writer/loader wire contracts are not certified; typed streaming remains fail-closed"),
        "mssql": "typed_streaming_fallback",
        "postgres": "typed_streaming_fallback",
        "clickhouse": "typed_streaming_fallback",
        "bigquery": "typed_streaming_fallback",
        "kafka": "typed_streaming_fallback",
    }


def reverse_readback_passes(rows: list[dict[str, Any]], *, root_table: str) -> bool:
    options = NestedNormalizationOptions.from_config({"enabled": True, "guardrails": {"max_array_length": 16}})
    result = NestedNormalizationService().normalize_rows(
        rows[:10],
        root_table=root_table,
        options=options,
        unique_key=["order_id"],
    )
    rebuilt = HierarchyBuilderService().rebuild(result, root_table=root_table)
    return rebuilt == rows[:10]


__all__ = [
    "child_delete_diff_passes",
    "child_finalizers_pass",
    "child_identity_is_stable",
    "child_options",
    "child_quality_passes",
    "child_schema_evolution_contract",
    "native_fast_path_execution_contract",
    "native_fast_path_passes",
    "native_spill_passes",
    "reverse_readback_passes",
    "snapshot_store_passes",
    "sql_snapshot_store_passes",
]
