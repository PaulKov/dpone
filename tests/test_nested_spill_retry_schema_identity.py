from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from pathlib import Path
from threading import Barrier

import pytest

from dpone.runtime.etl.nested_spill_rows import semantic_rows as _load_semantic_rows
from dpone.runtime.etl.nested_spill_rows import spilled_rows as _spilled_rows
from dpone.runtime.normalization.child_quality import ChildQualityService, ChildQualityViolationError
from dpone.runtime.normalization.normalizer import NestedNormalizationService
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.spill import SpilledNormalizationResult, SpillToDiskNormalizationService
from dpone.runtime.normalization.spill_child_quality import SpilledChildQualityService

_LOAD_ID = "01JZ0000000000000000000001"
_IDENTITY_COLUMNS = (
    "__dpone__row_id",
    "__dpone__parent_row_id",
    "__dpone__root_row_id",
    "__dpone__list_index",
)


def test_normalize_rows_does_not_expose_caller_controlled_identity_offset() -> None:
    assert "row_index_offset" not in signature(NestedNormalizationService.normalize_rows).parameters


def test_spill_retry_publishes_a_new_generation_instead_of_appending(tmp_path: Path) -> None:
    service = SpillToDiskNormalizationService()
    options = NestedNormalizationOptions(enabled=True)

    first = service.spill_rows(
        [{"order_id": 1}],
        root_table="orders",
        options=options,
        output_dir=tmp_path,
        load_id=_LOAD_ID,
    )
    retried = service.spill_rows(
        [{"order_id": 2}],
        root_table="orders",
        options=options,
        output_dir=tmp_path,
        load_id=_LOAD_ID,
    )

    assert first.files["orders"] != retried.files["orders"]
    assert [row["order_id"] for row in _jsonl_rows(first.files["orders"])] == [1]
    assert [row["order_id"] for row in _jsonl_rows(retried.files["orders"])] == [2]


def test_failed_spill_retry_preserves_previous_commit_and_removes_partial_attempt(tmp_path: Path) -> None:
    service = SpillToDiskNormalizationService()
    committed = service.spill_rows(
        [{"order_id": 1}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        load_id=_LOAD_ID,
    )
    committed_bytes = committed.files["orders"].read_bytes()

    with pytest.raises(RuntimeError, match="source retry failed"):
        service.spill_rows(
            _rows_failing_after_one(),
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
            output_dir=tmp_path,
            load_id=_LOAD_ID,
        )

    assert committed.files["orders"].read_bytes() == committed_bytes
    assert not any(path.is_dir() for path in tmp_path.glob(".dpone-spill-stage-*"))


def test_empty_retry_preserves_previous_generation_and_unrelated_files(tmp_path: Path) -> None:
    unrelated = tmp_path / "keep.txt"
    unrelated.write_text("user-owned", encoding="utf-8")
    first = SpillToDiskNormalizationService().spill_rows(
        [{"order_id": 1, "items": [{"sku": "A"}]}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        load_id=_LOAD_ID,
        output_format="tsv",
    )
    first_bytes = {name: path.read_bytes() for name, path in first.files.items()}
    generations_before = set(tmp_path.glob(".dpone-spill-generation-*"))

    empty = SpillToDiskNormalizationService().spill_rows(
        [],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        load_id=_LOAD_ID,
        output_format="tsv",
    )

    assert empty.files == {}
    assert {name: path.read_bytes() for name, path in first.files.items()} == first_bytes
    assert all(path.exists() for path in first.semantic_files.values())
    assert unrelated.read_text(encoding="utf-8") == "user-owned"
    assert set(tmp_path.glob(".dpone-spill-generation-*")) == generations_before


def test_invalid_output_format_does_not_leave_spill_directories(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsupported nested spill output_format"):
        SpillToDiskNormalizationService().spill_rows(
            [],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
            output_dir=tmp_path,
            load_id=_LOAD_ID,
            output_format="invalid",
        )

    assert not any(tmp_path.glob(".dpone-spill-stage-*"))
    assert not any(tmp_path.glob(".dpone-spill-generation-*"))


def test_existing_unowned_target_is_preserved_outside_generation(tmp_path: Path) -> None:
    unowned = tmp_path / "orders.tsv"
    unowned.write_bytes(b"user-owned\n")

    result = SpillToDiskNormalizationService().spill_rows(
        [{"order_id": 1}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        load_id=_LOAD_ID,
        output_format="tsv",
    )

    assert unowned.read_bytes() == b"user-owned\n"
    assert result.files["orders"] != unowned
    assert list(_spilled_rows(result.files["orders"], "tsv", result.schemas["orders"]))[0]["order_id"] == 1
    assert not (tmp_path / ".dpone-spill-manifest.json").exists()
    assert not any(path.is_dir() for path in tmp_path.glob(".dpone-spill-stage-*"))


def test_concurrent_spills_return_distinct_immutable_generations(tmp_path: Path) -> None:
    barrier = Barrier(2)

    def spill(value: int) -> SpilledNormalizationResult:
        def rows() -> Iterator[Mapping[str, object]]:
            barrier.wait()
            yield {"order_id": value}

        return SpillToDiskNormalizationService().spill_rows(
            rows(),
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
            output_dir=tmp_path,
            load_id=_LOAD_ID,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(spill, 1)
        second_future = executor.submit(spill, 2)
        first = first_future.result()
        second = second_future.result()

    assert first.files["orders"] != second.files["orders"]
    assert [row["order_id"] for row in _jsonl_rows(first.files["orders"])] == [1]
    assert [row["order_id"] for row in _jsonl_rows(second.files["orders"])] == [2]


def test_tsv_schema_growth_preserves_later_columns_and_scalar_types(tmp_path: Path) -> None:
    result = SpillToDiskNormalizationService().spill_rows(
        [
            {"order_id": 1, "items": [{"sku": "A"}]},
            {"order_id": 2, "items": [{"sku": "B", "qty": 2, "active": True, "note": None}]},
        ],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        load_id=_LOAD_ID,
        output_format="tsv",
    )
    schema = result.schemas["orders__items"]

    rows = list(_spilled_rows(result.files["orders__items"], "tsv", schema))

    assert {"qty", "active", "note"} <= {name for name, _logical_type in schema}
    assert rows[0]["qty"] is None
    assert rows[0]["active"] is None
    assert rows[1]["qty"] == 2
    assert rows[1]["active"] is True
    assert rows[1]["note"] is None


def test_tsv_quality_uses_typed_semantic_rows_for_numeric_duplicates(tmp_path: Path) -> None:
    result, options = _spilled_key_rows(tmp_path, (1, 1.0))

    for table_rows in (_semantic_rows(result), _native_rows(result)):
        with pytest.raises(ChildQualityViolationError, match="duplicate_child_key=1"):
            SpilledChildQualityService().evaluate_package(
                root_table="orders",
                table_rows=table_rows,
                table_keys=result.table_keys,
                table_parents=result.table_parents,
                options=options.child_quality,
                work_dir=tmp_path,
            )


def test_tsv_incompatible_key_types_fail_instead_of_collapsing_identity(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot represent schema types losslessly"):
        _spilled_key_rows(tmp_path, (1, "1"))


def test_tsv_reserved_null_marker_string_fails_instead_of_becoming_null(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="reserved null marker"):
        SpillToDiskNormalizationService().spill_rows(
            [{"note": r"\N"}],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
            output_dir=tmp_path,
            output_format="tsv",
        )


@pytest.mark.parametrize("unsafe_table", ["../escaped", "nested/escaped", r"nested\escaped", ".", ".."])
def test_spill_rejects_table_paths_that_are_not_single_safe_components(
    tmp_path: Path,
    unsafe_table: str,
) -> None:
    with pytest.raises(ValueError, match="safe single filename component"):
        SpillToDiskNormalizationService().spill_rows(
            [{"items": [{"sku": "A"}]}],
            root_table="orders",
            options=NestedNormalizationOptions.from_config(
                {
                    "enabled": True,
                    "split_paths": [{"path": "$.items[*]", "table": unsafe_table}],
                }
            ),
            output_dir=tmp_path,
            load_id=_LOAD_ID,
        )

    assert not (tmp_path / ".dpone-spill-manifest.json").exists()
    assert not any(path.is_dir() for path in tmp_path.glob(".dpone-spill-stage-*"))


def test_repeated_unkeyed_rows_have_memory_spill_identity_parity(tmp_path: Path) -> None:
    source_rows = [
        {"items": [{"sku": "A"}]},
        {"items": [{"sku": "A"}]},
    ]
    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "raw_landing": {"enabled": True},
            "child_quality": {
                "duplicate_child_key": "skip",
                "orphan_child_rows": "fail",
            },
        }
    )
    memory = NestedNormalizationService().normalize_rows(
        source_rows,
        root_table="orders",
        options=options,
        load_id=_LOAD_ID,
    )
    spill = SpillToDiskNormalizationService().spill_rows(
        source_rows,
        root_table="orders",
        options=options,
        output_dir=tmp_path,
        load_id=_LOAD_ID,
    )
    spilled_rows = {
        name: tuple(_spilled_rows(path, spill.formats[name], spill.schemas[name])) for name, path in spill.files.items()
    }

    assert _identity_projection(memory.table("orders").rows) == _identity_projection(spilled_rows["orders"])
    assert _identity_projection(memory.table("orders__items").rows) == _identity_projection(
        spilled_rows["orders__items"]
    )
    assert _identity_projection(memory.table("orders__raw").rows) == _identity_projection(spilled_rows["orders__raw"])
    assert len({row["__dpone__row_id"] for row in spilled_rows["orders"]}) == 2
    assert len({row["__dpone__row_id"] for row in spilled_rows["orders__items"]}) == 2
    assert len({row["__dpone__row_id"] for row in spilled_rows["orders__raw"]}) == 2

    memory_quality = ChildQualityService().evaluate_package(
        root_table="orders",
        table_rows={table.name: table.rows for table in memory.tables},
        table_keys={},
        table_parents=memory.table_parents(),
        options=options.child_quality,
        ignored_hierarchy_tables=("orders__raw",),
    )
    spill_quality = SpilledChildQualityService().evaluate_package(
        root_table="orders",
        table_rows=spilled_rows,
        table_keys={},
        table_parents=spill.table_parents,
        options=options.child_quality,
        work_dir=tmp_path,
        ignored_hierarchy_tables=("orders__raw",),
    )

    assert all(result.row_identity_status == "passed" for result in memory_quality)
    assert all(result.row_identity_status == "passed" for result in spill_quality)


def _rows_failing_after_one() -> Iterator[Mapping[str, object]]:
    yield {"order_id": 2}
    raise RuntimeError("source retry failed")


def _jsonl_rows(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _spilled_key_rows(
    tmp_path: Path,
    values: tuple[object, object],
) -> tuple[SpilledNormalizationResult, NestedNormalizationOptions]:
    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "split_paths": [{"path": "$.items[*]", "unique_key": ["item_key"]}],
        }
    )
    result = SpillToDiskNormalizationService().spill_rows(
        [{"items": [{"item_key": value}]} for value in values],
        root_table="orders",
        options=options,
        output_dir=tmp_path,
        load_id=_LOAD_ID,
        output_format="tsv",
    )
    return result, options


def _semantic_rows(result: SpilledNormalizationResult) -> dict[str, tuple[Mapping[str, object], ...]]:
    return {
        table_name: tuple(_load_semantic_rows(result, table_name, result.files[table_name]))
        for table_name in result.semantic_files
    }


def _native_rows(result: SpilledNormalizationResult) -> dict[str, tuple[Mapping[str, object], ...]]:
    return {
        table_name: tuple(_spilled_rows(path, result.formats[table_name], result.schemas[table_name]))
        for table_name, path in result.files.items()
    }


def _identity_projection(rows: Sequence[Mapping[str, object]]) -> list[tuple[object, ...]]:
    return [tuple(row[column] for column in _IDENTITY_COLUMNS) for row in rows]
