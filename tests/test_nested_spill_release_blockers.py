from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.nested_load import NestedLoadService
from dpone.runtime.etl.nested_spill_rows import semantic_rows as _load_semantic_rows
from dpone.runtime.etl.nested_spill_rows import spilled_rows as _spilled_rows
from dpone.runtime.lineage import LineageOptions
from dpone.runtime.normalization.child_quality import ChildQualityViolationError
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.spill import SpilledNormalizationResult, SpillToDiskNormalizationService
from dpone.runtime.normalization.spill_child_quality import SpilledChildQualityService
from dpone.runtime.sinks import LoadPayload
from tests.nested_package_test_support import RecordingStagedSink, identity_evaluate, identity_prepare

_LOAD_ID = "01JZ0000000000000000000001"


def test_failed_staged_validation_never_publishes_generation(tmp_path: Path) -> None:
    service = SpillToDiskNormalizationService()
    committed = service.spill_rows(
        [{"order_id": 1}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        load_id=_LOAD_ID,
    )

    def reject(_candidate: SpilledNormalizationResult) -> None:
        raise RuntimeError("quality rejected staged generation")

    with pytest.raises(RuntimeError, match="quality rejected staged generation"):
        service.spill_rows(
            [{"order_id": 2}],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
            output_dir=tmp_path,
            load_id=_LOAD_ID,
            staged_validator=reject,
        )

    assert committed.files["orders"].exists()
    assert _jsonl_rows(committed.files["orders"])[0]["order_id"] == 1
    assert len(list(tmp_path.glob(".dpone-spill-generation-*"))) == 1
    assert not any(tmp_path.glob(".dpone-spill-stage-*"))


def test_child_quality_failure_removes_unreachable_spill_generation(tmp_path: Path) -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}, {"sku": "A"}]}]),
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
        options={"sink_type": "mssql"},
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = _LOAD_ID

    class _Identity:
        events: list[str] = []

        def mark_staged(self, *_args, **_kwargs) -> None:
            self.events.append("staged")

        def mark_committed(self, *_args, **_kwargs) -> None:
            self.events.append("committed")

        def mark_failed(self, *_args, **_kwargs) -> None:
            self.events.append("failed")

    identity = _Identity()
    sink = RecordingStagedSink()
    with pytest.raises(ChildQualityViolationError, match="duplicate_child_key"):
        NestedLoadService(load_identity_service=identity).load(
            load_config=load_config,
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=NestedNormalizationOptions.from_config(
                {
                    "enabled": True,
                    "materialization": "spill_to_disk",
                    "spill_output_dir": str(tmp_path),
                    "spill_output_format": "tsv",
                    "split_paths": [
                        {
                            "path": "$.items[*]",
                            "table": "order_lines",
                            "unique_key": ["order_id", "sku"],
                        }
                    ],
                    "child_quality": {"duplicate_child_key": "fail"},
                }
            ),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert not any(tmp_path.rglob(".dpone-spill-generation-*"))
    assert sink.artifacts == {}
    assert identity.events == []


@pytest.mark.parametrize(
    "values",
    [
        (date(2026, 7, 30), date(2026, 7, 30)),
        (
            datetime(2026, 7, 30, 10, 0, tzinfo=UTC),
            datetime.fromisoformat("2026-07-30T12:00:00+02:00"),
        ),
    ],
)
def test_tsv_quality_preserves_temporal_key_identity(
    tmp_path: Path,
    values: tuple[object, object],
) -> None:
    result, options = _spilled_key_rows(tmp_path, values)

    with pytest.raises(ChildQualityViolationError, match="duplicate_child_key=1"):
        SpilledChildQualityService().evaluate_package(
            root_table="orders",
            table_rows=_semantic_rows(result),
            table_keys=result.table_keys,
            table_parents=result.table_parents,
            options=options.child_quality,
            work_dir=tmp_path,
        )


@pytest.mark.parametrize("output_format", ["jsonl", "tsv"])
def test_spill_semantic_rows_preserve_temporal_types(tmp_path: Path, output_format: str) -> None:
    expected_date = date(2026, 7, 30)
    expected_timestamp = datetime(2026, 7, 30, 12, 34, 56, tzinfo=UTC)
    result = SpillToDiskNormalizationService().spill_rows(
        [{"event_date": expected_date, "event_timestamp": expected_timestamp}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        output_format=output_format,
    )

    semantic_row = _semantic_rows(result)["orders"][0]
    native_row = _native_rows(result)["orders"][0]

    assert semantic_row["event_date"] == native_row["event_date"] == expected_date
    assert type(semantic_row["event_date"]) is type(native_row["event_date"]) is date
    assert semantic_row["event_timestamp"] == native_row["event_timestamp"] == expected_timestamp
    assert isinstance(semantic_row["event_timestamp"], datetime)
    assert isinstance(native_row["event_timestamp"], datetime)


@pytest.mark.parametrize("output_format", ["jsonl", "tsv"])
def test_spill_operator_files_preserve_bytes(tmp_path: Path, output_format: str) -> None:
    expected = b"\x00dpone\xff"
    result = SpillToDiskNormalizationService().spill_rows(
        [{"blob": expected}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        output_dir=tmp_path,
        output_format=output_format,
    )

    assert result.schemas["orders"][0] == ("blob", "bytes")
    assert _semantic_rows(result)["orders"][0]["blob"] == expected
    assert _native_rows(result)["orders"][0]["blob"] == expected


@pytest.mark.parametrize("output_format", ["jsonl", "tsv"])
def test_spill_rejects_nested_bytes_in_operator_json_values(tmp_path: Path, output_format: str) -> None:
    with pytest.raises(ValueError, match="cannot contain bytes losslessly"):
        SpillToDiskNormalizationService().spill_rows(
            [{"metadata": {"blob": b"dpone"}}],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True, preserve_nested_json=True),
            output_dir=tmp_path,
            output_format=output_format,
        )


def _spilled_key_rows(
    tmp_path: Path,
    values: tuple[object, object],
) -> tuple[SpilledNormalizationResult, NestedNormalizationOptions]:
    options = NestedNormalizationOptions.from_config(
        {"enabled": True, "split_paths": [{"path": "$.items[*]", "unique_key": ["item_key"]}]}
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


def _jsonl_rows(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
