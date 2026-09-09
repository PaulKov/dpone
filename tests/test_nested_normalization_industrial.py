from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pytest

from dpone.commands.normalize_cmd import cmd_normalize_lint
from dpone.readiness.nested_certification import (
    NestedNormalizationBenchmarkService,
    NestedNormalizationCertificationService,
)
from dpone.readiness.nested_lint import NestedNormalizationLintService
from dpone.runtime.normalization.child_quality import (
    ChildQualityService,
    ChildQualityViolationError,
)
from dpone.runtime.normalization.options import NestedChildQualityOptions, NestedNormalizationOptions
from dpone.runtime.normalization.spill import SpillToDiskNormalizationService
from dpone.runtime.normalization.spill_child_quality import SpilledChildQualityService


def test_spill_to_disk_normalization_writes_per_table_jsonl_without_in_memory_result(tmp_path: Path) -> None:
    rows = [
        {"order_id": 1, "items": [{"sku": "A"}], "tags": ["vip"]},
        {"order_id": 2, "items": [{"sku": "B"}, {"sku": "C"}], "tags": []},
    ]

    result = SpillToDiskNormalizationService().spill_rows(
        rows,
        root_table="orders",
        options=NestedNormalizationOptions.from_config({"enabled": True, "raw_landing": True}),
        output_dir=tmp_path,
    )

    assert result.row_counts["orders"] == 2
    assert result.row_counts["orders__items"] == 3
    assert result.row_counts["orders__raw"] == 2
    assert result.files["orders"].name == "orders.jsonl"
    first_root = json.loads(result.files["orders"].read_text(encoding="utf-8").splitlines()[0])
    assert first_root["order_id"] == 1
    assert "items" not in first_root


def test_spilled_child_quality_uses_bounded_external_sort_and_cleans_runs(tmp_path: Path) -> None:
    rows_per_table = 512

    def root_rows():
        for index in range(rows_per_table):
            row_id = f"root-{index}"
            yield {
                "__dpone__row_id": row_id,
                "__dpone__root_row_id": row_id,
                "__dpone__parent_row_id": None,
            }

    def child_rows():
        for index in range(rows_per_table):
            yield {
                "sku": f"sku-{index}",
                "__dpone__row_id": f"line-{index}",
                "__dpone__root_row_id": f"root-{index}",
                "__dpone__parent_row_id": f"root-{index}",
            }

    (result,) = SpilledChildQualityService(sort_chunk_bytes=128).evaluate_package(
        root_table="orders",
        table_rows={"orders": root_rows(), "order_lines": child_rows()},
        table_keys={"order_lines": ("sku",)},
        options=NestedChildQualityOptions(),
        table_parents={"order_lines": "orders"},
        work_dir=tmp_path,
    )

    assert result.status == "passed"
    assert result.child_rows == rows_per_table
    assert not tuple(tmp_path.glob(".dpone-quality-*"))
    assert not tuple(tmp_path.glob(".dpone-external-sort-run-*"))


def test_spilled_child_quality_cleans_external_sort_files_after_failure(tmp_path: Path) -> None:
    with pytest.raises(ChildQualityViolationError, match="missing_row_id=1"):
        SpilledChildQualityService().evaluate_package(
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
            table_parents={"order_lines": "orders"},
            work_dir=tmp_path,
        )

    assert not tuple(tmp_path.glob(".dpone-quality-*"))
    assert not tuple(tmp_path.glob(".dpone-external-sort-run-*"))


@pytest.mark.parametrize(
    ("first_key", "second_key", "duplicates"),
    [
        (None, None, True),
        (1, 1.0, True),
        (True, 1, True),
        (-0.0, 0.0, True),
        ("1", 1, False),
    ],
)
def test_spilled_child_quality_matches_memory_policy_results(
    tmp_path: Path,
    first_key: object,
    second_key: object,
    duplicates: bool,
) -> None:
    tables = {
        "orders": [
            {
                "__dpone__row_id": "root-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": None,
            }
        ],
        "order_lines": [
            {
                "sku": first_key,
                "__dpone__row_id": "line-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "root-1",
            },
            {
                "sku": second_key,
                "__dpone__row_id": "line-2",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "root-1",
            },
        ],
    }
    options = NestedChildQualityOptions(
        duplicate_child_key="warn",
        orphan_child_rows="fail",
    )
    arguments = {
        "root_table": "orders",
        "table_rows": tables,
        "table_keys": {"order_lines": ("sku",)},
        "options": options,
        "table_parents": {"order_lines": "orders"},
    }

    if duplicates:
        with pytest.warns(UserWarning, match="duplicate_child_key=1"):
            memory = ChildQualityService().evaluate_package(**arguments)
        with pytest.warns(UserWarning, match="duplicate_child_key=1"):
            spilled = SpilledChildQualityService().evaluate_package(
                **arguments,
                work_dir=tmp_path,
            )
    else:
        memory = ChildQualityService().evaluate_package(**arguments)
        spilled = SpilledChildQualityService().evaluate_package(
            **arguments,
            work_dir=tmp_path,
        )

    assert [result.to_dict() for result in spilled] == [result.to_dict() for result in memory]


def test_spilled_child_quality_rejects_unhashable_keys_like_memory(tmp_path: Path) -> None:
    tables = {
        "orders": [{"__dpone__row_id": "root-1", "__dpone__root_row_id": "root-1"}],
        "order_lines": [
            {
                "sku": ["unsupported"],
                "__dpone__row_id": "line-1",
                "__dpone__root_row_id": "root-1",
                "__dpone__parent_row_id": "root-1",
            }
        ],
    }
    arguments = {
        "root_table": "orders",
        "table_rows": tables,
        "table_keys": {"order_lines": ("sku",)},
        "options": NestedChildQualityOptions(),
        "table_parents": {"order_lines": "orders"},
    }

    with pytest.raises(TypeError, match="unhashable type"):
        ChildQualityService().evaluate_package(**arguments)
    with pytest.raises(TypeError, match="unhashable type"):
        SpilledChildQualityService().evaluate_package(
            **arguments,
            work_dir=tmp_path,
        )

    assert not tuple(tmp_path.glob(".dpone-quality-*"))
    assert not tuple(tmp_path.glob(".dpone-external-sort-run-*"))


def test_nested_lint_detects_table_collisions_and_missing_guardrails() -> None:
    report = NestedNormalizationLintService().lint_config(
        {
            "enabled": True,
            "split_paths": [
                {"path": "$.items[*]", "table": "order_lines"},
                {"path": "$.lines[*]", "table": "order_lines"},
            ],
        },
        root_table="orders",
    )

    assert report.has_errors
    assert any(issue.code == "nested.table_collision" for issue in report.issues)
    assert any(issue.code == "nested.guardrails_missing" for issue in report.issues)


def test_normalize_lint_command_writes_json_report(tmp_path: Path) -> None:
    config = tmp_path / "normalization.json"
    config.write_text(
        json.dumps(
            {
                "enabled": True,
                "split_paths": [
                    {"path": "$.items[*]", "table": "order_lines"},
                    {"path": "$.lines[*]", "table": "order_lines"},
                ],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "lint.json"
    args = argparse.Namespace(
        root_table="orders", config=str(config), format="json", output=str(output), fail_on_error=False
    )

    assert cmd_normalize_lint(args, ctx=object(), logger=logging.getLogger(__name__)) == 0

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["has_errors"] is True
    assert payload["issues"][0]["severity"] == "error"


def test_certification_service_writes_evidence_with_reverse_readback_and_quarantine(tmp_path: Path) -> None:
    artifact = NestedNormalizationCertificationService().certify(
        output_dir=tmp_path,
        row_count=25,
        root_table="orders",
    )

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "unverified"
    assert payload["checks"]["spill_to_disk"] == "passed"
    assert payload["checks"]["reverse_readback"] == "passed"
    assert payload["checks"]["quarantine"] == "passed"
    assert artifact.markdown_path.exists()


def test_benchmark_service_caps_rows_and_reports_throughput(tmp_path: Path) -> None:
    service = NestedNormalizationBenchmarkService(max_rows=100)

    with pytest.raises(ValueError, match="max_rows=100"):
        service.run(output_dir=tmp_path, row_count=101)

    artifact = service.run(output_dir=tmp_path, row_count=40)
    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["row_count"] == 40
    assert payload["normalized_rows"] >= 40
    assert payload["rows_per_second"] > 0


def test_normalize_certify_and_benchmark_commands_do_not_require_output_file(tmp_path: Path) -> None:
    import argparse
    import logging

    from dpone.commands.normalize_cmd import cmd_normalize_benchmark, cmd_normalize_certify

    certify_args = argparse.Namespace(
        output_dir=str(tmp_path / "certify"),
        root_table="orders",
        row_count=10,
        format="text",
    )
    benchmark_args = argparse.Namespace(
        output_dir=str(tmp_path / "benchmark"),
        root_table="orders",
        row_count=10,
        max_rows=100,
        format="text",
    )

    assert cmd_normalize_certify(certify_args, ctx=object(), logger=logging.getLogger(__name__)) == 0
    assert cmd_normalize_benchmark(benchmark_args, ctx=object(), logger=logging.getLogger(__name__)) == 0
    assert (tmp_path / "certify" / "nested_normalization_certification.json").exists()
    assert (tmp_path / "benchmark" / "nested_normalization_benchmark.json").exists()


def test_child_unique_key_keeps_row_identity_stable_when_array_reorders() -> None:
    from dpone.runtime.normalization.normalizer import NestedNormalizationService

    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "split_paths": [
                {
                    "path": "$.items[*]",
                    "table": "order_lines",
                    "unique_key": ["order_id", "sku"],
                }
            ],
        }
    )
    first = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "B"}]}],
        root_table="orders",
        options=options,
        unique_key=["order_id"],
    )
    second = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "items": [{"sku": "B"}, {"sku": "A"}]}],
        root_table="orders",
        options=options,
        unique_key=["order_id"],
    )

    first_by_sku = {row["sku"]: row for row in first.table("order_lines").rows}
    second_by_sku = {row["sku"]: row for row in second.table("order_lines").rows}

    assert first_by_sku["A"]["__dpone__row_id"] == second_by_sku["A"]["__dpone__row_id"]
    assert first_by_sku["B"]["__dpone__row_id"] == second_by_sku["B"]["__dpone__row_id"]
    assert first_by_sku["A"]["order_id"] == 1


def test_child_reconciliation_detects_physical_deletes_by_child_unique_key() -> None:
    from dpone.runtime.normalization.normalizer import NestedNormalizationService
    from dpone.runtime.normalization.reconciliation import ChildTableReconciliationService

    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
        }
    )
    previous = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "B"}, {"sku": "C"}]}],
        root_table="orders",
        options=options,
        unique_key=["order_id"],
    )
    current = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "C"}]}],
        root_table="orders",
        options=options,
        unique_key=["order_id"],
    )

    plan = ChildTableReconciliationService().diff_results(previous, current, options=options)

    assert plan.tables["order_lines"].deleted_keys == ({"order_id": 1, "sku": "B"},)


def test_nested_load_service_can_materialize_child_tables_from_spill_files(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from dpone.config import LoadConfig, LoadStrategy
    from dpone.runtime.etl.nested_load import NestedLoadService
    from dpone.runtime.lineage.options import LineageOptions
    from dpone.runtime.row_artifacts import InMemoryRowsArtifact, StreamingRowsArtifact
    from dpone.runtime.sinks.base import LoadPayload

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
        options={"normalization": {"nested": {"enabled": True, "materialization": "spill_to_disk"}}},
    )
    from tests.nested_package_test_support import (
        RecordingStagedSink,
        artifact_rows,
        identity_evaluate,
        identity_prepare,
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _Identity:
        def mark_staged(self, *_args, **_kwargs) -> None:
            return None

        def mark_committed(self, *_args, **_kwargs) -> None:
            return None

    sink = RecordingStagedSink()
    result = NestedLoadService(load_identity_service=_Identity()).load(
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
            }
        ),
        package_mutation=sink.mutation(),
        prepare_member=identity_prepare,
        evaluate_package=identity_evaluate,
    )

    assert result.total_rows == 2
    seen_artifacts = [type(sink.artifacts[name]) for name in ("orders", "orders__items")]
    assert seen_artifacts == [StreamingRowsArtifact, StreamingRowsArtifact]
    loaded_rows_by_table = {
        name: [dict(row) for row in artifact_rows(artifact)] for name, artifact in sink.artifacts.items()
    }
    assert loaded_rows_by_table["orders"][0]["order_id"] == 1
    assert isinstance(loaded_rows_by_table["orders"][0]["order_id"], int)
    assert loaded_rows_by_table["orders__items"][0]["sku"] == "A"
    assert result.reconciliation_metrics["nested_normalization"]["spill_formats"] == {
        "orders": "tsv",
        "orders__items": "tsv",
    }
