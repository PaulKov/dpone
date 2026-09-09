from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from dpone.adapters.hermetic_test_memory import InMemoryHermeticStrategyExecutor
from dpone.cli import main as cli_main
from dpone.commands.test_cmd import render_test_report
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.hermetic_test import HermeticExecutionPlan, hermetic_error
from dpone.manifest import confined_files
from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.hermetic_test import parse_hermetic_test_contract
from dpone.services.hermetic_test_service import HermeticTestService


def _write_pipeline(
    root: Path,
    *,
    pipeline_id: str = "orders_daily",
    mode: str = "incremental_merge",
    unique_key: str | list[str] | None = "id",
    process_count: int = 1,
    process_extra: dict[str, Any] | None = None,
    strategy_extra: dict[str, Any] | None = None,
) -> Path:
    processes: list[dict[str, Any]] = []
    for index in range(process_count):
        name = pipeline_id if index == 0 else f"{pipeline_id}_{index}"
        strategy: dict[str, Any] = {"mode": mode}
        if unique_key is not None:
            strategy["unique_key"] = unique_key
        if strategy_extra:
            strategy.update(strategy_extra)
        process: dict[str, Any] = {
            "name": name,
            "source": {
                "type": "mssql",
                "connection_ref": "mssql_dev",
                "table": {"schema": "dbo", "name": f"orders_{index}"},
            },
            "sink": {
                "type": "clickhouse",
                "connection_ref": "clickhouse_dev",
                "table": {"schema": "analytics", "name": f"orders_{index}"},
                "strategy": strategy,
            },
        }
        if process_extra:
            process.update(process_extra)
        processes.append(process)
    path = root / "pipelines" / pipeline_id / "pipeline.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "kind": "dpone.flow.v1",
                "authoring": {"mode": "flow", "source": f"pipelines/{pipeline_id}/pipeline.yaml"},
                "metadata": {"id": pipeline_id, "domain": "sales", "tags": ["airflow"]},
                "processes": processes,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _write_test(
    root: Path,
    *,
    pipeline_id: str = "orders_daily",
    name: str = "orders_daily_happy_path",
    fixture: str = "fixtures/orders_daily.input.jsonl",
    rows: int = 2,
    process: str | None = None,
    initial_target: str | None = None,
    output_fixture: str | None = None,
    schema: dict[str, str] | None = None,
    limits: dict[str, int] | None = None,
) -> Path:
    payload: dict[str, Any] = {
        "kind": "dpone.test.v1",
        "name": name,
        "pipeline": f"../pipelines/{pipeline_id}/pipeline.yaml",
        "input": {"fixture": fixture, "format": "jsonl"},
        "expect": {"rows": rows, "rejected_rows": 0},
    }
    if process is not None:
        payload["process"] = process
    if initial_target is not None:
        payload["input"]["initial_target"] = {"fixture": initial_target}
    if output_fixture is not None:
        payload["expect"].update({"output_fixture": output_fixture, "match": "exact_unordered"})
    if schema is not None:
        payload["expect"]["schema"] = schema
    if limits is not None:
        payload["limits"] = limits
    path = root / "tests" / f"{pipeline_id}.test.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


@pytest.mark.parametrize(
    ("mode", "initial", "incoming", "expected"),
    [
        ("full_refresh", ({"id": 9},), ({"id": 1}, {"id": 2}), ({"id": 1}, {"id": 2})),
        (
            "incremental_append",
            ({"id": 9},),
            ({"id": 1}, {"id": 2}),
            ({"id": 9}, {"id": 1}, {"id": 2}),
        ),
        (
            "incremental_merge",
            ({"id": 1, "status": "old"}, {"id": 3, "status": "keep"}),
            ({"id": 1, "status": "new"}, {"id": 2, "status": "add"}),
            ({"id": 1, "status": "new"}, {"id": 2, "status": "add"}, {"id": 3, "status": "keep"}),
        ),
    ],
)
def test_in_memory_executor_applies_only_supported_final_state_semantics(
    mode: str,
    initial: tuple[dict[str, Any], ...],
    incoming: tuple[dict[str, Any], ...],
    expected: tuple[dict[str, Any], ...],
) -> None:
    plan = HermeticExecutionPlan(mode=mode, unique_key=("id",) if mode == "incremental_merge" else ())

    result = InMemoryHermeticStrategyExecutor().execute(
        plan,
        incoming,
        initial,
        cancelled=lambda: False,
    )

    assert result.rows == expected


def test_hermetic_error_links_to_the_owning_catalog() -> None:
    test_error = hermetic_error("DPONE_TEST_TIMEOUT", "timeout", stage="test_execute")
    compiler_error = hermetic_error(
        "DPONE_PIPELINE_PROCESS_INVALID",
        "invalid pipeline",
        stage="test_compile",
    )

    assert test_error["docs_url"] == "docs/errors/DPONE_TEST.md#dpone_test_timeout"
    assert compiler_error["docs_url"] == "docs/errors/DPONE_PIPELINE_PROCESS_INVALID.md"


@pytest.mark.parametrize(
    "incoming",
    [
        ({"id": 1}, {"id": 1}),
        ({"id": None},),
        ({"status": "missing"},),
    ],
)
def test_merge_rejects_duplicate_null_or_missing_keys_without_row_values(incoming: tuple[dict[str, Any], ...]) -> None:
    executor = InMemoryHermeticStrategyExecutor()

    with pytest.raises(Exception) as exc:
        executor.execute(
            HermeticExecutionPlan(mode="incremental_merge", unique_key=("id",)),
            incoming,
            (),
            cancelled=lambda: False,
        )

    assert getattr(exc.value, "code", "").startswith("DPONE_TEST_")
    assert repr(incoming) not in str(exc.value)


def test_service_runs_canonical_pipeline_and_is_content_deterministic(tmp_path: Path) -> None:
    _write_pipeline(tmp_path)
    _write_test(tmp_path, schema={"id": "int64", "status": "string"})
    _write_jsonl(
        tmp_path / "tests/fixtures/orders_daily.input.jsonl",
        [{"id": 1, "status": "new"}, {"id": 2, "status": "ready"}],
    )

    first = HermeticTestService(root=tmp_path).run("pipelines/orders_daily")
    second = HermeticTestService(root=tmp_path).run("orders_daily")

    assert first.passed is True
    assert first.exit_code == 0
    assert first.tests[0].test_id == second.tests[0].test_id
    payload = first.to_jsonable()
    assert payload["tests"][0]["coverage"]["level"] == "hermetic_contract"
    assert payload["tests"][0]["pipeline"]["semantic_fingerprint"].startswith("sha256:")
    assert payload["tests"][0]["temporary_target"]["rows"] == 2
    assert payload["tests"][0]["temporary_target"]["uri"].startswith("tmp://dpone-tests/sha256-")
    assert "rows_data" not in json.dumps(payload)


def test_service_test_id_uses_the_approved_canonical_identity_formula(tmp_path: Path) -> None:
    pipeline_path = _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    test_path = _write_test(tmp_path)
    fixture_path = tmp_path / "tests/fixtures/orders_daily.input.jsonl"
    _write_jsonl(fixture_path, [{"id": 1}, {"id": 2}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")
    raw_test = yaml.safe_load(test_path.read_text(encoding="utf-8"))
    contract = parse_hermetic_test_contract(raw_test, default_name="orders_daily")
    compilation = default_authoring_compiler().compile(
        yaml.safe_load(pipeline_path.read_text(encoding="utf-8")),
        source_path=pipeline_path,
        project_root=tmp_path,
    )
    input_digest = "sha256:" + hashlib.sha256(fixture_path.read_bytes()).hexdigest()
    expected = canonical_fingerprint(
        {
            "contract_version": "dpone.test.v1",
            "normalized_test_manifest": contract.normalized_payload,
            "pipeline_semantic_fingerprint": compilation.semantic_fingerprint,
            "canonical_selected_process": compilation.processes[0],
            "input_fixture_sha256": input_digest,
            "initial_target_sha256_or_null": None,
            "expected_output_sha256_or_null": None,
        }
    )

    assert report.tests[0].test_id == expected


def test_service_compares_expected_output_as_duplicate_preserving_unordered_rows(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="incremental_merge")
    _write_test(
        tmp_path,
        initial_target="fixtures/orders_daily.target.jsonl",
        output_fixture="fixtures/orders_daily.expected.jsonl",
        rows=3,
    )
    _write_jsonl(
        tmp_path / "tests/fixtures/orders_daily.input.jsonl",
        [{"id": 1, "status": "new"}, {"id": 2, "status": "add"}],
    )
    _write_jsonl(
        tmp_path / "tests/fixtures/orders_daily.target.jsonl",
        [{"id": 3, "status": "keep"}, {"id": 1, "status": "old"}],
    )
    _write_jsonl(
        tmp_path / "tests/fixtures/orders_daily.expected.jsonl",
        [{"id": 2, "status": "add"}, {"id": 3, "status": "keep"}, {"id": 1, "status": "new"}],
    )

    report = HermeticTestService(root=tmp_path).run(tmp_path / "tests/orders_daily.test.yaml")

    assert report.passed is True
    assert report.tests[0].status == "passed"


def test_service_normalizes_comma_delimited_composite_unique_key_like_runtime(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="incremental_merge", unique_key="id, region")
    _write_test(tmp_path, rows=3, initial_target="fixtures/orders_daily.target.jsonl")
    _write_jsonl(
        tmp_path / "tests/fixtures/orders_daily.input.jsonl",
        [{"id": 1, "region": "west"}, {"id": 1, "region": "east"}],
    )
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.target.jsonl", [{"id": 2, "region": "west"}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.passed is True
    assert report.tests[0].temporary_target["rows"] == 3


def test_service_uses_runtime_unique_key_precedence_from_source_options(tmp_path: Path) -> None:
    _write_pipeline(
        tmp_path,
        mode="incremental_merge",
        unique_key=None,
        process_extra={
            "source": {
                "type": "mssql",
                "connection_ref": "mssql_dev",
                "table": {"schema": "dbo", "name": "orders"},
                "options": {"unique_key": "tenant_id, order_id"},
            }
        },
    )
    _write_test(tmp_path, rows=2)
    _write_jsonl(
        tmp_path / "tests/fixtures/orders_daily.input.jsonl",
        [{"tenant_id": 1, "order_id": 10}, {"tenant_id": 1, "order_id": 11}],
    )

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.passed is True
    assert report.tests[0].temporary_target["rows"] == 2


def test_service_fails_closed_for_unmodeled_only_new_rows_append(tmp_path: Path) -> None:
    _write_pipeline(
        tmp_path,
        mode="incremental_append",
        unique_key="id",
        strategy_extra={"only_new_rows": True},
    )
    _write_test(tmp_path, rows=1)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.exit_code == 2
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_EXECUTION_UNSUPPORTED"


@pytest.mark.parametrize("unsupported", ["source_options_query", "nested_normalization"])
def test_service_fails_closed_for_runtime_behavior_outside_hermetic_coverage(
    tmp_path: Path,
    unsupported: str,
) -> None:
    pipeline_path = _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    payload = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    process = payload["processes"][0]
    if unsupported == "source_options_query":
        process["source"]["options"] = {"query": {"mode": "inline", "sql": "SELECT 1"}}
    else:
        process["sink"]["options"] = {"normalization": {"nested": {"enabled": True}}}
    pipeline_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.tests[0].status == "blocked"
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_EXECUTION_UNSUPPORTED"


@pytest.mark.parametrize("mode", ["full_refresh", "incremental_append", "incremental_merge"])
def test_service_rejects_non_fail_duplicate_policy_for_every_supported_strategy(
    tmp_path: Path,
    mode: str,
) -> None:
    _write_pipeline(
        tmp_path,
        mode=mode,
        unique_key="id" if mode == "incremental_merge" else None,
        strategy_extra={"duplicate_policy": "latest_by"},
    )
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.tests[0].status == "blocked"
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_EXECUTION_UNSUPPORTED"


@pytest.mark.parametrize(
    "strategy_extra",
    [
        {"merge_policy": "mutation_delete_insert"},
        {"allow_non_recommended_policy": True},
        {"mutations_sync": 1},
        {"diff": {"delete_policy": "hard_delete"}},
        {"custom_predicate": "business_date = CURRENT_DATE"},
    ],
)
def test_service_rejects_unmodeled_strategy_options(
    tmp_path: Path,
    strategy_extra: dict[str, Any],
) -> None:
    _write_pipeline(
        tmp_path,
        mode="incremental_merge",
        unique_key="id",
        strategy_extra=strategy_extra,
    )
    _write_test(tmp_path, rows=1)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.tests[0].status == "blocked"
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_EXECUTION_UNSUPPORTED"


@pytest.mark.parametrize(
    ("option_owner", "execution_options"),
    [
        (
            "source",
            {
                "with_dedup": True,
                "dedup_expression": "ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1",
            },
        ),
        (
            "sink",
            {
                "with_dedup": True,
                "dedup_expression": "ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1",
            },
        ),
        ("sink", {"schema_evolution": {"enabled": True, "apply_safe": True}}),
    ],
)
def test_service_rejects_unmodeled_connector_execution_options(
    tmp_path: Path,
    option_owner: str,
    execution_options: dict[str, Any],
) -> None:
    pipeline_path = _write_pipeline(tmp_path, mode="incremental_append", unique_key="id")
    payload = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    payload["processes"][0][option_owner]["options"] = execution_options
    pipeline_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    _write_test(tmp_path, rows=1)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.tests[0].status == "blocked"
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_EXECUTION_UNSUPPORTED"


def test_service_fails_expectations_without_exposing_fixture_values(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=99)
    secret = "password=fixture-value-must-not-leak"
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1, "note": secret}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")
    encoded = json.dumps(report.to_jsonable())

    assert report.passed is False
    assert report.exit_code == 1
    assert report.tests[0].status == "failed"
    assert report.tests[0].execution_status == "succeeded"
    assert secret not in encoded


def test_service_requires_process_when_pipeline_is_ambiguous(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, process_count=2)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.exit_code == 2
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_PROCESS_AMBIGUOUS"


@pytest.mark.parametrize(
    "process_extra",
    [
        {"transforms": [{"type": "sql", "sql": "select 1"}]},
        {"source": {"type": "mssql", "query": "select 1", "table": {"schema": "dbo", "name": "orders"}}},
        {"reconciliation": True},
        {
            "schema_contract": {
                "enforcement": "quarantine",
                "columns": {"id": {"logical_type": "integer", "nullable": False}},
            }
        },
        {"quality": {"checks": [{"type": "min_rows", "value": 1, "mode": "warn"}]}},
    ],
)
def test_service_fails_closed_for_unimplemented_execution_semantics(
    tmp_path: Path,
    process_extra: dict[str, Any],
) -> None:
    _write_pipeline(tmp_path, process_extra=process_extra)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.exit_code == 2
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_EXECUTION_UNSUPPORTED"


def test_fixture_reader_rejects_duplicate_json_keys_and_symlinks(tmp_path: Path) -> None:
    _write_pipeline(tmp_path)
    _write_test(tmp_path)
    fixture = tmp_path / "tests/fixtures/orders_daily.input.jsonl"
    fixture.parent.mkdir(parents=True)
    fixture.write_text('{"id": 1, "id": 2}\n', encoding="utf-8")

    duplicate = HermeticTestService(root=tmp_path).run("orders_daily")

    assert duplicate.exit_code == 2
    assert duplicate.tests[0].errors[0]["code"] == "DPONE_TEST_FIXTURE_INVALID"

    external = tmp_path.parent / f"{tmp_path.name}-outside.jsonl"
    external.write_text('{"id": 1}\n', encoding="utf-8")
    fixture.unlink()
    fixture.symlink_to(external)
    try:
        symlink = HermeticTestService(root=tmp_path).run("orders_daily")
    finally:
        external.unlink()

    assert symlink.exit_code == 4
    assert symlink.tests[0].errors[0]["code"] == "DPONE_TEST_PATH_UNSAFE"


def test_fixture_reader_rejects_excessive_json_depth_before_execution(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=1, fixture="fixtures/deep.json")
    fixture = tmp_path / "tests/fixtures/deep.json"
    fixture.parent.mkdir(parents=True)
    fixture.write_text('{"payload":' + "[" * 128 + "0" + "]" * 128 + "}\n", encoding="utf-8")

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.exit_code == 2
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_FIXTURE_INVALID"


def test_project_suite_continues_after_expectation_failure(tmp_path: Path) -> None:
    for pipeline_id, expected_rows in (("orders_daily", 1), ("customers_daily", 2)):
        _write_pipeline(tmp_path, pipeline_id=pipeline_id, mode="full_refresh", unique_key=None)
        _write_test(
            tmp_path,
            pipeline_id=pipeline_id,
            name=f"{pipeline_id}_happy_path",
            fixture=f"fixtures/{pipeline_id}.input.jsonl",
            rows=expected_rows,
        )
        _write_jsonl(tmp_path / f"tests/fixtures/{pipeline_id}.input.jsonl", [{"id": 1}])

    report = HermeticTestService(root=tmp_path).run(".")

    assert report.exit_code == 1
    assert report.counts == {"total": 2, "passed": 1, "failed": 1, "blocked": 0}
    assert [test.name for test in report.tests] == ["customers_daily_happy_path", "orders_daily_happy_path"]


def test_cli_scaffold_creates_an_immediately_executable_test(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    assert _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)[0] == 0
    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr
    assert len(json.loads(stdout)["changes"]) == 4
    assert (tmp_path / "tests/fixtures/orders_daily.input.jsonl").exists()

    code, stdout, stderr = _run_cli(["test", "pipelines/orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone test: PASS" in stdout
    assert "- strategy: incremental_merge" in stdout
    assert "- rows: 2" in stdout
    assert "- coverage: hermetic contract" in stdout
    assert "- journey: offline golden path complete" in stdout
    assert "route certified" not in stdout.lower()


@pytest.mark.parametrize("authoring", ["flow", "classic", "folder"])
def test_cli_runs_scaffolded_primary_authoring_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    authoring: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    assert _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)[0] == 0
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--authoring",
            authoring,
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    code, stdout, stderr = _run_cli(["test", "pipelines/orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone test: PASS" in stdout


def test_cli_writes_complete_json_report_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "test-artifacts" / "report.json"

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(output)],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone test: PASS" in stdout
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema"] == "dpone.test-suite-report.v1"
    assert payload["counts"] == {"total": 1, "passed": 1, "failed": 0, "blocked": 0}
    assert not output.with_name(f".{output.name}.tmp").exists()
    first_content = output.read_bytes()
    first_identity = (output.stat().st_ino, output.stat().st_mtime_ns)

    second_code, _, second_stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(output)],
        capsys,
    )
    assert second_code == 0, second_stderr
    assert output.read_bytes() == first_content
    assert (output.stat().st_ino, output.stat().st_mtime_ns) == first_identity


def test_json_report_no_op_ignores_only_contract_timing_fields() -> None:
    from dpone.commands import test_cmd

    existing = {
        "duration_ms": 1,
        "tests": [
            {
                "duration_ms": 2,
                "temporary_target": {"schema": {"duration_ms": "int64"}},
            }
        ],
    }
    desired: dict[str, Any] = {
        "duration_ms": 99,
        "tests": [
            {
                "duration_ms": 100,
                "temporary_target": {"schema": {"duration_ms": "int64"}},
            }
        ],
    }

    assert test_cmd._reports_equivalent(json.dumps(existing).encode(), json.dumps(desired).encode())
    desired["tests"][0]["temporary_target"]["schema"]["duration_ms"] = "string"
    assert not test_cmd._reports_equivalent(json.dumps(existing).encode(), json.dumps(desired).encode())


@pytest.mark.parametrize(("winner", "expected_code"), [("identical", 0), ("different", 4)])
def test_concurrent_first_report_create_converges_only_for_identical_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    winner: str,
    expected_code: int,
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    output = tmp_path / "test-artifacts/report.json"
    monkeypatch.chdir(tmp_path)

    def publish_winner(source: str, target: Path) -> None:
        target.write_bytes(Path(source).read_bytes() if winner == "identical" else b"concurrent user content\n")
        raise FileExistsError

    monkeypatch.setattr("dpone.commands.test_cmd.os.link", publish_winner)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(output)],
        capsys,
    )

    assert code == expected_code
    assert stderr == ""
    if winner == "identical":
        assert json.loads(output.read_text(encoding="utf-8"))["schema"] == "dpone.test-suite-report.v1"
    else:
        assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
        assert output.read_bytes() == b"concurrent user content\n"
    assert not tuple(output.parent.glob(f".{output.name}.*.tmp"))


@pytest.mark.parametrize(
    "relative_output",
    [
        "pipelines/orders_daily/pipeline.yaml",
        "tests/orders_daily.test.yaml",
        "tests/fixtures/orders_daily.input.jsonl",
    ],
)
def test_cli_never_overwrites_authoring_or_fixture_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    relative_output: str,
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    target = tmp_path / relative_output
    before = target.read_bytes()
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(target)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert target.read_bytes() == before


def test_cli_never_overwrites_unrelated_file_or_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    unrelated = tmp_path / "notes.txt"
    unrelated.write_text("# schema: dpone.test-suite-report.v1\nreviewed user content\n", encoding="utf-8")
    linked = tmp_path / "report-link.json"
    linked.symlink_to(unrelated)
    monkeypatch.chdir(tmp_path)

    for target in (unrelated, linked):
        code, stdout, stderr = _run_cli(
            ["test", "orders_daily", "--format", "json", "--output", str(target)],
            capsys,
        )
        assert code == 4
        assert stderr == ""
        assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert unrelated.read_text(encoding="utf-8") == ("# schema: dpone.test-suite-report.v1\nreviewed user content\n")
    assert linked.is_symlink()


def test_cli_never_overwrites_incomplete_json_with_suite_schema_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    unrelated = tmp_path / "report.json"
    before = b'{"schema":"dpone.test-suite-report.v1"}\n'
    unrelated.write_bytes(before)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(unrelated)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert unrelated.read_bytes() == before


@pytest.mark.parametrize(
    "before",
    [
        "dpone test: PASS\n- schema: dpone.test-suite-report.v1\n- tests: 1 (1 passed, 0 failed, 0 blocked)\nuser notes\n",
        "# dpone test\n\n- Schema: `dpone.test-suite-report.v1`\n- Status: **PASS**\n- tests: 1 (1 passed, 0 failed, 0 blocked)\nuser notes\n",
    ],
)
def test_cli_never_overwrites_incomplete_text_report_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    before: str,
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    unrelated = tmp_path / "report.txt"
    unrelated.write_text(before, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "text", "--output", str(unrelated)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert unrelated.read_text(encoding="utf-8") == before


def test_cli_never_overwrites_spoofed_or_tampered_text_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    output = tmp_path / "report.txt"
    output.write_text(
        "dpone test: PASS\n- schema: dpone.test-suite-report.v1\n- tests: planning notes\n- report-complete: true\n",
        encoding="utf-8",
    )
    before = output.read_bytes()
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "text", "--output", str(output)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert output.read_bytes() == before

    output.unlink()
    assert _run_cli(["test", "orders_daily", "--format", "text", "--output", str(output)], capsys)[0] == 0
    generated = output.read_text(encoding="utf-8")
    assert "- report-sha256: sha256:" in generated
    output.write_text(generated.replace("- rows: 2", "- rows: 999"), encoding="utf-8")
    tampered = output.read_bytes()

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "text", "--output", str(output)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert output.read_bytes() == tampered


def test_cli_protects_folder_fragment_consumed_by_compiler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    assert _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)[0] == 0
    assert (
        _run_cli(
            [
                "init",
                "pipeline",
                "orders_daily",
                "--recipe",
                "mssql-to-clickhouse-incremental",
                "--authoring",
                "folder",
                "--airflow",
                "--format",
                "json",
            ],
            capsys,
        )[0]
        == 0
    )
    fragment = tmp_path / "pipelines/orders_daily/steps/load.yaml"
    fragment.write_text(
        "# schema: dpone.test-suite-report.v1\n" + fragment.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    before = fragment.read_bytes()

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(fragment)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert fragment.read_bytes() == before


def test_cli_protects_declared_folder_fragment_when_compilation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    assert _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)[0] == 0
    assert (
        _run_cli(
            [
                "init",
                "pipeline",
                "orders_daily",
                "--recipe",
                "mssql-to-clickhouse-incremental",
                "--authoring",
                "folder",
                "--airflow",
                "--format",
                "json",
            ],
            capsys,
        )[0]
        == 0
    )
    valid_report = HermeticTestService(root=tmp_path).run("orders_daily").to_jsonable()
    fragment = tmp_path / "pipelines/orders_daily/steps/load.yaml"
    before = json.dumps(valid_report, sort_keys=True).encode()
    fragment.write_bytes(before)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(fragment)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert fragment.read_bytes() == before


def test_cli_protects_fixture_before_unsupported_process_planning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    fixture = tmp_path / "tests/fixtures/orders_daily.input.jsonl"
    _write_jsonl(fixture, [{"id": 1}, {"id": 2}])
    valid_report = HermeticTestService(root=tmp_path).run("orders_daily").to_jsonable()
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    source["processes"][0]["transforms"] = [{"type": "rename"}]
    source_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")
    before = json.dumps(valid_report, sort_keys=True).encode()
    fixture.write_bytes(before)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(fixture)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert fixture.read_bytes() == before


def test_cli_protects_later_suite_fixture_after_earlier_safety_abort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    later_test = _write_test(
        tmp_path,
        fixture="fixtures/protected-report.json",
        rows=1,
    )
    later_test.rename(tmp_path / "tests/b.test.yaml")
    protected_fixture = tmp_path / "tests/fixtures/protected-report.json"
    _write_jsonl(protected_fixture, [{"id": 1}])
    valid_report = HermeticTestService(root=tmp_path).run("tests/b.test.yaml").to_jsonable()
    before = (json.dumps(valid_report, sort_keys=True) + "\n").encode()
    protected_fixture.write_bytes(before)
    unsafe_test = {
        "kind": "dpone.test.v1",
        "name": "abort_before_later_test",
        "pipeline": "../pipelines/orders_daily/pipeline.yaml",
        "input": {"fixture": "../../../outside.jsonl", "format": "jsonl"},
        "expect": {"rows": 1, "rejected_rows": 0},
    }
    (tmp_path / "tests/a.test.yaml").write_text(
        yaml.safe_dump(unsafe_test, sort_keys=False),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["test", ".", "--format", "json", "--output", str(protected_fixture)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert protected_fixture.read_bytes() == before


def test_cli_protects_declared_fixture_when_test_contract_is_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=1, fixture="fixtures/protected-report.json")
    protected_fixture = tmp_path / "tests/fixtures/protected-report.json"
    _write_jsonl(protected_fixture, [{"id": 1}])
    valid_report = HermeticTestService(root=tmp_path).run("orders_daily").to_jsonable()
    before = (json.dumps(valid_report, sort_keys=True) + "\n").encode()
    protected_fixture.write_bytes(before)
    test_path = tmp_path / "tests/orders_daily.test.yaml"
    invalid_test = yaml.safe_load(test_path.read_text(encoding="utf-8"))
    invalid_test["expect"]["rows"] = "not-an-integer"
    test_path.write_text(yaml.safe_dump(invalid_test, sort_keys=False), encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["test", ".", "--format", "json", "--output", str(protected_fixture)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert protected_fixture.read_bytes() == before


def test_output_open_race_is_classified_as_unsafe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.commands import test_cmd

    output = tmp_path / "report.json"
    output.write_text('{"schema":"dpone.test-suite-report.v1"}\n', encoding="utf-8")
    identity = test_cmd._file_identity(output.lstat())

    def unavailable_open(path: Path, flags: int) -> int:
        del path, flags
        raise FileNotFoundError

    monkeypatch.setattr(test_cmd.os, "open", unavailable_open)

    with pytest.raises(test_cmd._UnsafeOutputError):
        test_cmd._read_existing_output(output, expected_identity=identity)


@pytest.mark.parametrize(("output_format", "suffix"), [("text", "txt"), ("md", "md")])
def test_cli_treats_identical_text_reports_as_no_op(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    output_format: str,
    suffix: str,
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    output = tmp_path / f"report.{suffix}"
    monkeypatch.chdir(tmp_path)

    first = _run_cli(
        ["test", "orders_daily", "--format", output_format, "--output", str(output)],
        capsys,
    )
    first_content = output.read_bytes()
    first_identity = (output.stat().st_ino, output.stat().st_mtime_ns)
    second = _run_cli(
        ["test", "orders_daily", "--format", output_format, "--output", str(output)],
        capsys,
    )

    assert first[0] == 0, first[2]
    assert second[0] == 0, second[2]
    assert "dpone.test-suite-report.v1" in output.read_text(encoding="utf-8")
    assert output.read_bytes() == first_content
    assert (output.stat().st_ino, output.stat().st_mtime_ns) == first_identity


@pytest.mark.parametrize(("output_format", "suffix"), [("text", "txt"), ("md", "md")])
@pytest.mark.parametrize("separator", ["\n", "\u2028", "\u2029"])
def test_cli_keeps_multiline_text_report_as_verified_no_op(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    output_format: str,
    suffix: str,
    separator: str,
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, name=f"orders{separator}- report-complete: true{separator}continued")
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    output = tmp_path / f"report.{suffix}"
    monkeypatch.chdir(tmp_path)

    first = _run_cli(["test", "orders_daily", "--format", output_format, "--output", str(output)], capsys)
    first_content = output.read_bytes()
    first_identity = (output.stat().st_ino, output.stat().st_mtime_ns)
    second = _run_cli(["test", "orders_daily", "--format", output_format, "--output", str(output)], capsys)

    assert first[0] == 0
    assert second[0] == 0
    content = output.read_text(encoding="utf-8")
    assert content.splitlines().count("- report-complete: true") == 1
    escape = f"\\u{ord(separator):04x}"
    assert f"orders{escape}- report-complete: true{escape}continued" in content
    assert output.read_bytes() == first_content
    assert (output.stat().st_ino, output.stat().st_mtime_ns) == first_identity


def test_cli_does_not_replace_report_changed_between_plan_and_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.commands import test_cmd

    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    output = tmp_path / "report.json"
    monkeypatch.chdir(tmp_path)
    assert _run_cli(["test", "orders_daily", "--format", "json", "--output", str(output)], capsys)[0] == 0
    original_read = test_cmd._read_existing_output
    read_count = 0
    raced_content = b""

    def read_after_change(path: Path, *, expected_identity: tuple[int, int, int, int]) -> bytes:
        nonlocal read_count, raced_content
        read_count += 1
        if read_count == 2:
            before = path.stat()
            raced_content = b"x" * before.st_size
            path.write_bytes(raced_content)
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        return original_read(path, expected_identity=expected_identity)

    monkeypatch.setattr(test_cmd, "_read_existing_output", read_after_change)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(output)],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_UNSAFE" in stdout
    assert output.read_bytes() == raced_content
    assert not tuple(output.parent.glob(f".{output.name}.*.tmp"))


def test_cli_output_write_failure_is_redacted_and_leaves_no_partial_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=1)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}])
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "test-artifacts" / "sensitive-report-name.json"

    def fail_link(source: str, target: Path) -> None:
        del source, target
        raise OSError("sensitive-report-name.json must not leak through exception text")

    monkeypatch.setattr("dpone.commands.test_cmd.os.link", fail_link)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(output)],
        capsys,
    )

    assert code == 5
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_WRITE_FAILED" in stdout
    assert "sensitive-report-name" not in stdout
    assert not output.exists()
    assert not tuple(output.parent.glob(f".{output.name}.*.tmp"))


def test_cli_fsync_failure_removes_named_temporary_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=1)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}])
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "test-artifacts" / "report.json"

    def fail_fsync(descriptor: int) -> None:
        del descriptor
        raise OSError("simulated disk failure")

    monkeypatch.setattr("dpone.commands.test_cmd.os.fsync", fail_fsync)

    code, stdout, stderr = _run_cli(
        ["test", "orders_daily", "--format", "json", "--output", str(output)],
        capsys,
    )

    assert code == 5
    assert stderr == ""
    assert "DPONE_TEST_OUTPUT_WRITE_FAILED" in stdout
    assert not output.exists()
    assert not tuple(output.parent.glob(f".{output.name}.*.tmp"))


def test_confined_reader_dispatches_to_windows_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import contextmanager

    payload = b'{"id":1}\n'
    fixture = tmp_path / "fixture.jsonl"
    fixture.write_bytes(payload)
    calls: list[tuple[Path, tuple[str, ...]]] = []

    @contextmanager
    def fake_windows_open(root: Path, parts: tuple[str, ...]):
        calls.append((root.resolve(), parts))
        descriptor = os.open(fixture, os.O_RDONLY)
        try:
            yield descriptor
        finally:
            os.close(descriptor)

    monkeypatch.setattr(confined_files.sys, "platform", "win32")
    monkeypatch.setattr(confined_files, "_open_confined_file_windows", fake_windows_open)

    assert confined_files.read_confined_file(tmp_path, "fixture.jsonl", max_bytes=1024) == payload
    assert calls == [(tmp_path.resolve(), ("fixture.jsonl",))]


def test_manifest_rejects_schema_assertions_beyond_report_limit(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    test_path = _write_test(
        tmp_path,
        rows=0,
        schema={f"column_{index}": "string" for index in range(1001)},
    )
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [])

    report = HermeticTestService(root=tmp_path).run("orders_daily")
    manifest_schema = json.loads(Path("src/dpone/schema/test-manifest.schema.json").read_text(encoding="utf-8"))
    manifest = yaml.safe_load(test_path.read_text(encoding="utf-8"))

    assert report.tests[0].status == "blocked"
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_MANIFEST_INVALID"
    assert list(Draft202012Validator(manifest_schema).iter_errors(manifest))


def test_report_schema_allows_every_bounded_expectation_mismatch(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(
        tmp_path,
        rows=0,
        schema={f"column_{index}": "string" for index in range(1000)},
        output_fixture="fixtures/orders_daily.expected.jsonl",
    )
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"actual": 1}])
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.expected.jsonl", [])
    schema = json.loads(Path("docs/schemas/gitops/test-report.schema.json").read_text(encoding="utf-8"))

    report = HermeticTestService(root=tmp_path).run("orders_daily").tests[0].to_jsonable()

    assert len(report["expectations"]) == 1002
    assert not list(Draft202012Validator(schema).iter_errors(report))


@pytest.mark.parametrize(
    ("content", "rows", "expected_schema"),
    [
        (b"", 0, {}),
        (b'{"id":1}', 1, {"id": "int64"}),
        (b'{"value":1}\n{"value":"two"}\n', 2, {"value": "mixed"}),
    ],
)
def test_fixture_edges_are_deterministic(
    tmp_path: Path,
    content: bytes,
    rows: int,
    expected_schema: dict[str, str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=rows, schema=expected_schema)
    fixture = tmp_path / "tests/fixtures/orders_daily.input.jsonl"
    fixture.parent.mkdir(parents=True)
    fixture.write_bytes(content)

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.passed is True
    assert report.tests[0].temporary_target["schema"] == expected_schema


def test_timeout_is_a_safety_failure_with_stable_error_code(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=2, limits={"timeout_seconds": 1})
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    ticks = iter((0.0, 0.0, 2.0, 2.0))

    report = HermeticTestService(root=tmp_path, clock=lambda: next(ticks, 2.0)).run("orders_daily")

    assert report.exit_code == 4
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_TIMEOUT"


def test_timeout_is_checked_for_an_empty_fixture(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=0, limits={"timeout_seconds": 1})
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [])
    ticks = iter((0.0, 0.0, 2.0, 2.0))

    report = HermeticTestService(root=tmp_path, clock=lambda: next(ticks, 2.0)).run("orders_daily")

    assert report.exit_code == 4
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_TIMEOUT"


def test_fixture_byte_limit_and_parent_traversal_fail_closed(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=1, limits={"max_bytes": 5})
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}])

    oversized = HermeticTestService(root=tmp_path).run("orders_daily")

    assert oversized.exit_code == 4
    assert oversized.tests[0].errors[0]["code"] == "DPONE_TEST_FIXTURE_LIMIT_EXCEEDED"

    test_path = _write_test(tmp_path, rows=1, fixture="../../../outside.jsonl")
    unsafe = HermeticTestService(root=tmp_path).run(test_path)

    assert unsafe.exit_code == 4
    assert unsafe.tests[0].errors[0]["code"] == "DPONE_TEST_PATH_UNSAFE"


@pytest.mark.parametrize("dependency_kind", ["oversized", "symlink"])
def test_sql_dependency_reads_are_bounded_and_no_follow(
    tmp_path: Path,
    dependency_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_path = _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    source = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    source["processes"][0]["source"]["query"] = {"sql_file": "query.sql"}
    pipeline_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")
    sql_path = pipeline_path.parent / "query.sql"
    if dependency_kind == "oversized":
        sql_path.write_bytes(b"x" * (1024 * 1024 + 1))
    else:
        real = pipeline_path.parent / "real.sql"
        real.write_text("SELECT 1\n", encoding="utf-8")
        sql_path.symlink_to(real)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])

    def forbidden_dependency_read(*args: object, **kwargs: object) -> str:
        del args, kwargs
        raise AssertionError("hermetic compilation must not read SQL dependencies")

    monkeypatch.setattr("dpone.manifest.authoring_folder.sha256_confined_file", forbidden_dependency_read)

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.tests[0].status == "blocked"
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_EXECUTION_UNSUPPORTED"


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        (("process",), None),
        (("input", "initial_target"), None),
        (("expect", "output_fixture"), None),
        (("expect", "match"), None),
        (("limits",), None),
    ],
)
def test_manifest_parser_rejects_explicit_null_exactly_like_json_schema(
    tmp_path: Path,
    field_path: tuple[str, ...],
    value: object,
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    test_path = _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    payload = yaml.safe_load(test_path.read_text(encoding="utf-8"))
    cursor = payload
    for key in field_path[:-1]:
        cursor = cursor[key]
    cursor[field_path[-1]] = value
    test_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    schema = json.loads(Path("src/dpone/schema/test-manifest.schema.json").read_text(encoding="utf-8"))

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.exit_code == 2
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_MANIFEST_INVALID"
    assert list(Draft202012Validator(schema).iter_errors(payload))


def test_unknown_manifest_field_name_is_redacted_in_all_report_formats(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    test_path = _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    payload = yaml.safe_load(test_path.read_text(encoding="utf-8"))
    secret = "password=must-not-appear"
    payload[secret] = "also-secret"
    test_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    report = HermeticTestService(root=tmp_path).run("orders_daily")

    assert report.exit_code == 2
    for output_format in ("text", "json", "md"):
        rendered = render_test_report(report, output_format=output_format)
        assert secret not in rendered
        assert "also-secret" not in rendered


def test_suite_enumeration_error_is_normalized_without_filesystem_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    original = Path.iterdir

    def fail_for_tests(path: Path):
        if path == tests_dir:
            raise PermissionError("secret filesystem topology")
        return original(path)

    monkeypatch.setattr(Path, "iterdir", fail_for_tests)

    report = HermeticTestService(root=tmp_path).run(".")
    encoded = json.dumps(report.to_jsonable())

    assert report.exit_code == 4
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_SUITE_UNAVAILABLE"
    assert "secret filesystem topology" not in encoded


def test_single_input_permission_failure_is_unavailable_safety_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.manifest import hermetic_test_io

    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)

    def deny_read(*args: object, **kwargs: object) -> bytes:
        del args, kwargs
        raise confined_files.ConfinedFileError("file_unavailable", "secret filesystem topology")

    monkeypatch.setattr(hermetic_test_io, "read_confined_file", deny_read)

    report = HermeticTestService(root=tmp_path).run("orders_daily")
    encoded = json.dumps(report.to_jsonable())

    assert report.exit_code == 4
    assert report.tests[0].errors[0]["code"] == "DPONE_TEST_INPUT_UNAVAILABLE"
    assert "secret filesystem topology" not in encoded


def test_cli_test_does_not_construct_environment_backed_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}, {"id": 2}])
    monkeypatch.chdir(tmp_path)

    def forbidden(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("environment-backed CLI setup is forbidden")

    monkeypatch.setattr("dpone.cli.main.setup_logging", forbidden)
    monkeypatch.setattr("dpone.cli.main.AppContext.from_env", forbidden)

    code, stdout, stderr = _run_cli(["test", "orders_daily"], capsys)

    assert code == 0, stderr
    assert "dpone test: PASS" in stdout


def test_suite_isolates_invalid_manifest_and_runs_valid_sibling(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, pipeline_id="valid", mode="full_refresh", unique_key=None)
    _write_test(tmp_path, pipeline_id="valid", fixture="fixtures/valid.input.jsonl", rows=1)
    _write_jsonl(tmp_path / "tests/fixtures/valid.input.jsonl", [{"id": 1}])
    (tmp_path / "tests/invalid.test.yaml").write_text("kind: [unterminated", encoding="utf-8")

    report = HermeticTestService(root=tmp_path).run(".")

    assert report.exit_code == 2
    assert report.counts == {"total": 2, "passed": 1, "failed": 0, "blocked": 1}
    assert {test.name for test in report.tests} == {"invalid", "orders_daily_happy_path"}


def test_generated_schemas_validate_passed_and_blocked_reports(tmp_path: Path) -> None:
    source_schema = json.loads((Path("src/dpone/schema/test-manifest.schema.json")).read_text(encoding="utf-8"))
    docs_schema = json.loads((Path("docs/schemas/gitops/test-manifest.schema.json")).read_text(encoding="utf-8"))
    assert source_schema == docs_schema

    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    test_path = _write_test(tmp_path, rows=1)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [{"id": 1}])
    manifest = yaml.safe_load(test_path.read_text(encoding="utf-8"))
    passed = HermeticTestService(root=tmp_path).run("orders_daily")
    blocked = HermeticTestService(root=tmp_path).run("missing")

    Draft202012Validator(source_schema).validate(manifest)
    suite_schema = json.loads((Path("docs/schemas/gitops/test-suite-report.schema.json")).read_text(encoding="utf-8"))
    validator = Draft202012Validator(suite_schema)
    validator.validate(passed.to_jsonable())
    validator.validate(blocked.to_jsonable())
    assert blocked.exit_code == 2
    assert blocked.tests[0].errors[0]["code"] == "DPONE_TEST_INPUT_NOT_FOUND"


def test_importing_hermetic_service_does_not_load_runtime_or_orchestrator_sdks() -> None:
    command = (
        "import json,sys; import dpone.services.hermetic_test_service; "
        "print(json.dumps(sorted(name for name in sys.modules "
        "if name.startswith(('dpone.runtime','hvac','airflow','kubernetes')))))"
    )

    completed = subprocess.run(
        [sys.executable, "-c", command],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == []


def test_cli_test_path_does_not_load_runtime_or_orchestrator_sdks(tmp_path: Path) -> None:
    _write_pipeline(tmp_path, mode="full_refresh", unique_key=None)
    _write_test(tmp_path, rows=0)
    _write_jsonl(tmp_path / "tests/fixtures/orders_daily.input.jsonl", [])
    command = (
        "import json,os,sys; os.chdir(sys.argv[1]); from dpone.cli.main import main; "
        "code=0; "
        "\ntry: main(['test','orders_daily'])"
        "\nexcept SystemExit as exc: code=int(exc.code or 0)"
        "\nprint(json.dumps({'code':code,'modules':sorted(name for name in sys.modules "
        "if name.startswith(('dpone.runtime','hvac','airflow','kubernetes')))}))"
    )

    completed = subprocess.run(
        [sys.executable, "-c", command, str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout.splitlines()[-1])

    assert payload == {"code": 0, "modules": []}
