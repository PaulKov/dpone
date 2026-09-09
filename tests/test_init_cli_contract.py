from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.cli import main as cli_main


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[object, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return exc.value.code, captured.out, captured.err


def test_bare_init_is_invalid_cli_usage_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init"], capsys)

    assert code == 2
    assert stdout == ""
    assert "dpone init requires" in stderr
    assert "project, domain, pipeline, dag" in stderr
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("args", "invalid_option"),
    [
        (["init", "project", "--recipe", "mssql-to-clickhouse-incremental"], "--recipe"),
        (["init", "project", "--source-type", "mssql"], "--source-type"),
        (["init", "pipeline", "orders_daily", "--source-type", "mssql"], "--source-type"),
        (["init", "domain", "crm", "--airflow"], "--airflow"),
        (["init", "--recipe", "mssql-to-clickhouse-incremental", "project"], "--recipe"),
        (["init", "--source-type", "mssql", "project"], "--source-type"),
        (["init", "--source-type", "mssql", "pipeline", "orders_daily"], "--source-type"),
    ],
)
def test_beginner_init_rejects_target_inapplicable_options_before_writes(
    args: list[str],
    invalid_option: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(args, capsys)

    assert code == 2
    assert stdout == ""
    assert "usage:" in stderr
    assert "error:" in stderr
    assert invalid_option in stderr
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("before_target", "after_target", "target", "invalid_option"),
    [
        (
            ["init", "--recipe", "mssql-to-clickhouse-incremental", "project"],
            ["init", "project", "--recipe", "mssql-to-clickhouse-incremental"],
            "project",
            "--recipe",
        ),
        (
            ["init", "--source-type", "mssql", "pipeline", "orders_daily"],
            ["init", "pipeline", "orders_daily", "--source-type", "mssql"],
            "pipeline",
            "--source-type",
        ),
    ],
)
def test_beginner_init_invalid_option_diagnostic_is_order_independent(
    before_target: list[str],
    after_target: list[str],
    target: str,
    invalid_option: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    before_code, before_stdout, before_stderr = _run_cli(before_target, capsys)
    after_code, after_stdout, after_stderr = _run_cli(after_target, capsys)

    assert before_code == after_code == 2
    assert before_stdout == after_stdout == ""
    assert before_stderr == after_stderr
    assert f"usage: dpone init {target}" in before_stderr
    assert f"error: {target} does not accept: {invalid_option}" in before_stderr
    assert list(tmp_path.iterdir()) == []


def test_pipeline_name_is_required_by_argparse_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "pipeline", "--airflow"], capsys)

    assert code == 2
    assert stdout == ""
    assert "the following arguments are required: name" in stderr
    assert list(tmp_path.iterdir()) == []


def test_domain_ownership_options_are_required_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "domain", "crm"], capsys)

    assert code == 2
    assert stdout == ""
    assert "--owner-team" in stderr
    assert "--owner-contact" in stderr
    assert "--approver-team" in stderr
    assert list(tmp_path.iterdir()) == []


def test_init_help_exposes_distinct_legacy_project_and_pipeline_surfaces(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, legacy_help, stderr = _run_cli(["init", "--help"], capsys)
    assert code == 0, stderr
    assert "--source-type" in legacy_help
    assert "--out" in legacy_help
    assert "--airflow" not in legacy_help
    assert "--recipe" not in legacy_help
    assert "[{project,domain,pipeline,dag}]" in legacy_help
    assert "Omit a target to use legacy manifest bundle options." in legacy_help

    code, project_help, stderr = _run_cli(["init", "project", "--help"], capsys)
    assert code == 0, stderr
    assert "--airflow" in project_help
    assert "--format" in project_help
    assert "--recipe" not in project_help
    assert "--source-type" not in project_help

    code, pipeline_help, stderr = _run_cli(["init", "pipeline", "--help"], capsys)
    assert code == 0, stderr
    assert "name" in pipeline_help
    assert "--airflow" in pipeline_help
    assert "--recipe" in pipeline_help
    assert "--profile" in pipeline_help
    assert "--answers" in pipeline_help
    assert "--authoring" in pipeline_help
    assert "--format" in pipeline_help
    assert "--source-type" not in pipeline_help
    assert "--out" not in pipeline_help


def test_beginner_init_keeps_valid_options_before_the_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "--airflow", "--format", "json", "project"], capsys)
    assert code == 0, stderr
    assert json.loads(stdout)["passed"] is True

    code, stdout, stderr = _run_cli(
        [
            "init",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
            "pipeline",
            "orders_daily",
        ],
        capsys,
    )
    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    assert payload["pipeline_id"] == "orders_daily"


def test_legacy_init_still_accepts_its_complete_option_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "--source-type",
            "mssql",
            "--sink-type",
            "clickhouse",
            "--source-connection",
            "mssql_dev",
            "--sink-connection",
            "clickhouse_dev",
            "--source-schema",
            "dbo",
            "--source-table",
            "orders",
            "--target-schema",
            "analytics",
            "--target-table",
            "orders",
            "--strategy",
            "incremental",
            "--unique-key",
            "order_id",
            "--out",
            "orders.yaml",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["manifest_path"] == "orders.yaml"
    assert (tmp_path / "orders.yaml").is_file()


@pytest.mark.parametrize(
    "beginner_option",
    (
        ["--airflow"],
        ["--no-airflow"],
        ["--recipe", "mssql-to-clickhouse-incremental"],
        ["--profile", "default"],
        ["--answers", "answers.yaml"],
        ["--authoring", "flow"],
    ),
)
def test_legacy_init_rejects_beginner_only_options_instead_of_ignoring_them(
    beginner_option: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    legacy_args = [
        "init",
        *beginner_option,
        "--source-type",
        "mssql",
        "--sink-type",
        "clickhouse",
        "--source-connection",
        "mssql_dev",
        "--sink-connection",
        "clickhouse_dev",
        "--source-schema",
        "dbo",
        "--source-table",
        "orders",
        "--target-schema",
        "analytics",
        "--target-table",
        "orders",
        "--out",
        "orders.yaml",
    ]

    code, stdout, stderr = _run_cli(legacy_args, capsys)

    assert code == 2
    assert stdout == ""
    assert "legacy init does not accept:" in stderr
    assert beginner_option[0] in stderr
    assert not (tmp_path / "orders.yaml").exists()
