from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.cli import main as cli_main
from dpone.gitops.schema_validation import GitOpsSchemaValidator


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, dict[str, object], str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out else {}
    return int(exc.value.code or 0), payload, captured.err


def _init_pipeline(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, _, stderr = _run_cli(
        ["init", "project", "--airflow", "--format", "json"],
        capsys,
    )
    assert code == 0, stderr
    code, _, stderr = _run_cli(
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


@pytest.mark.parametrize(
    "args",
    [
        ["check", "../outside", "--format", "json"],
        ["airflow", "preview", "../outside", "--format", "json"],
        ["airflow", "explain", "../outside", "--format", "json"],
    ],
)
def test_authoring_commands_preserve_security_exit_code_without_mutation(
    args: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, payload, stderr = _run_cli(args, capsys)

    assert code == 4, stderr
    assert payload["passed"] is False
    errors = payload["errors"]
    assert isinstance(errors, list)
    assert errors[0]["code"] == "DPONE_PIPELINE_SOURCE_PATH_INVALID"
    assert not (tmp_path / ".dpone-cache").exists()


def test_check_treats_dangling_pipeline_symlink_as_security_violation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    try:
        (tmp_path / "pipeline.yaml").symlink_to(tmp_path / "missing.yaml")
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    code, payload, stderr = _run_cli(
        ["check", "pipeline.yaml", "--format", "json"],
        capsys,
    )

    assert code == 4, stderr
    assert payload["errors"][0]["code"] == "DPONE_PIPELINE_SOURCE_PATH_INVALID"
    assert payload["errors"][0]["reason"] == "symlink_forbidden"
    assert not (tmp_path / ".dpone-cache").exists()


def test_selected_connection_check_reports_actual_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _init_pipeline(capsys)

    code, payload, stderr = _run_cli(
        [
            "check",
            ".",
            "--select",
            "id:orders_daily",
            "--connections",
            "--environment",
            "dev",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert payload["mode"] == "connections"
    assert payload["network"] is False
    assert payload["secrets"] is False
    assert payload["source_queries"] is False
    assert payload["selected_checks"][0]["details"]["handshake"] == "configuration_only"


def test_preview_producer_emits_schema_valid_positive_artifact_sizes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _init_pipeline(capsys)

    code, _, stderr = _run_cli(
        ["airflow", "preview", "orders_daily", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    index = json.loads((tmp_path / ".dpone-cache" / "current" / "airflow-index.json").read_text(encoding="utf-8"))
    assert (
        GitOpsSchemaValidator().validate(
            index,
            expected_kind="dpone.airflow-deployment-index.v1",
        )
        == ()
    )
    artifacts = [*index["dag_specs"], *index["workload_packs"]]
    assert artifacts
    assert all(artifact["bytes"] > 0 for artifact in artifacts)


def test_selected_live_check_preserves_dependency_exit_and_planned_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _init_pipeline(capsys)

    code, payload, stderr = _run_cli(
        [
            "check",
            ".",
            "--select",
            "id:orders_daily",
            "--live",
            "--environment",
            "dev",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 3, stderr
    assert payload["network"] is False
    assert payload["secrets"] is False
    assert payload["source_queries"] is False
    assert payload["planned_network"] is True
    assert payload["planned_secrets"] is True
    assert payload["planned_source_queries"] == "bounded_probes"
    assert payload["errors"][0]["code"] == "DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED"


def test_connection_and_live_modes_are_rejected_before_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, payload, stderr = _run_cli(
        ["check", ".", "--connections", "--live", "--format", "json"],
        capsys,
    )

    assert code == 2
    assert payload == {}
    assert "not allowed with argument" in stderr
    assert tuple(tmp_path.iterdir()) == ()
