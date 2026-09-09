"""Beginner-facing rendering contracts for domain-first initialization."""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.cli import main as cli_main
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _project(root: Path) -> None:
    assert (
        build_airflow_self_service_service(root=root)
        .init_project(
            airflow=True,
            layout="domain_first",
        )
        .passed
    )


def test_invalid_domain_markdown_uses_hosted_docs_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "domain",
            "CRM Team",
            "--owner-team",
            "data-crm",
            "--owner-contact",
            "crm@example.com",
            "--approver-team",
            "data-platform",
            "--format",
            "md",
        ],
        capsys,
    )

    assert code == 2, stderr
    assert stdout.startswith("# dpone init domain\n")
    assert "https://paulkov.github.io/dpone/errors/DPONE_DOMAIN_ID_INVALID/" in stdout
    assert "'docs_url':" not in stdout


def test_domain_init_next_step_requires_explicit_route_locators(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "domain",
            "crm",
            "--owner-team",
            "data-crm",
            "--owner-contact",
            "crm@example.com",
            "--approver-team",
            "data-platform",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "--route mssql:clickhouse:incremental_merge" in stdout
    assert "--from <source-ref>:<schema>.<table>" in stdout
    assert "--to <sink-ref>:<schema>.<table>" in stdout
    assert "--recipe" not in stdout


def test_init_help_contains_copyable_domain_first_start(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(["init", "project", "--help"], capsys)
    help_text = stdout + stderr

    assert code == 0
    assert "dpone init project --airflow --layout domain-first" in help_text
    assert "dpone init domain crm --owner-team data-crm" in help_text
    assert "--route mssql:clickhouse:incremental_merge" in help_text
