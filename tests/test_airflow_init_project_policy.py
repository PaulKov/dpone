from __future__ import annotations

import json
import shlex
from pathlib import Path

import pytest
import yaml

from dpone.cli import main as cli_main


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _write_project_config(root: Path, *, airflow_enabled: bool) -> None:
    (root / "dpone.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.project.v1",
                "authoring": {"primary_source_policy": "one_per_pipeline"},
                "airflow": {
                    "enabled": airflow_enabled,
                    "index_path": ".dpone-cache/current/airflow-index.json",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _pipeline_domain(root: Path) -> dict[str, object]:
    return yaml.safe_load((root / "domains/sales.yaml").read_text(encoding="utf-8"))


def test_pipeline_inherits_airflow_enabled_from_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    project_code, _, project_stderr = _run_cli(["init", "project", "--airflow"], capsys)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--format",
            "json",
        ],
        capsys,
    )

    assert project_code == 0, project_stderr
    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["pipeline_id"] == "orders_daily"
    assert payload["airflow_enabled"] is True
    assert payload["airflow_source"] == "project"
    domain = _pipeline_domain(tmp_path)
    assert domain["dags"] == {
        "orders_daily": {
            "workloads": ["orders_daily"],
            "schedule": None,
            "start_date": "2026-01-01",
            "catchup": False,
        }
    }
    assert domain["workloads"]["orders_daily"]["tags"] == ["airflow"]  # type: ignore[index]


def test_pipeline_no_airflow_overrides_enabled_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project_config(tmp_path, airflow_enabled=True)

    code, stdout, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--no-airflow", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    assert json.loads(stdout)["airflow_source"] == "explicit"
    domain = _pipeline_domain(tmp_path)
    assert "dags" not in domain
    assert domain["workloads"]["orders_daily"]["tags"] == []  # type: ignore[index]


def test_pipeline_airflow_overrides_disabled_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_project_config(tmp_path, airflow_enabled=False)

    code, stdout, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--airflow", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    assert json.loads(stdout)["airflow_source"] == "explicit"
    assert "dags" in _pipeline_domain(tmp_path)


def test_pipeline_without_project_config_preserves_legacy_non_airflow_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["airflow_enabled"] is False
    assert payload["airflow_source"] == "legacy_default"
    assert "dags" not in _pipeline_domain(tmp_path)


def test_invalid_pipeline_id_fix_preserves_all_authoring_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    answers = "answers/team orders.yaml"

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "Orders Daily",
            "--recipe",
            "team/orders@1",
            "--authoring",
            "flow",
            "--profile",
            "production",
            "--answers",
            answers,
            "--no-airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2, stderr
    payload = json.loads(stdout)
    command = payload["errors"][0]["fixes"][0]["command"]
    assert shlex.split(command) == [
        "dpone",
        "init",
        "pipeline",
        "orders_daily",
        "--recipe",
        "team/orders@1",
        "--authoring",
        "flow",
        "--no-airflow",
        "--profile",
        "production",
        "--answers",
        answers,
    ]
    assert not (tmp_path / "pipelines").exists()


def test_invalid_pipeline_id_fix_preserves_route_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "Orders Daily",
            "--route",
            "mssql:clickhouse:incremental_merge",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2, stderr
    command = json.loads(stdout)["errors"][0]["fixes"][0]["command"]
    assert shlex.split(command) == [
        "dpone",
        "init",
        "pipeline",
        "orders_daily",
        "--route",
        "mssql:clickhouse:incremental_merge",
        "--authoring",
        "flow",
        "--airflow",
    ]
    assert not (tmp_path / "pipelines").exists()


@pytest.mark.parametrize(
    "content",
    (
        b"schema: dpone.project.v1\nairflow: [\n",
        b"schema: wrong.project.v1\nairflow:\n  enabled: true\n",
        b"schema: dpone.project.v1\nairflow:\n  enabled: true\n  enabled: false\n",
        b'schema: dpone.project.v1\nairflow:\n  enabled: "yes"\n',
    ),
)
def test_invalid_project_config_fails_before_pipeline_writes(
    content: bytes,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dpone.yaml").write_bytes(content)

    code, stdout, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--format", "json"],
        capsys,
    )

    assert code == 2, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_PROJECT_CONFIG_INVALID"
    assert payload["errors"][0]["stage"] == "init_pipeline"
    assert not (tmp_path / "pipelines").exists()
    assert not (tmp_path / "domains").exists()
    assert not (tmp_path / "tests").exists()


def test_explicit_airflow_override_still_rejects_malformed_project_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dpone.yaml").write_text("schema: [\n", encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--airflow", "--format", "json"],
        capsys,
    )

    assert code == 2, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_PROJECT_CONFIG_INVALID"
    assert not (tmp_path / "pipelines").exists()


@pytest.mark.parametrize(
    "args",
    (
        ["init", "pipeline", "orders_daily", "--airflow", "--no-airflow"],
        ["init", "pipeline", "orders_daily", "--no-airflow", "--airflow"],
        ["init", "--airflow", "pipeline", "orders_daily", "--no-airflow"],
        ["init", "--no-airflow", "pipeline", "orders_daily", "--airflow"],
    ),
)
def test_pipeline_rejects_conflicting_airflow_overrides_before_writes(
    args: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(args, capsys)

    assert code == 2
    assert stdout == ""
    assert "usage:" in stderr
    assert "--airflow" in stderr
    assert "--no-airflow" in stderr
    assert list(tmp_path.iterdir()) == []


def test_project_rejects_no_airflow_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "project", "--no-airflow"], capsys)

    assert code == 2
    assert stdout == ""
    assert "project does not accept: --no-airflow" in stderr
    assert list(tmp_path.iterdir()) == []


def test_project_config_symlink_is_rejected_before_pipeline_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside-project.yaml"
    outside.write_text(
        "schema: dpone.project.v1\nairflow:\n  enabled: true\n",
        encoding="utf-8",
    )
    (tmp_path / "dpone.yaml").symlink_to(outside)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "pipeline", "orders_daily", "--format", "json"], capsys)

    assert code == 2, stderr
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_PROJECT_CONFIG_INVALID"
    assert not (tmp_path / "pipelines").exists()
    outside.unlink()


def test_oversized_project_config_is_rejected_before_pipeline_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "dpone.yaml").write_bytes(b"#" * (64 * 1024 + 1))
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "pipeline", "orders_daily", "--format", "json"], capsys)

    assert code == 2, stderr
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_PROJECT_CONFIG_INVALID"
    assert not (tmp_path / "pipelines").exists()


def test_deep_project_config_is_rejected_before_pipeline_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    nested = "true"
    for _ in range(20):
        nested = f"{{nested: {nested}}}"
    (tmp_path / "dpone.yaml").write_text(
        f"schema: dpone.project.v1\nairflow:\n  enabled: true\nextra: {nested}\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["init", "pipeline", "orders_daily", "--format", "json"], capsys)

    assert code == 2, stderr
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_PROJECT_CONFIG_INVALID"
    assert not (tmp_path / "pipelines").exists()


def test_inherited_airflow_scaffold_is_deterministic_no_op(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    assert _run_cli(["init", "project", "--airflow"], capsys)[0] == 0
    command = ["init", "pipeline", "orders_daily", "--format", "json"]
    first_code, _, first_stderr = _run_cli(command, capsys)
    before = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    }

    second_code, stdout, second_stderr = _run_cli(command, capsys)

    assert first_code == 0, first_stderr
    assert second_code == 0, second_stderr
    payload = json.loads(stdout)
    assert {change["action"] for change in payload["changes"]} == {"no_op"}
    assert {
        path.relative_to(tmp_path).as_posix(): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    } == before
