from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.readiness import airflow_local_safe_sample_deployment
from dpone.services.hermetic_test_service import HermeticTestService


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _initialize_project(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, _, stderr = _run_cli(["init", "project", "--airflow"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--recipe", "mssql-to-clickhouse-incremental"],
        capsys,
    )
    assert code == 0, stderr


def test_exact_five_command_offline_journey_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    commands = (
        ["init", "project", "--airflow"],
        ["init", "pipeline", "orders_daily", "--recipe", "mssql-to-clickhouse-incremental"],
        ["check", "orders_daily"],
        ["airflow", "preview", "orders_daily"],
        ["test", "orders_daily"],
    )

    for command in commands:
        code, stdout, stderr = _run_cli(command, capsys)
        assert code == 0, (command, stdout, stderr)
        assert stdout
        assert stderr == ""

    domain = yaml.safe_load((tmp_path / "domains/sales.yaml").read_text(encoding="utf-8"))
    assert domain["dags"]["orders_daily"]["workloads"] == ["orders_daily"]
    assert domain["workloads"]["orders_daily"]["tags"] == ["airflow"]
    deployment = json.loads((tmp_path / ".dpone-cache/current/deployment.json").read_text(encoding="utf-8"))
    index = json.loads((tmp_path / ".dpone-cache/current/airflow-index.json").read_text(encoding="utf-8"))
    assert deployment["runnable"] is False
    assert index["dag_specs"][0]["id"] == "orders_daily"


@pytest.mark.parametrize(
    "target_kind",
    ("id", "directory", "file", "absolute"),
)
def test_beginner_commands_share_pipeline_reference_resolution(
    target_kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _initialize_project(capsys)
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    targets = {
        "id": "orders_daily",
        "directory": "pipelines/orders_daily",
        "file": "pipelines/orders_daily/pipeline.yaml",
        "absolute": source.as_posix(),
    }
    target = targets[target_kind]

    for command in (
        ["check", target, "--format", "json"],
        ["airflow", "preview", target, "--format", "json"],
        ["test", target, "--format", "json"],
    ):
        code, stdout, stderr = _run_cli(command, capsys)
        assert code == 0, (command, stdout, stderr)

    code, stdout, stderr = _run_cli(
        ["run", target, "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )
    assert code == 3, stderr
    payload = json.loads(stdout)
    assert payload["safe_sample"]["source_snapshot"] == {
        "pipeline_id": "orders_daily",
        "path": "pipelines/orders_daily/pipeline.yaml",
        "sha256": payload["safe_sample"]["source_snapshot"]["sha256"],
    }
    assert payload["safe_sample"]["source_snapshot"]["sha256"].startswith("sha256:")
    assert payload["safe_sample"]["execution_mode"] == "local_handoff"
    assert payload["result"]["extracted_rows"] == 0


def test_hermetic_test_does_not_fall_back_to_unrelated_basename(tmp_path: Path) -> None:
    service = HermeticTestService(root=tmp_path)
    (tmp_path / "pipelines/orders_daily").mkdir(parents=True)
    (tmp_path / "pipelines/orders_daily/pipeline.yaml").write_text(
        "kind: dpone.flow.v1\n",
        encoding="utf-8",
    )

    report = service.run("missing/orders_daily")

    assert report.passed is False
    assert report.tests[0].errors[0]["path"] == "missing/orders_daily/pipeline.yaml"


def test_safe_sample_reference_failure_precedes_durable_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["run", "missing_pipeline", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 4, stderr
    assert json.loads(stdout)["result"]["errors"][0]["code"] == "DPONE_PIPELINE_SOURCE_NOT_FOUND"
    assert not (tmp_path / ".dpone-cache").exists()


def test_bare_test_id_cannot_run_test_for_different_source_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _initialize_project(capsys)
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    source["metadata"]["id"] = "customers_daily"
    source_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(["test", "orders_daily", "--format", "json"], capsys)

    assert code == 2, stderr
    payload = json.loads(stdout)
    assert payload["tests"][0]["errors"][0]["code"] == "DPONE_PIPELINE_ID_MISMATCH"


def test_invalid_pipeline_target_does_not_echo_raw_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    raw_target = "../Orders Secret"

    code, stdout, stderr = _run_cli(["check", raw_target, "--format", "json"], capsys)

    assert code == 4, stderr
    assert raw_target not in stdout
    assert json.loads(stdout)["errors"][0]["entity"] == {"kind": "pipeline", "id": "pipeline"}


def test_failed_sample_policy_creates_no_release_cache_or_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _initialize_project(capsys)

    code, stdout, stderr = _run_cli(
        [
            "run",
            "orders_daily",
            "--sample",
            "1000",
            "--target",
            "temporary",
            "--environment",
            "production",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    assert json.loads(stdout)["result"]["errors"][0]["code"] == "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"
    assert not (tmp_path / ".dpone-cache").exists()
    assert not tuple(tmp_path.rglob("safe-sample-execution-plan.json"))


def test_metadata_less_pipeline_cannot_materialize_safe_sample_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "pipeline.yaml"
    source.write_text(
        "\n".join(
            (
                "schema: dpone.pipeline.v1",
                "processes:",
                "  - name: orders_daily",
                "    source:",
                "      type: mssql",
                "      connection_ref: mssql_dev",
                "      table: {schema: dbo, name: orders}",
                "    sink:",
                "      type: clickhouse",
                "      connection_ref: clickhouse_dev",
                "      table: {schema: analytics, name: orders}",
                "      strategy: {mode: incremental_merge}",
                "",
            )
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        ["run", "pipeline.yaml", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code != 0, stderr
    payload = json.loads(stdout)
    assert payload["result"]["errors"][0]["code"] == "DPONE_PIPELINE_ID_INVALID"
    assert payload["safe_sample"]["source_snapshot"] is None
    assert not (tmp_path / ".dpone-cache").exists()
    assert not tuple(tmp_path.rglob("safe-sample-execution-plan.json"))


def test_source_change_after_local_release_blocks_runtime_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _initialize_project(capsys)
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    original_ensure = airflow_local_safe_sample_deployment.ensure_local_safe_sample_deployment

    def mutate_after_release(**kwargs: object) -> object:
        result = original_ensure(**kwargs)
        source_path.write_text(
            source_path.read_text(encoding="utf-8") + "\n# concurrent mutation\n",
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(
        "dpone.commands.run_safe_sample_cmd.ensure_local_safe_sample_deployment",
        mutate_after_release,
    )

    code, stdout, stderr = _run_cli(
        ["run", "orders_daily", "--sample", "1000", "--target", "temporary", "--format", "json"],
        capsys,
    )

    assert code == 4, stderr
    payload = json.loads(stdout)
    assert payload["result"]["errors"][0]["code"] == "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD"
    assert not tuple(tmp_path.rglob("safe-sample-execution-plan.json"))
