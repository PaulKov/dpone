"""Real CLI/service composition with synthetic resource bytes, not live qualification."""

import json
import shlex

import pytest

from dpone.cli import main as cli_main
from tests.test_dbt_starter import starter_policy, starter_service


def run(argv, capsys):
    with pytest.raises(SystemExit) as caught:
        cli_main.main(argv)
    output = capsys.readouterr()
    return caught.value.code, output.out, output.err


@pytest.fixture
def synthetic_resources(monkeypatch):
    from dpone.adapters.dbt_starter_resources import InstalledDbtStarterResources

    resources = starter_service()._resources
    monkeypatch.setattr(InstalledDbtStarterResources, "files", lambda self: resources.files())


def arguments(target, policy):
    return ["init", "dbt", str(target), "--profiles", str(policy), "--profile", "local", "--workflow", "orders"]


@pytest.mark.parametrize("fmt", ["text", "json", "md"])
@pytest.mark.parametrize("dry_run", [False, True])
def test_real_service_dry_run_and_apply_outputs(tmp_path, capsys, synthetic_resources, fmt, dry_run):
    target = tmp_path / "project with spaces"
    policy = starter_policy(tmp_path)
    argv = arguments(target, policy) + ["--format", fmt] + (["--dry-run"] if dry_run else [])
    code, stdout, stderr = run(argv, capsys)
    assert code == 0 and stderr == ""
    assert target.exists() is not dry_run
    assert "synthetic package fixture" not in stdout
    if fmt == "json":
        payload = json.loads(stdout)
        assert payload["passed"] and len(payload["changes"]) == 23
        assert payload["dry_run"] is dry_run
        if dry_run:
            assert shlex.split(payload["next_command"])[0:4] == ["dpone", "init", "dbt", str(target)]
            assert "--dry-run" not in shlex.split(payload["next_command"])
        else:
            assert "dbt parse" in payload["next_command"] and "dbt check" in payload["next_command"]
    else:
        assert "- next:" in stdout
        assert ("dbt parse" in stdout) is not dry_run


def test_repeat_noop_and_conflict_preserve_user_sql(tmp_path, capsys, synthetic_resources):
    target = tmp_path / "project"
    policy = starter_policy(tmp_path)
    argv = arguments(target, policy) + ["--format", "json"]
    assert run(argv, capsys)[0] == 0
    code, stdout, _ = run(argv, capsys)
    assert code == 0 and all(item["action"] == "no_op" for item in json.loads(stdout)["changes"])
    model = target / "models/orders.sql"
    model.write_text("PRIVATE_SQL_SENTINEL")
    code, stdout, stderr = run(argv, capsys)
    assert code == 1 and stderr == "" and "PRIVATE_SQL_SENTINEL" not in stdout
    assert model.read_text() == "PRIVATE_SQL_SENTINEL"
    from dpone.cli.parser import build_parser

    rerun = build_parser().parse_args(shlex.split(json.loads(stdout)["rerun_command"])[1:])
    assert (rerun.dbt_path, rerun.dbt_profiles, rerun.dbt_profile, rerun.dbt_workflow, rerun.format) == (
        str(target),
        str(policy),
        "local",
        "orders",
        "json",
    )


def test_missing_installed_resources_fail_before_destination(tmp_path, capsys, monkeypatch):
    from dpone.adapters.dbt_starter_resources import InstalledDbtStarterResources

    def missing(self):
        raise ValueError("PRIVATE_SENTINEL")

    monkeypatch.setattr(InstalledDbtStarterResources, "files", missing)
    target = tmp_path / "missing-project"
    code, stdout, stderr = run(arguments(target, starter_policy(tmp_path)) + ["--format", "json"], capsys)
    assert code == 2 and stderr == "" and not target.exists()
    assert "PRIVATE_SENTINEL" not in stdout
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_DBT_STARTER_RESOURCES_INVALID"


def test_help_and_syntax_error_never_construct_starter(monkeypatch, capsys):
    from dpone.readiness.dbt_starter import DbtStarterService

    monkeypatch.setattr(DbtStarterService, "__init__", lambda *a, **k: pytest.fail("unexpected service construction"))
    code, stdout, stderr = run(["init", "dbt", "--help"], capsys)
    assert code == 0 and "--dry-run" in stdout and stderr == ""
    code, stdout, stderr = run(["init", "dbt", "project", "--format", "json"], capsys)
    assert code == 2 and stdout == ""
    assert json.loads(stderr)["errors"][0]["code"] == "DPONE_CLI_USAGE_INVALID"


def test_lock_failure_preserves_service_exit(tmp_path, capsys, monkeypatch, synthetic_resources):
    from contextlib import contextmanager

    import dpone.adapters.project_authoring_lock as locks
    from dpone.ports.project_authoring_lock import ProjectAuthoringLockError

    @contextmanager
    def unavailable(root):
        raise ProjectAuthoringLockError("PRIVATE_SENTINEL")
        yield

    monkeypatch.setattr(locks, "project_authoring_lock", unavailable)
    target = tmp_path / "project"
    code, stdout, stderr = run(arguments(target, starter_policy(tmp_path)) + ["--format", "json"], capsys)
    assert code == 4 and stderr == "" and not target.exists()
    assert "PRIVATE_SENTINEL" not in stdout


def test_invalid_policy_has_no_destination(tmp_path, capsys, synthetic_resources):
    policy = starter_policy(tmp_path)
    policy.write_text("duplicate: 1\nduplicate: PRIVATE_SENTINEL\n")
    target = tmp_path / "project"
    code, stdout, stderr = run(arguments(target, policy) + ["--format", "json"], capsys)
    assert code == 1 and stderr == "" and not target.exists()
    assert "PRIVATE_SENTINEL" not in stdout


@pytest.mark.parametrize("option", ["--profile", "--workflow"])
def test_invalid_selector_does_not_reappear_in_rerun_command(tmp_path, capsys, synthetic_resources, option):
    target = tmp_path / "project"
    argv = arguments(target, starter_policy(tmp_path)) + ["--format", "json"]
    argv[argv.index(option) + 1] = "PRIVATE_SELECTOR_SENTINEL"
    code, stdout, stderr = run(argv, capsys)
    assert code == 1 and stderr == "" and not target.exists()
    assert "PRIVATE_SELECTOR_SENTINEL" not in stdout
    assert "rerun_command" not in json.loads(stdout)


def test_dry_run_next_command_preserves_dash_prefixed_values(tmp_path, capsys, monkeypatch, synthetic_resources):
    from dpone.cli.parser import build_parser

    monkeypatch.chdir(tmp_path)
    policy = starter_policy(tmp_path)
    value = json.loads(policy.read_text().split("\n", 1)[1])
    value["profiles"]["-local"] = value["profiles"].pop("local")
    policy.write_text(json.dumps(value))
    policy.rename(tmp_path / "-policy.yml")
    argv = [
        "init",
        "dbt",
        "--profiles=-policy.yml",
        "--profile=-local",
        "--workflow=orders",
        "--dry-run",
        "--format=json",
        "--",
        "-project",
    ]
    code, stdout, stderr = run(argv, capsys)
    assert code == 0 and stderr == ""
    next_argv = shlex.split(json.loads(stdout)["next_command"])
    parsed = build_parser().parse_args(next_argv[1:])
    assert parsed.dbt_profile == "-local" and parsed.dbt_profiles == "-policy.yml"
    assert not parsed.dbt_dry_run
    assert run(next_argv[1:], capsys)[0] == 0
    model = tmp_path / "-project/models/orders.sql"
    model.write_text("PRIVATE_SQL_SENTINEL")
    code, stdout, stderr = run(next_argv[1:], capsys)
    assert code == 1 and stderr == "" and "PRIVATE_SQL_SENTINEL" not in stdout
    rerun = build_parser().parse_args(shlex.split(json.loads(stdout)["rerun_command"])[1:])
    assert rerun.dbt_profile == "-local" and rerun.dbt_profiles == "-policy.yml"
    assert model.read_text() == "PRIVATE_SQL_SENTINEL"


def test_public_init_does_not_run_dependencies_network_or_policy_discovery(
    tmp_path, capsys, monkeypatch, synthetic_resources
):
    import socket
    import subprocess

    from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry

    def forbidden(*args, **kwargs):
        pytest.fail("offline init crossed an I/O boundary")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(DbtPublishProfileRegistry, "load", forbidden)
    assert run(arguments(tmp_path / "project", starter_policy(tmp_path)), capsys)[0] == 0


def test_failed_apply_preserves_exit_four(tmp_path, capsys, monkeypatch, synthetic_resources):
    from dpone.readiness.airflow_scaffold_apply import ScaffoldFileSystem

    actual = ScaffoldFileSystem.create
    count = 0

    def fail_third(self, file):
        nonlocal count
        count += 1
        if count == 3:
            raise OSError("PRIVATE_SENTINEL")
        return actual(self, file)

    monkeypatch.setattr(ScaffoldFileSystem, "create", fail_third)
    code, stdout, stderr = run(arguments(tmp_path / "project", starter_policy(tmp_path)) + ["--format", "json"], capsys)
    assert code == 4 and stderr == "" and "PRIVATE_SENTINEL" not in stdout
