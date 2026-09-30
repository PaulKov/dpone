"""Registered operator CLI is context-free, fail-closed and machine-readable."""

import argparse
import json

import pytest

from dpone.commands.registry_top import top_level_commands


def parser():
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(required=True)
    commands = top_level_commands()
    assert any(command.name == "publication-authority" for command in commands), "schema CLI is not registered"
    for command in commands:
        command.register(sub)
    return root


def run(arguments):
    args = parser().parse_args(["publication-authority", "schema", *arguments])
    assert args._command.requires_app_context is False
    return args._command.run(args, None)


def test_help_needs_no_runtime_context_or_database(monkeypatch, capsys):
    monkeypatch.setenv("DPONE_RUNTIME_CONNECTION_CONTEXT", "/missing-private-context")
    with pytest.raises(SystemExit) as result:
        run(["--help"])
    assert result.value.code == 0
    output = capsys.readouterr().out
    assert "plan" in output and "apply" in output and "inspect" in output
    assert "/missing-private-context" not in output


def test_plan_without_verified_context_is_blocked_json(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DPONE_RUNTIME_CONNECTION_CONTEXT", raising=False)
    code = run(["plan", "--connection-ref", "metadata", "--environment", "prod", "--plan-file", str(tmp_path / "plan")])
    assert code == 2
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "blocked"
    assert result["reason_code"] == "verified_runtime_context_required"
    assert not (tmp_path / "plan").exists()


def test_apply_requires_explicit_digest_at_argument_boundary(tmp_path):
    with pytest.raises(SystemExit) as result:
        run(["apply", "--environment", "prod", "--plan-file", str(tmp_path / "plan")])
    assert result.value.code == 2


@pytest.mark.parametrize("action", ["apply", "inspect"])
def test_unreadable_plan_is_operational_failure_without_private_path(tmp_path, action, capsys):
    path = tmp_path / "sensitive-path"
    arguments = [action, "--environment", "prod", "--plan-file", str(path)]
    if action == "apply":
        arguments.extend(["--confirm-digest", "0" * 64])
    assert run(arguments) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "outcome_unknown"
    assert "sensitive-path" not in output.out + output.err


@pytest.mark.parametrize("mode,expected", [("help", 0), ("plan", 2), ("inspect", 1), ("apply", 2)])
def test_actual_cli_main_never_builds_business_context(tmp_path, monkeypatch, capsys, mode, expected):
    from dpone.cli import main as cli_main

    monkeypatch.delenv("DPONE_RUNTIME_CONNECTION_CONTEXT", raising=False)
    monkeypatch.setattr(cli_main.AppContext, "from_env", lambda **_: pytest.fail("business context construction"))
    monkeypatch.setattr(cli_main, "setup_logging", lambda: pytest.fail("business logging configuration"))
    arguments = ["publication-authority", "schema"]
    if mode == "help":
        arguments.append("--help")
    else:
        arguments.extend([mode, "--environment", "prod", "--plan-file", str(tmp_path / "private-absent-plan")])
        if mode == "plan":
            arguments.extend(["--connection-ref", "metadata"])
    with pytest.raises(SystemExit) as result:
        cli_main.main(arguments)
    assert result.value.code == expected
    output = capsys.readouterr()
    assert "private-absent-plan" not in output.out + output.err
    if mode in {"plan", "inspect"}:
        assert json.loads(output.out)["contract"] == "dpone.publication-schema-result.v1"
