"""The CLI cannot manufacture a trusted deployment observer from arguments."""

import json

import pytest

from tests.test_publication_schema_command import parser


def run(arguments):
    args = parser().parse_args(["publication-authority", "recover", *arguments])
    assert args._command.requires_app_context is False
    return args._command.run(args, None)


def test_recovery_help_is_source_free(monkeypatch, capsys):
    monkeypatch.setenv("DPONE_RUNTIME_CONNECTION_CONTEXT", "/private/unavailable")
    with pytest.raises(SystemExit) as result:
        run(["--help"])
    assert result.value.code == 0
    assert "execute" in capsys.readouterr().out


def test_standalone_cli_cannot_plan_without_injected_observer(tmp_path, capsys):
    code = run(
        [
            "plan",
            "--environment",
            "prod",
            "--plan-file",
            str(tmp_path / "plan"),
            "--connection-ref",
            "metadata",
            "--sink-connection-ref",
            "warehouse",
            "--cluster",
            "cluster",
            "--database",
            "analytics",
            "--target",
            "target",
            "--operation-id",
            "operation",
            "--expected-version",
            "1",
        ]
    )
    assert code == 2
    assert json.loads(capsys.readouterr().out)["reason_code"] == "held_recovery_observer_required"
    assert not (tmp_path / "plan").exists()


def test_execute_requires_confirmation_before_application(tmp_path):
    with pytest.raises(SystemExit) as result:
        run(["execute", "--environment", "prod", "--plan-file", str(tmp_path / "plan")])
    assert result.value.code == 2
