"""The CLI cannot certify a freeze or reset an attempted retirement."""

import json

import pytest

from tests.test_publication_schema_command import parser


def run(arguments):
    args = parser().parse_args(["publication-authority", "retire", *arguments])
    assert args._command.requires_app_context is False
    return args._command.run(args, None)


def test_help_is_source_free_and_does_not_offer_admission_flags(monkeypatch, capsys):
    monkeypatch.setenv("DPONE_RUNTIME_CONNECTION_CONTEXT", "/private/unavailable")
    with pytest.raises(SystemExit) as result:
        run(["plan", "--help"])
    assert result.value.code == 0
    output = capsys.readouterr().out
    assert "--connection-ref" in output
    assert "--journal" not in output and "--force" not in output and "--freeze" not in output


def test_standalone_cannot_plan_without_trusted_observer(tmp_path, capsys):
    code = run(["plan", "--environment", "prod", "--connection-ref", "metadata", "--plan-file", str(tmp_path / "plan")])
    assert code == 2
    assert json.loads(capsys.readouterr().out)["reason_code"] == "held_retirement_observer_required"
    assert not (tmp_path / "plan").exists()


@pytest.mark.parametrize("action", ["apply", "verify"])
def test_mutation_and_readback_both_require_exact_confirmation(tmp_path, action):
    with pytest.raises(SystemExit) as result:
        run([action, "--environment", "prod", "--plan-file", str(tmp_path / "plan")])
    assert result.value.code == 2
