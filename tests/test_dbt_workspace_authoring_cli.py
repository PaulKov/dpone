"""Author-facing CLI discovers and checks locally without an application context."""

import json
from pathlib import Path

import jsonschema
import pytest

from dpone.app import dbt_workspace_composition
from dpone.cli import main as cli_main
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from tests.test_dbt_publish_atomicity import DEMO
from tests.test_dbt_workspace_discovery import _project
from tests.test_dbt_workspace_inputs import _inputs


def _invoke(arguments, capsys):
    with pytest.raises(SystemExit) as result:
        cli_main.main(arguments)
    return result.value.code, capsys.readouterr()


def test_discover_json_has_complete_project_classification(tmp_path: Path, capsys) -> None:
    _project(tmp_path, "new_team/analytics", name="analytics")
    _project(tmp_path, "quality", name="quality", policy=None)
    code, output = _invoke(["dbt", "workspace", "discover", "--root", str(tmp_path), "--format", "json"], capsys)
    assert code == 0
    report = json.loads(output.out)
    jsonschema.validate(report, dbt_schema_contracts()[report["schema"]])
    assert report["schema"] == "dpone.dbt-workspace-discovery.v1"
    assert [row["publishing"] for row in report["projects"]] == [True, False]


def test_check_missing_manifest_retains_both_projects(tmp_path: Path, capsys) -> None:
    _project(tmp_path, "a", name="a")
    _project(tmp_path, "b", name="b")
    code, output = _invoke(["dbt", "workspace", "check", "--root", str(tmp_path), "--format", "json"], capsys)
    assert code == 2
    report = json.loads(output.out)
    jsonschema.validate(report, dbt_schema_contracts()[report["schema"]])
    assert not report["passed"] and len(report["projects"]) == 2
    assert all(row["report"]["blockers"][0]["code"] == "DPONE_DBT_MANIFEST_MISSING" for row in report["projects"])
    assert not (tmp_path / "a/target").exists()


def test_text_failure_keeps_diagnostics_on_stderr(tmp_path: Path, capsys) -> None:
    _project(tmp_path, "a", name="a")
    code, output = _invoke(["dbt", "workspace", "check", "--root", str(tmp_path)], capsys)
    assert code == 2
    assert "FAIL" in output.out and "a" in output.out
    assert "DPONE_DBT_MANIFEST_MISSING" in output.err


@pytest.mark.parametrize("command", ["discover", "check"])
def test_workspace_authoring_help_and_empty_root(tmp_path: Path, capsys, monkeypatch, command: str) -> None:
    assert _invoke(["dbt", "workspace", command, "--help"], capsys)[0] == 0
    monkeypatch.chdir(tmp_path)
    code, output = _invoke(["dbt", "workspace", command, "--format", "json"], capsys)
    assert code == 0 and json.loads(output.out)["passed"]


def test_check_composition_never_relaxes_route_certification(tmp_path: Path, monkeypatch) -> None:
    _project(tmp_path, "a", name="a")
    observed = []
    real = dbt_workspace_composition.build_dbt_dpone_compiler

    def spy(**kwargs):
        observed.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(dbt_workspace_composition, "build_dbt_dpone_compiler", spy)
    report = dbt_workspace_composition.build_dbt_workspace_service(environment={}).check(tmp_path)
    assert not report.passed
    assert observed[0]["require_certified_routes"] is True


def test_discover_rejects_global_override_without_disclosing_it(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("DPONE_DBT_PUBLISH_PROFILES", "SECRET")
    code, output = _invoke(["dbt", "workspace", "discover", "--root", str(tmp_path), "--format", "json"], capsys)
    assert code == 2 and "SECRET" not in output.out + output.err
    assert not json.loads(output.out)["passed"]


@pytest.mark.parametrize("kind", ["policy_schema_array", "manifest_null_constraints"])
def test_malformed_input_is_project_validation_failure_not_internal_abort(tmp_path: Path, capsys, kind: str) -> None:
    _, manifest, policy = _inputs(tmp_path)
    (manifest.parent.parent / "dbt_project.yml").write_bytes((DEMO / "dbt_project.yml").read_bytes())
    if kind == "policy_schema_array":
        policy.write_text("schema: []\n")
        expected = "DPONE_DBT_PROFILES_INVALID"
    else:
        value = json.loads(manifest.read_bytes())
        model = next(node for node in value["nodes"].values() if node.get("resource_type") == "model")
        next(iter(model["columns"].values()))["constraints"] = None
        manifest.write_text(json.dumps(value))
        expected = "DPONE_DBT_MANIFEST_INVALID"
    _project(tmp_path, "other", name="other")
    code, output = _invoke(["dbt", "workspace", "check", "--root", str(tmp_path), "--format", "json"], capsys)
    assert code == 2
    report = json.loads(output.out)
    assert len(report["projects"]) == 2
    assert report["projects"][0]["report"]["blockers"][0]["code"] == expected
    assert report["projects"][1]["report"]["blockers"][0]["code"] == "DPONE_DBT_MANIFEST_MISSING"
