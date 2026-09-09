"""Public compile CLI uses the service and reports one complete publication."""

import json

import jsonschema
import pytest

from dpone.app import dbt_workspace_composition
from dpone.commands import dbt_workspace_authoring_cmd
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactPublicationError
from dpone.services.dbt_workspace import DbtWorkspaceService
from tests.test_dbt_workspace_artifact_writer import _case
from tests.test_dbt_workspace_authoring_cli import _invoke
from tests.test_dbt_workspace_compile_service import _service
from tests.test_dbt_workspace_discovery import _project


def _arguments(root, output, mode="json"):
    return ["dbt", "workspace", "compile", "--root", str(root), "--output-dir", str(output), "--format", mode]


def test_compile_requires_output_directory_without_constructing_writer(tmp_path, capsys, monkeypatch):
    def forbidden():
        raise AssertionError("writer must remain lazy")

    monkeypatch.setattr(dbt_workspace_composition, "_workspace_writer", forbidden)
    assert _invoke(["dbt", "workspace", "compile", "--help"], capsys)[0] == 0
    assert _invoke(["dbt", "workspace", "compile", "--root", str(tmp_path)], capsys)[0] == 2
    code, output = _invoke(_arguments(tmp_path, tmp_path / "output"), capsys)
    assert code == 2
    result = json.loads(output.out)
    jsonschema.validate(result, dbt_schema_contracts()[result["schema"]])
    assert result["blockers"][0]["code"] == "DPONE_DBT_NO_PUBLISH_MODELS"
    assert not (tmp_path / "output").exists()


def test_missing_manifest_returns_both_project_rows_and_no_output(tmp_path, capsys):
    _project(tmp_path, "alpha", name="alpha")
    _project(tmp_path, "beta", name="beta")
    code, output = _invoke(_arguments(tmp_path, tmp_path / "output"), capsys)
    report = json.loads(output.out)
    assert code == 2 and not report["passed"]
    assert len(report["check"]["projects"]) == 2
    assert report["release_id"] is report["subject_sha256"] is None
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("mode", ["text", "json"])
def test_cli_publishes_one_real_tree_and_retries_identically(tmp_path, capsys, monkeypatch, mode):
    # Bundle, pack, source verification and atomic publication are real.
    # Selection and route receipts are synthetic fixtures, not live certification.
    writer, check, calls, publications = _case(tmp_path)

    class Service(DbtWorkspaceService):
        def check(self, root):
            return check

    service = Service(discovery=object(), compiler_factory=object(), writer_factory=lambda: writer)
    monkeypatch.setattr(dbt_workspace_authoring_cmd, "build_dbt_workspace_service", lambda: service)
    destination = tmp_path / "output"
    code, output = _invoke(_arguments(tmp_path, destination, mode), capsys)
    assert code == 0
    assert calls == ["alpha", "beta"] and publications == [destination]
    if mode == "json":
        assert output.err == ""
        report = json.loads(output.out)  # Exactly one JSON document.
        jsonschema.validate(report, dbt_schema_contracts()[report["schema"]])
        assert report["release_id"] == json.loads((destination / "release-set.json").read_bytes())["release_id"]
    else:
        assert "PASS" in output.out and "Release: sha256:" in output.out
        assert "WARNING DPONE_DBT_MANIFEST_ADAPTER_SCHEMA_EXTENSION" in output.err
    before = (destination / "release-set.json").stat()
    assert _invoke(_arguments(tmp_path, destination, mode), capsys)[0] == 0
    after = (destination / "release-set.json").stat()
    assert (before.st_ino, before.st_mtime_ns) == (after.st_ino, after.st_mtime_ns)


@pytest.mark.parametrize("mode", ["text", "json"])
def test_uncertain_publication_is_exit_five_with_retained_diagnostics(tmp_path, capsys, monkeypatch, mode):
    service, _, _ = _service(tmp_path, failure=DbtArtifactPublicationError("PRIVATE"))
    monkeypatch.setattr(dbt_workspace_authoring_cmd, "build_dbt_workspace_service", lambda: service)
    code, output = _invoke(_arguments(tmp_path, tmp_path / "output", mode), capsys)
    assert code == 5 and "PRIVATE" not in output.out + output.err
    if mode == "json":
        report = json.loads(output.out)
        jsonschema.validate(report, dbt_schema_contracts()[report["schema"]])
        assert not report["passed"] and len(report["check"]["projects"]) == 2
        assert report["release_id"] is report["source_snapshot_sha256"] is report["subject_sha256"] is None
    else:
        assert "FAIL" in output.out and "DPONE_DBT_OUTPUT_WRITE_FAILED" in output.err
        assert "Release:" not in output.out


def test_discovery_and_check_do_not_construct_execution_dependencies(tmp_path, monkeypatch):
    def forbidden():
        raise AssertionError("unexpected execution dependency")

    monkeypatch.setattr(dbt_workspace_composition, "_workspace_writer", forbidden)
    service = dbt_workspace_composition.build_dbt_workspace_service(environment={})
    assert service.discover(tmp_path).passed and service.check(tmp_path).passed


def test_shared_profile_cli_resolves_relative_to_invocation_cwd(tmp_path, capsys, monkeypatch):
    service, _, _ = _service(tmp_path)
    observed = []

    def composition(*, dbt_profiles_dir):
        observed.append(dbt_profiles_dir)
        return service

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dbt_workspace_authoring_cmd, "build_dbt_workspace_service", composition)
    code, _ = _invoke(_arguments(tmp_path, tmp_path / "output") + ["--dbt-profiles-dir", "parse"], capsys)
    assert code == 0 and observed == [tmp_path / "parse"]


@pytest.mark.parametrize("value", ["", " ", "\x00"])
def test_compile_rejects_blank_profile_directory(tmp_path, capsys, value):
    code, _ = _invoke(_arguments(tmp_path, tmp_path / "output") + ["--dbt-profiles-dir", value], capsys)
    assert code == 2 and not (tmp_path / "output").exists()


@pytest.mark.parametrize("kind", ["model", "transfer"])
@pytest.mark.parametrize("mode", ["text", "json"])
def test_relation_collision_is_exit_two_with_both_owners_and_no_release(tmp_path, capsys, monkeypatch, kind, mode):
    writer, check, calls, publications = _case(tmp_path, collision_kind=kind)

    class Service(DbtWorkspaceService):
        def check(self, root):
            return check

    service = Service(discovery=object(), compiler_factory=object(), writer_factory=lambda: writer)
    monkeypatch.setattr(dbt_workspace_authoring_cmd, "build_dbt_workspace_service", lambda: service)
    destination = tmp_path / "output"
    code, output = _invoke(_arguments(tmp_path, destination, mode), capsys)
    assert code == 2 and calls == ["alpha", "beta"]
    assert not publications and not destination.exists()
    if mode == "json":
        report = json.loads(output.out)
        jsonschema.validate(report, dbt_schema_contracts()[report["schema"]])
        assert output.err == "" and not report["passed"]
        assert [row["project_path"] for row in report["check"]["projects"]] == ["alpha", "beta"]
        assert report["check"]["passed"]  # Offline check is not selected-write certification.
        issue = report["blockers"][0]
        assert issue["code"] == "DPONE_DBT_WORKSPACE_TARGET_COLLISION" and issue["path"] == "beta"
        assert "alpha:alpha/" in issue["message"] and "beta:beta/" in issue["message"]
        assert report["release_id"] is report["source_snapshot_sha256"] is report["subject_sha256"] is None
    else:
        assert "FAIL" in output.out and "Release:" not in output.out
        assert "DPONE_DBT_WORKSPACE_TARGET_COLLISION" in output.err
        assert "alpha:alpha/" in output.err and "beta:beta/" in output.err


def test_parse_profile_failure_retains_both_checked_projects(tmp_path, capsys, monkeypatch):
    writer, check, _, publications = _case(tmp_path)
    (tmp_path / "beta" / "profiles.yml").unlink()

    class Service(DbtWorkspaceService):
        def check(self, root):
            return check

    service = Service(discovery=object(), compiler_factory=object(), writer_factory=lambda: writer)
    monkeypatch.setattr(dbt_workspace_authoring_cmd, "build_dbt_workspace_service", lambda: service)
    code, output = _invoke(_arguments(tmp_path, tmp_path / "output"), capsys)
    report = json.loads(output.out)
    assert code == 2 and len(report["check"]["projects"]) == 2
    assert report["blockers"][0]["path"] == "beta"
    assert report["blockers"][0]["code"] == "DPONE_DBT_WORKSPACE_PARSE_PROFILE_INVALID"
    assert report["release_id"] is None and not publications
