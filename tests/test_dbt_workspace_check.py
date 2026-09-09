"""Complete-project planning with injectable compilers and no publication side effects."""

from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts.dbt_publish_models import DbtCompileReport, DbtPublishIssue
from dpone.manifest.dbt_workspace_discovery import DbtWorkspaceDiscovery
from dpone.services.dbt_workspace import DbtWorkspaceService
from tests.test_dbt_publish_atomicity import _compile
from tests.test_dbt_workspace_discovery import _project


def _service(results, calls):
    def factory(root, project):
        class Compiler:
            def build(self, manifest_path, **kwargs):
                calls.append((root, project, manifest_path, kwargs))
                result = results[project.project_name]
                if isinstance(result, Exception):
                    raise result
                return result

        return Compiler()

    return DbtWorkspaceService(discovery=DbtWorkspaceDiscovery(environment={}), compiler_factory=factory)


def _empty_report():
    return DbtCompileReport(manifest_path="target/manifest.json", manifest_schema_version=12)


def test_check_includes_unchanged_projects_and_continues_after_failure(tmp_path: Path) -> None:
    _project(tmp_path, "b", name="b")
    _project(tmp_path, "a", name="a")
    _project(tmp_path, "quality", name="quality", policy=None)
    calls = []
    failed = replace(
        _empty_report(),
        blockers=(DbtPublishIssue("DPONE_DBT_MANIFEST_MISSING", "Missing canonical manifest", "target/manifest.json"),),
    )
    report = _service({"a": failed, "b": _empty_report()}, calls).check(tmp_path)
    assert not report.passed
    assert [row.project.project_path for row in report.projects] == ["a", "b"]
    assert report.projects[1].report.passed
    assert len(calls) == 2
    assert all(call[3]["allow_empty"] is False and call[3]["require_contracts"] is True for call in calls)
    assert calls[0][3]["profiles_path"] == tmp_path / "a/dpone/dbt-publish-profiles.yml"


@pytest.mark.parametrize("kind", ["workflow", "dag", "workload"])
def test_global_identity_collisions_name_both_project_owners(tmp_path: Path, kind: str) -> None:
    _project(tmp_path, "a", name="a")
    _project(tmp_path, "b", name="b")
    base = _compile()
    other = replace(
        base,
        models=tuple(replace(model, workload_id="other__" + model.workload_id) for model in base.models),
        workflows=tuple(
            replace(workflow, workflow="other_" + workflow.workflow, dag_id="other_" + workflow.dag_id)
            for workflow in base.workflows
        ),
    )
    if kind == "workflow":
        other = replace(
            other,
            workflows=tuple(
                replace(workflow, workflow=original.workflow)
                for original, workflow in zip(base.workflows, other.workflows, strict=True)
            ),
        )
    elif kind == "dag":
        other = replace(
            other,
            workflows=tuple(
                replace(workflow, dag_id=original.dag_id)
                for original, workflow in zip(base.workflows, other.workflows, strict=True)
            ),
        )
    else:
        other = replace(other, models=base.models)
    report = _service({"a": base, "b": other}, []).check(tmp_path)
    assert not report.passed
    assert any(
        issue.code == "DPONE_DBT_WORKSPACE_IDENTITY_COLLISION" and "a" in issue.message and "b" in issue.message
        for issue in report.blockers
    )


def test_same_project_local_node_id_is_not_a_global_identity_collision(tmp_path: Path) -> None:
    _project(tmp_path, "a", name="a")
    _project(tmp_path, "b", name="b")
    base = _compile()
    other = replace(
        base,
        models=tuple(replace(model, workload_id="other__" + model.workload_id) for model in base.models),
        workflows=tuple(
            replace(workflow, workflow="other_" + workflow.workflow, dag_id="other_" + workflow.dag_id)
            for workflow in base.workflows
        ),
    )
    report = _service({"a": base, "b": other}, []).check(tmp_path)
    assert report.passed
    assert base.models[0].model.unique_id == other.models[0].model.unique_id


def test_bad_discovery_never_invokes_compiler(tmp_path: Path) -> None:
    _project(tmp_path, "a", name="same")
    _project(tmp_path, "b", name="same")
    calls = []
    report = _service({}, calls).check(tmp_path)
    assert not report.passed and calls == []


def test_io_failure_becomes_failed_project_row_without_leaking_exception(tmp_path: Path) -> None:
    _project(tmp_path, "a", name="a")
    _project(tmp_path, "b", name="b")
    report = _service({"a": OSError("SECRET"), "b": _empty_report()}, []).check(tmp_path)
    assert not report.passed and len(report.projects) == 2
    assert "SECRET" not in str(report.to_dict())


def test_empty_workspace_check_is_noop(tmp_path: Path) -> None:
    calls = []
    report = _service({}, calls).check(tmp_path)
    assert report.passed and report.projects == () and calls == []
