"""Offline workspace discovery uses project-local policy, never domain names."""

from pathlib import Path

import pytest

from dpone.contracts.dbt_workspace_paths import validate_dbt_project_layout
from dpone.manifest.dbt_workspace_discovery import DbtWorkspaceDiscovery


def _project(
    root: Path, path: str, *, name: str, policy: str | None = "dpone/dbt-publish-profiles.yml", extra: str = ""
) -> Path:
    project = root / path
    project.mkdir(parents=True, exist_ok=True)
    (project / "dbt_project.yml").write_text(f"name: {name}\nversion: '1.0'\n{extra}")
    if policy:
        target = project / policy
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("not parsed during discovery: true\n")
    return project


def test_discovery_is_domain_neutral_complete_sorted_and_does_not_read_secrets(tmp_path: Path) -> None:
    _project(tmp_path, "workloads/new_domain/analytics", name="analytics")
    _project(tmp_path, "dbt/quality", name="quality", policy=None)
    _project(tmp_path, "another", name="another", policy=".dpone/dbt-publish-profiles.yml")
    (tmp_path / "another/profiles.yml").write_bytes(b"\xffSECRET")
    report = DbtWorkspaceDiscovery(environment={}).discover(tmp_path)
    assert report.passed
    rows = report.to_dict()["projects"]
    assert [row["project_path"] for row in rows] == ["another", "dbt/quality", "workloads/new_domain/analytics"]
    assert [row["publishing"] for row in rows] == [True, False, True]
    assert rows[1]["reason"] == "not_configured"
    assert rows[0]["profiles_path"] == ".dpone/dbt-publish-profiles.yml"
    assert rows[0]["manifest_path"] == "target/manifest.json"


@pytest.mark.parametrize(
    "directory",
    [".git", ".worktrees", ".venv", "venv", "node_modules", "target", "logs", "dbt_packages", ".dpone-cache", ".ci"],
)
def test_default_ignored_directories_are_not_projects(tmp_path: Path, directory: str) -> None:
    _project(tmp_path, directory + "/dependency", name="ignored")
    assert DbtWorkspaceDiscovery(environment={}).discover(tmp_path).projects == ()


def test_custom_package_and_target_directories_are_not_projects(tmp_path: Path) -> None:
    _project(
        tmp_path, "dbt/app", name="app", extra="packages-install-path: vendor/packages\ntarget-path: build/artifacts\n"
    )
    _project(tmp_path, "dbt/app/vendor/packages/dependency", name="dependency")
    _project(tmp_path, "dbt/app/build/artifacts/cache", name="cache")
    report = DbtWorkspaceDiscovery(environment={}).discover(tmp_path)
    assert report.passed
    assert [project.project_path for project in report.projects] == ["dbt/app"]
    assert report.projects[0].manifest_path == "build/artifacts/manifest.json"


@pytest.mark.parametrize("value", ["../outside", "/tmp/outside", ".", "{{ env_var('SECRET') }}", "target/../other"])
def test_unsafe_configured_paths_fail_without_interpolation(tmp_path: Path, value: str) -> None:
    _project(tmp_path, "dbt/app", name="app", extra=f'packages-install-path: "{value}"\n')
    report = DbtWorkspaceDiscovery(environment={}).discover(tmp_path)
    assert not report.passed and report.projects[0].reason == "invalid"
    assert report.blockers[0].code == "DPONE_DBT_WORKSPACE_DISCOVERY_INVALID"


def test_ambiguous_local_policy_is_not_silently_dbt_only(tmp_path: Path) -> None:
    root = _project(tmp_path, "dbt/app", name="app")
    _project(tmp_path, "dbt/app", name="app", policy=".dpone/dbt-publish-profiles.yml")
    report = DbtWorkspaceDiscovery(environment={}).discover(tmp_path)
    assert not report.passed
    assert report.projects[0].reason == "invalid"
    assert root.exists()


@pytest.mark.parametrize("second_path,second_name", [("b", "Same"), ("a/nested", "other")])
def test_duplicate_names_and_nested_project_roots_fail(tmp_path: Path, second_path: str, second_name: str) -> None:
    _project(tmp_path, "a", name="same")
    _project(tmp_path, second_path, name=second_name)
    report = DbtWorkspaceDiscovery(environment={}).discover(tmp_path)
    assert not report.passed
    assert report.blockers[-1].code == "DPONE_DBT_WORKSPACE_IDENTITY_COLLISION"


def test_root_project_supported_only_on_its_own(tmp_path: Path) -> None:
    _project(tmp_path, ".", name="root")
    scanner = DbtWorkspaceDiscovery(environment={})
    assert scanner.discover(tmp_path).projects[0].project_path == "."
    assert scanner.discover(tmp_path).passed
    _project(tmp_path, "child", name="child")
    assert not scanner.discover(tmp_path).passed


@pytest.mark.parametrize("in_root", [True, False])
def test_directory_symlink_cannot_hide_or_duplicate_projects(tmp_path: Path, in_root: bool) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    target = _project(workspace if in_root else tmp_path, "real", name="real")
    (workspace / "linked").symlink_to(target, target_is_directory=True)
    assert not DbtWorkspaceDiscovery(environment={}).discover(workspace).passed


def test_symlink_policy_is_rejected_without_reading_target(tmp_path: Path) -> None:
    project = _project(tmp_path, "app", name="app", policy=None)
    (project / "dpone").mkdir()
    (project / "dpone/dbt-publish-profiles.yml").symlink_to("/nonexistent-secret")
    assert not DbtWorkspaceDiscovery(environment={}).discover(tmp_path).passed


@pytest.mark.parametrize("content", ["name: a\nname: b", "name: &a [*a]", "name: {{ env_var('SECRET') }}", "name: 5"])
def test_malformed_metadata_is_a_visible_failed_row(tmp_path: Path, content: str) -> None:
    project = _project(tmp_path, "bad", name="bad")
    (project / "dbt_project.yml").write_text(content)
    _project(tmp_path, "good", name="good")
    report = DbtWorkspaceDiscovery(environment={}).discover(tmp_path)
    assert not report.passed
    assert [row.project_path for row in report.projects] == ["bad", "good"]
    assert report.projects[0].project_name is None
    assert report.projects[1].publishing


def test_profile_environment_override_is_rejected_without_disclosing_value(tmp_path: Path) -> None:
    report = DbtWorkspaceDiscovery(environment={"DPONE_DBT_PUBLISH_PROFILES": "SECRET"}).discover(tmp_path)
    assert not report.passed and "SECRET" not in str(report.to_dict())
    assert report.blockers[0].code == "DPONE_DBT_WORKSPACE_PROFILE_OVERRIDE"


@pytest.mark.parametrize("setting", ["max_entries", "max_projects", "max_depth"])
def test_limits_are_enforced_during_traversal(tmp_path: Path, setting: str) -> None:
    _project(tmp_path, "a/child", name="a")
    _project(tmp_path, "b/child", name="b")
    report = DbtWorkspaceDiscovery(environment={}, **{setting: 1}).discover(tmp_path)
    assert not report.passed
    assert any(issue.code == "DPONE_DBT_WORKSPACE_DISCOVERY_LIMIT" for issue in report.blockers)


def test_empty_workspace_is_a_successful_noop_and_root_must_exist(tmp_path: Path) -> None:
    scanner = DbtWorkspaceDiscovery(environment={})
    assert scanner.discover(tmp_path).to_dict() == {
        "schema": "dpone.dbt-workspace-discovery.v1",
        "passed": True,
        "projects": [],
        "blockers": [],
    }
    assert not scanner.discover(tmp_path / "missing").passed


def test_structural_parent_case_collision_fails_before_mirror_installation() -> None:
    with pytest.raises(ValueError, match="case collision"):
        validate_dbt_project_layout((("DBT/alpha", "alpha"), ("dbt/beta", "beta")))


def test_empty_profile_override_is_still_rejected(tmp_path: Path) -> None:
    assert not DbtWorkspaceDiscovery(environment={"DPONE_DBT_PUBLISH_PROFILES": ""}).discover(tmp_path).passed


def test_symlinked_root_and_policy_parent_are_rejected(tmp_path: Path) -> None:
    project = _project(tmp_path, "real", name="real")
    alias = tmp_path / "alias"
    alias.symlink_to(project, target_is_directory=True)
    scanner = DbtWorkspaceDiscovery(environment={})
    assert not scanner.discover(alias).passed
    (project / "dpone/dbt-publish-profiles.yml").unlink()
    (project / "dpone").rmdir()
    (project / "dpone").symlink_to(tmp_path, target_is_directory=True)
    assert not scanner.discover(project).passed


def test_exact_entry_project_and_depth_boundaries(tmp_path: Path) -> None:
    _project(tmp_path, "a", name="a", policy=None)
    # One directory and its project file: two entries, one project, depth one.
    assert DbtWorkspaceDiscovery(environment={}, max_entries=2, max_projects=1, max_depth=1).discover(tmp_path).passed
    (tmp_path / "extra").touch()
    assert not DbtWorkspaceDiscovery(environment={}, max_entries=2).discover(tmp_path).passed
