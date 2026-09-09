"""Workspace compilation reuses canonical parsers behind confined acquisition."""

import json
from pathlib import Path

import pytest

from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.app.dbt_workspace_composition import _project_compiler
from dpone.manifest.dbt_workspace_discovery import DbtWorkspaceDiscovery
from dpone.manifest.dbt_workspace_inputs import ConfinedDbtWorkspaceInputs
from tests.test_dbt_publish_atomicity import MANIFEST, PROFILES
from tests.test_dbt_workspace_discovery import _project


def _inputs(tmp_path: Path):
    payload = json.loads(MANIFEST.read_bytes())
    project = _project(tmp_path, "app", name=payload["metadata"]["project_name"])
    (project / "target").mkdir()
    manifest = project / "target/manifest.json"
    manifest.write_bytes(MANIFEST.read_bytes())
    policy = project / "dpone/dbt-publish-profiles.yml"
    policy.write_bytes(PROFILES.read_bytes())
    row = DbtWorkspaceDiscovery(environment={}).discover(tmp_path).projects[0]
    return ConfinedDbtWorkspaceInputs(root=tmp_path, project=row, reader=DbtArtifactReader()), manifest, policy


def test_confined_inputs_preserve_canonical_inline_intent_and_registry(tmp_path: Path) -> None:
    inputs, manifest, policy = _inputs(tmp_path)
    artifact, issues = inputs.read(manifest)
    assert artifact is not None
    assert not [issue for issue in issues if issue.severity == "error"]
    assert any(model.meta.get("dpone", {}).get("publish", {}).get("enabled") for model in artifact.models)
    registry, issues = inputs.load(manifest, policy)
    assert registry is not None and not issues


@pytest.mark.parametrize("kind", ["manifest", "policy"])
def test_confined_inputs_reject_symlinks(tmp_path: Path, kind: str) -> None:
    inputs, manifest, policy = _inputs(tmp_path)
    path = manifest if kind == "manifest" else policy
    path.unlink()
    path.symlink_to(MANIFEST if kind == "manifest" else PROFILES)
    result, issues = inputs.read(manifest) if kind == "manifest" else inputs.load(manifest, policy)
    assert result is None and issues


def test_manifest_name_must_match_discovered_project(tmp_path: Path) -> None:
    inputs, manifest, _ = _inputs(tmp_path)
    payload = json.loads(manifest.read_bytes())
    payload["metadata"]["project_name"] = "other"
    manifest.write_text(json.dumps(payload))
    result, issues = inputs.read(manifest)
    assert result is None and issues[0].code == "DPONE_DBT_MANIFEST_INVALID"


@pytest.mark.parametrize(
    "policy_bytes",
    [b"schema: []", b"schema: one\nschema: two", b"schema: &a [*a]", b"- not a mapping", b"x: " + b"x" * (1024 * 1024)],
)
def test_invalid_or_unbounded_policy_is_structured_and_redacted(tmp_path: Path, policy_bytes: bytes) -> None:
    inputs, manifest, policy = _inputs(tmp_path)
    policy.write_bytes(policy_bytes)
    registry, issues = inputs.load(manifest, policy)
    assert registry is None and issues[0].code == "DPONE_DBT_PROFILES_INVALID"


def test_profile_override_and_other_project_manifest_are_rejected(tmp_path: Path) -> None:
    inputs, manifest, policy = _inputs(tmp_path)
    assert inputs.load(manifest, PROFILES)[0] is None
    assert inputs.load(MANIFEST, policy)[0] is None
    assert inputs.read(MANIFEST)[0] is None


def test_graph_reader_rejects_manifest_rewritten_after_acquisition(tmp_path: Path) -> None:
    inputs, manifest, _ = _inputs(tmp_path)
    assert inputs.read(manifest)[0] is not None
    assert inputs.graph_manifest(manifest)["metadata"] == json.loads(MANIFEST.read_bytes())["metadata"]
    manifest.write_text('{"nodes": {}, "parent_map": {}, "child_map": {}}')
    with pytest.raises(ValueError, match="changed"):
        inputs.graph_manifest(manifest)


def test_graph_reader_requires_a_successful_manifest_acquisition(tmp_path: Path) -> None:
    inputs, manifest, _ = _inputs(tmp_path)
    with pytest.raises(ValueError):
        inputs.graph_manifest(manifest)


def test_real_workspace_composition_binds_graph_validation_to_the_acquired_manifest(tmp_path: Path) -> None:
    from tests.test_dbt_publish_atomicity import DEMO

    _, manifest, policy = _inputs(tmp_path)
    (manifest.parent.parent / "dbt_project.yml").write_bytes((DEMO / "dbt_project.yml").read_bytes())
    project = DbtWorkspaceDiscovery(environment={}).discover(tmp_path).projects[0]
    compiler = _project_compiler(tmp_path, project)
    original_load = compiler._profile_loader.load

    def rewrite_between_model_and_graph_validation(*args, **kwargs):
        result = original_load(*args, **kwargs)
        payload = json.loads(manifest.read_bytes())
        payload["metadata"]["invocation_id"] = "00000000-0000-0000-0000-000000000001"
        manifest.write_text(json.dumps(payload))
        return result

    compiler._profile_loader.load = rewrite_between_model_and_graph_validation
    report = compiler.build(manifest, profiles_path=policy)
    assert not report.passed
    assert any(issue.code == "DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED" for issue in report.blockers)
