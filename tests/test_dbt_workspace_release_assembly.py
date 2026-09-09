"""Offline aggregate assembly using real bundles/packs and synthetic authority.

Selection and route receipts are fixtures, not dbt CLI or live certification.
"""

import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.adapters.dbt_workflow_selection import ManifestPreviewSelectionResolver
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.contracts.dbt_execution_pack import DbtInvocationTarget
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_workspace import (
    DbtWorkspaceCheckReport,
    DbtWorkspaceDiscoveryReport,
    DbtWorkspaceProject,
    DbtWorkspaceProjectCheck,
)
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.dbt_sqlserver_project_policy import DbtSqlserverProjectPolicyValidator
from dpone.services.dbt_project_artifacts import DbtProjectArtifactProjector
from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
from dpone.services.dbt_workspace_release_assembly import DbtWorkspaceProjectArtifacts, assemble_workspace_release
from tests.test_dbt_airflow_release_e2e import _certified
from tests.test_dbt_publish_atomicity import DEMO


class FixtureSelection:
    """Inject synthetic selection authority for structural tests only."""

    def __init__(self, *, authority="dbt_cli"):
        self.authority = authority

    def resolve(self, **kwargs):
        return replace(
            ManifestPreviewSelectionResolver().resolve(**kwargs),
            authority=self.authority,
            invocation_target=DbtInvocationTarget("DWH_Stage", "fixture_base"),
        )


def _project(
    tmp_path: Path,
    name: str,
    *,
    preview: bool = False,
    model_namespace: str | None = None,
    transfer_namespace: str | None = None,
    model_alias_suffix: str = "",
):
    model_namespace = model_namespace or name
    transfer_namespace = transfer_namespace or name
    root = tmp_path / name
    shutil.copytree(DEMO, root)
    config = root / "dbt_project.yml"
    config.write_text(
        config.read_text()
        .replace("dpone_dbt_demo", name)
        .replace("workflow: competitive_pricing", f"workflow: {name}")
        .replace("+schema: pricing", f"+schema: pricing_{model_namespace}")
    )
    policy = root / "dpone/dbt-publish-profiles.yml"
    policy.write_text(
        policy.read_text()
        .replace("  competitive_pricing:", f"  {name}:")
        .replace("target_schema: DWH_Stage", f"target_schema: DWH_Stage_{transfer_namespace}")
    )
    path = root / "fixtures/manifest.v12.json"
    manifest = json.loads(path.read_bytes())
    manifest["metadata"]["project_name"] = name
    for node in manifest["nodes"].values():
        if node.get("resource_type") == "model":
            node["schema"] = f"{node['schema']}_{model_namespace}"
            node["alias"] += model_alias_suffix
            for meta in (node.get("meta", {}), node.get("config", {}).get("meta", {})):
                target = meta.get("dpone", {}).get("publish", {}).get("target")
                if target is not None:
                    target["schema"] = f"DWH_Stage_{transfer_namespace}"
    path.write_text(json.dumps(manifest).replace('"workflow": "competitive_pricing"', f'"workflow": "{name}"'))
    report = _certified(build_dbt_dpone_compiler(root=root).build(path, profiles_path=policy))
    assert report.passed, report.blockers
    check = DbtWorkspaceProjectCheck(
        DbtWorkspaceProject(
            name, name, True, "dpone/dbt-publish-profiles.yml", "fixtures/manifest.v12.json", "policy_present"
        ),
        report,
    )
    projector = DbtProjectArtifactProjector(
        selection_resolver=FixtureSelection(authority="manifest_preview" if preview else "dbt_cli"),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(),
    )
    return DbtWorkspaceProjectArtifacts(
        check, projector.project(report, project_root=root, wire_contract=DBT_RUNTIME_WIRE_V2)
    )


def _assemble(projects):
    check = DbtWorkspaceCheckReport(
        DbtWorkspaceDiscoveryReport(tuple(p.check.project for p in projects)),
        tuple(p.check for p in projects),
    )
    return assemble_workspace_release(check, projects, producer_version="0.74.28")


@pytest.mark.parametrize("kind", ["model", "transfer"])
def test_different_workflow_ids_cannot_hide_same_relation_writer(tmp_path, kind):
    from dpone.contracts.dbt_contract_validation import DbtPublishingError

    projects = [_project(tmp_path, name, **{f"{kind}_namespace": "shared"}) for name in ("alpha", "beta")]
    with pytest.raises(DbtPublishingError) as raised:
        _assemble(projects)
    assert raised.value.code == "DPONE_DBT_WORKSPACE_TARGET_COLLISION"
    assert "alpha" in str(raised.value) and "beta" in str(raised.value)


@pytest.mark.parametrize("suffix", ["__dbt_tmp_vw", "__dbt_tmp__dbt_tmp_vw"])
def test_workspace_producer_cannot_publish_models_over_other_project_helper_views(tmp_path, suffix):
    from dpone.contracts.dbt_contract_validation import DbtPublishingError

    projects = [
        _project(tmp_path, "alpha", model_namespace="shared"),
        _project(tmp_path, "beta", model_namespace="shared", model_alias_suffix=suffix),
    ]
    with pytest.raises(DbtPublishingError) as failure:
        _assemble(projects)
    assert failure.value.code == "DPONE_DBT_WORKSPACE_TARGET_COLLISION"
    assert "helper" in str(failure.value) and "alpha" in str(failure.value) and "beta" in str(failure.value)


def test_omitted_unchanged_project_is_rejected(tmp_path: Path):
    first, second = _project(tmp_path, "alpha"), _project(tmp_path, "beta")
    check = DbtWorkspaceCheckReport(
        DbtWorkspaceDiscoveryReport((first.check.project, second.check.project)),
        (first.check, second.check),
    )
    with pytest.raises(ValueError, match="complete"):
        assemble_workspace_release(check, [first], producer_version="0.74.28")


def test_two_projects_are_one_canonical_release_with_complete_reader_acceptance(tmp_path: Path):
    projects = [_project(tmp_path, "alpha"), _project(tmp_path, "beta")]
    assert projects[0].check.report.models[0].model.unique_id == projects[1].check.report.models[0].model.unique_id
    tree = _assemble(projects)
    assert tree == _assemble(list(reversed(projects)))
    release = json.loads(tree.files["release-set.json"])
    assert release["producer"]["wire_contract"] == DBT_RUNTIME_WIRE_V2
    assert len(release["artifacts"]["runtime_payloads"]) == 6
    # Both projects use the same two strategy variants, not four receipts.
    assert len(release["provenance"]["route_certifications"]) == 2
    assert [p.project_name for p in tree.inventory.projects] == ["alpha", "beta"]
    expected_paths = {"release-set.json", "_dbt/dbt-source-snapshot.json"}
    expected_paths.update(d["path"] for section in release["artifacts"].values() for d in section)
    assert set(tree.files) == expected_paths
    root = tmp_path / "compiled"
    for path, body in tree.files.items():
        file = root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(body)
    sources = DbtReleaseSourceReader(
        bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=read_confined_file
    ).read(root, expected_release_id=tree.release_id)
    assert {w.source.workflow_id for w in sources.workflows} == {"alpha", "beta"}


def test_empty_or_repeated_project_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError):
        _assemble([])
    project = _project(tmp_path, "alpha")
    with pytest.raises(ValueError, match="unique|collision"):
        _assemble([project, project])


def test_preview_authority_cannot_be_promoted_by_aggregation(tmp_path: Path):
    with pytest.raises(ValueError, match="authoritative"):
        _assemble([_project(tmp_path, "alpha", preview=True)])


def test_changed_report_after_projection_is_rejected(tmp_path: Path):
    first, second = _project(tmp_path, "alpha"), _project(tmp_path, "beta")
    models = tuple(
        replace(model, route_capability={**model.route_capability, "snapshot_id": "sha256:" + "f" * 64})
        for model in second.check.report.models
    )
    second = replace(second, check=replace(second.check, report=replace(second.check.report, models=models)))
    with pytest.raises(ValueError, match="certification.*conflict"):
        _assemble([first, second])


def test_route_variant_conflict_is_not_last_writer_wins(tmp_path: Path):
    first, second = _project(tmp_path, "alpha"), _project(tmp_path, "beta")
    report = replace(
        second.check.report,
        models=tuple(
            replace(model, route_capability={**model.route_capability, "snapshot_id": "sha256:" + "f" * 64})
            for model in second.check.report.models
        ),
    )
    projector = DbtProjectArtifactProjector(
        selection_resolver=FixtureSelection(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(),
    )
    second = DbtWorkspaceProjectArtifacts(
        replace(second.check, report=report),
        projector.project(report, project_root=tmp_path / "beta", wire_contract=DBT_RUNTIME_WIRE_V2),
    )
    with pytest.raises(ValueError, match="certification.*conflict"):
        _assemble([first, second])


def test_assembly_uses_project_receipts_without_recomputing(tmp_path: Path, monkeypatch):
    projects = [_project(tmp_path, "alpha"), _project(tmp_path, "beta")]
    expected = _assemble(projects)

    def unexpected(*args, **kwargs):
        raise AssertionError("route receipt projection must not run twice")

    monkeypatch.setattr("dpone.services.dbt_project_artifacts.route_certifications", unexpected)
    assert _assemble(projects) == expected


def test_unreferenced_project_runtime_bytes_are_not_silently_ignored(tmp_path: Path):
    project = _project(tmp_path, "alpha")
    payloads = replace(
        project.artifacts.payloads, files={**project.artifacts.payloads.files, "runtime/dbt/orphan.sql": b"select 1"}
    )
    project = replace(project, artifacts=replace(project.artifacts, payloads=payloads))
    with pytest.raises(ValueError, match="runtime.*(membership|inventory)"):
        _assemble([project])


def test_missing_selection_descriptor_is_a_validation_error(tmp_path: Path):
    project = _project(tmp_path, "alpha")
    payloads = replace(project.artifacts.payloads, descriptors=project.artifacts.payloads.descriptors[:2])
    project = replace(project, artifacts=replace(project.artifacts, payloads=payloads))
    with pytest.raises(ValueError, match="runtime inventory"):
        _assemble([project])


def test_runtime_limits_apply_to_the_combined_release(tmp_path: Path, monkeypatch):
    import dpone.contracts.dbt_workspace_release as assembly

    projects = [_project(tmp_path, name) for name in ("alpha", "beta")]
    total = sum(len(body) for project in projects for body in project.artifacts.runtime_files.values())
    monkeypatch.setattr(assembly, "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES", total)
    monkeypatch.setattr(assembly, "MAX_DBT_RUNTIME_PAYLOADS", 6)
    assert _assemble(projects).release_id
    monkeypatch.setattr(assembly, "MAX_DBT_RUNTIME_PAYLOADS", 5)
    with pytest.raises(ValueError, match="aggregate bound"):
        _assemble(projects)
    monkeypatch.setattr(assembly, "MAX_DBT_RUNTIME_PAYLOADS", 6)
    monkeypatch.setattr(assembly, "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES", total - 1)
    with pytest.raises(ValueError, match="aggregate bound"):
        _assemble(projects)


def test_duplicate_global_pack_identity_is_rejected_before_merge(tmp_path: Path):
    first, second = _project(tmp_path, "alpha"), _project(tmp_path, "beta")
    rogue = {**second.artifacts.pack_files, next(iter(first.artifacts.pack_files)): b"duplicate"}
    second = replace(second, artifacts=replace(second.artifacts, pack_files=rogue))
    with pytest.raises(ValueError, match="membership|collision"):
        _assemble([first, second])


def test_identical_runtime_objects_are_charged_only_once(tmp_path: Path, monkeypatch):
    """Object resource accounting is independent of global project ownership."""

    import dpone.contracts.dbt_workspace_release as assembly

    project = _project(tmp_path, "alpha")
    total = sum(len(body) for body in project.artifacts.runtime_files.values())
    monkeypatch.setattr(assembly, "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES", total)
    monkeypatch.setattr(assembly, "MAX_DBT_RUNTIME_PAYLOADS", 3)
    inventory = assembly._RuntimeInventory()
    inventory.add(project.artifacts)
    inventory.add(project.artifacts)
    assert inventory.files == project.artifacts.runtime_files
    assert len(inventory.descriptors) == 3 and inventory.total_bytes == total


def test_project_b_source_change_does_not_replace_project_a_objects(tmp_path: Path):
    first, second = _project(tmp_path, "alpha"), _project(tmp_path, "beta")
    original = _assemble([first, second])
    root = tmp_path / "beta"
    model = root / "models/competitive_pricing.sql"
    model.write_text(model.read_text() + "\n-- source-only comment in project beta\n")
    # Reacquire through the real bundle/projector; selection remains a fixture.
    # This proves object isolation, not canonical dbt parsing or SQL execution.
    projector = DbtProjectArtifactProjector(
        selection_resolver=FixtureSelection(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(),
    )
    second = replace(
        second, artifacts=projector.project(second.check.report, project_root=root, wire_contract=DBT_RUNTIME_WIRE_V2)
    )
    changed = _assemble([first, second])
    assert changed.release_id != original.release_id
    for path, body in first.artifacts.runtime_files.items():
        assert changed.files[path] == original.files[path] == body
    assert changed.inventory.projects[1].project_bundle_sha256 != original.inventory.projects[1].project_bundle_sha256
