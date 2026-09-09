"""A public project projection is reusable before any release is published."""

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.adapters.dbt_workflow_selection import ManifestPreviewSelectionResolver
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.contracts.dbt_execution_pack import DbtInvocationTarget
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from dpone.readiness.dbt_sqlserver_project_policy import DbtSqlserverProjectPolicyValidator
from dpone.services.dbt_project_artifacts import DbtProjectArtifactProjector
from tests.test_dbt_publish_atomicity import DEMO, _compile, _writer


def test_singleton_policy_migration_preserves_all_unaffected_bytes(tmp_path: Path, monkeypatch) -> None:
    """Keep the historical projector baseline and pin the explicit policy migration.

    The a6669cd parent had the old Python-dispatch authority. Its historical
    57-file baseline remains unchanged: 53 files still match exactly, while
    selection policy/graph and their pack/release identities must change. These
    four fixed hashes were observed through the canonical singleton writer and
    reviewed for the new 433e19b5 policy, not computed as test expectations.
    """

    from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

    from dpone.contracts.airflow_deployment import release_id
    from dpone.contracts.dbt_selection_lock import DbtSelectionLock
    from dpone.contracts.dbt_sqlserver_graph_policy import DBT_SQLSERVER_GRAPH_POLICY_SHA256
    from dpone.services.dbt_release_assets import canonical_schema_files

    migrated = {
        "runtime/dbt/competitive_pricing.selection-lock.json": "d3ae154e3d0ff96af894ec051a532cdb2ee481ead233f7c48b3305f12ec655da",
        "packs/dbt__competitive_pricing.airflow-pack.json": "1d90bac3bdf0b9b9f782b8403e9415bba299f4e1c646bc4bcf40c5b717454903",
        "dbt__competitive_pricing/airflow-pack.json": "1d90bac3bdf0b9b9f782b8403e9415bba299f4e1c646bc4bcf40c5b717454903",
        "release-set.json": "5d3827fceeaf58bca08280f5c304af919b0a98078d77220ada48b21f903fa269",
    }
    baseline = json.loads((Path(__file__).parent / "fixtures/dbt-singleton-pre-projector-sha256.json").read_bytes())
    schemas = {path: body for path, body in canonical_schema_files().items() if path in baseline}
    monkeypatch.setattr("dpone.services.dbt_publish_artifact_writer.canonical_schema_files", lambda: schemas)
    output = tmp_path / "singleton"
    assert _writer(producer_version="0.74.28").write(_compile(), output, project_root=DEMO).passed
    observed = {
        path.relative_to(output).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in output.rglob("*")
        if path.is_file()
    }
    assert len(baseline) == 57 and set(observed) == set(baseline)
    assert len(migrated) == 4
    assert {path for path in baseline if observed[path] != baseline[path]} == set(migrated)
    assert observed == baseline | migrated
    canonical_pack = (output / "packs/dbt__competitive_pricing.airflow-pack.json").read_bytes()
    assert canonical_pack == (output / "dbt__competitive_pricing/airflow-pack.json").read_bytes()
    assert verify_pack_fingerprint(canonical_pack)
    lock = DbtSelectionLock.from_mapping(
        json.loads((output / "runtime/dbt/competitive_pricing.selection-lock.json").read_bytes())
    )
    assert lock.graph_policy_sha256 == DBT_SQLSERVER_GRAPH_POLICY_SHA256
    release = json.loads((output / "release-set.json").read_bytes())
    assert release["release_id"] == release_id(release)
    for descriptors in release["artifacts"].values():
        for descriptor in descriptors:
            body = (output / descriptor["path"]).read_bytes()
            assert descriptor["bytes"] == len(body)
            assert descriptor["sha256"] == "sha256:" + hashlib.sha256(body).hexdigest()


class FixtureSelection(ManifestPreviewSelectionResolver):
    """Structural fixture target only; not a claim of dbt profile rendering."""

    def resolve(self, **kwargs):
        return replace(super().resolve(**kwargs), invocation_target=DbtInvocationTarget("DWH_Stage", "fixture_base"))


def _projector(**kwargs):
    return DbtProjectArtifactProjector(
        selection_resolver=FixtureSelection(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(),
        **kwargs,
    )


def test_public_v1_projection_matches_singleton_writer_without_a_nested_release(tmp_path: Path) -> None:
    report = _compile()
    project = _projector().project(report, project_root=DEMO, wire_contract=DBT_RUNTIME_WIRE_V1)
    output = tmp_path / "legacy"
    assert _writer().write(report, output, project_root=DEMO).passed
    assert "release-set.json" not in project.files
    for path, content in project.files.items():
        assert (output / path).read_bytes() == content
    assert project.runtime_payload_ids["dbt__competitive_pricing"] == (
        "dbt_project",
        "dbt_manifest",
        "dbt_selection_competitive_pricing",
    )
    with pytest.raises(TypeError):
        project.files["extra"] = b"x"


def test_v2_ids_are_supplied_before_execution_pack_identity_is_computed() -> None:
    project = _projector().project(_compile(), project_root=DEMO, wire_contract=DBT_RUNTIME_WIRE_V2)
    ids = project.runtime_payload_ids["dbt__competitive_pricing"]
    assert ids[0].startswith("dbt_project_sha256_")
    assert ids[1].startswith("dbt_manifest_sha256_")
    assert ids[2].startswith("dbt_selection_sha256_")
    pack = json.loads(project.pack_files["dbt__competitive_pricing"])
    assert pack["runtime_payload_ids"] == list(ids)
    from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

    assert verify_pack_fingerprint(project.pack_files["dbt__competitive_pricing"])
    assert all(path.startswith("runtime/dbt/objects/") for path in project.runtime_files)


def test_project_capture_and_selection_happen_once() -> None:
    class CountingBundles(RuntimeDbtProjectBundleOperations):
        captures = 0

        def build(self, project_root):
            self.captures += 1
            return super().build(project_root)

    bundles = CountingBundles()

    class CountingSelection(FixtureSelection):
        calls = 0

        def resolve(self, **kwargs):
            self.calls += 1
            return super().resolve(**kwargs)

    selection = CountingSelection()
    projector = DbtProjectArtifactProjector(
        selection_resolver=selection,
        bundle_operations=bundles,
        project_policy=DbtSqlserverProjectPolicyValidator(),
    )
    projector.project(_compile(), project_root=DEMO, wire_contract=DBT_RUNTIME_WIRE_V2)
    assert bundles.captures == 1
    assert selection.calls == len(_compile().workflows)


def test_projection_freezes_nested_receipts_and_checks_originating_report() -> None:
    report = _compile()
    project = _projector().project(report, project_root=DEMO)
    original = project.route_certifications(report)
    detached = project.route_certifications(report)
    detached[0]["evidence_refs"].append("sha256:" + "f" * 64)
    assert project.route_certifications(report) == original
    report.models[0].route_capability["evidence_refs"] = ["sha256:" + "a" * 64]
    with pytest.raises(ValueError, match="certification.*conflict"):
        project.route_certifications(report)


def test_projection_binds_profile_fields_omitted_from_public_compile_report() -> None:
    report = _compile()
    project = _projector().project(report, project_root=DEMO)
    report.models[0].profile.runtime["dbt_target"] = "another_target"
    with pytest.raises(ValueError, match="certification.*conflict"):
        project.route_certifications(report)


def test_singleton_projects_route_receipts_once(tmp_path: Path, monkeypatch) -> None:
    from dpone.readiness.dbt_airflow_artifact_projection import route_certifications

    calls = []

    def collect(report):
        calls.append(report)
        return route_certifications(report)

    monkeypatch.setattr("dpone.services.dbt_project_artifacts.route_certifications", collect, raising=False)
    assert _writer().write(_compile(), tmp_path / "single", project_root=DEMO).passed
    assert len(calls) == 1


def test_workload_defaults_do_not_mutate_checked_runtime_policy() -> None:
    from copy import deepcopy

    from dpone.readiness.dbt_airflow_artifact_projection import workload_definition

    model = _compile().models[0]
    before = deepcopy(model.profile.runtime)
    first = workload_definition(model, "manifest.yaml")
    second = workload_definition(model, "manifest.yaml")
    assert model.profile.runtime == before
    assert first == second
    assert "execution" in first.effective_config["airflow"]


@pytest.mark.parametrize("semantic", [False, True])
def test_full_report_identity_streams_identical_bytes_without_expanded_allocation(semantic, monkeypatch):
    from collections.abc import Mapping
    from dataclasses import fields, is_dataclass

    from dpone.contracts.dbt_publish_models import DbtCompileReport
    from dpone.services.dbt_project_artifacts import _report_identity
    from tests.test_dbt_semantic_refresh_artifact_projection import _compiled

    report = (
        DbtCompileReport("manifest.json", 12, models=(_compiled("model.analytics.events", "events", ()),))
        if semantic
        else _compile()
    )

    def encode(value):
        if is_dataclass(value):
            return {field.name: getattr(value, field.name) for field in fields(value)}
        if isinstance(value, Mapping):
            return dict(value)
        raise TypeError(type(value))

    payload = json.dumps(report, default=encode, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
    expected = "sha256:" + hashlib.sha256(payload).hexdigest()

    def whole_report_not_allowed(*args, **kwargs):
        raise AssertionError("the full expanded report must not be materialized")

    monkeypatch.setattr("dpone.contracts.dbt_project_artifacts.json.dumps", whole_report_not_allowed)
    assert _report_identity(report) == expected


def test_workload_explicit_execution_overrides_survive_repeated_projection():
    from dpone.readiness.dbt_airflow_artifact_projection import workload_definition

    model = _compile().models[0]
    explicit = {"deferrable": False, "get_logs": False}
    model.profile.runtime["airflow"]["execution"] = explicit
    first = workload_definition(model, "manifest.yaml")
    assert first.effective_config["airflow"]["execution"] == explicit
    assert first == workload_definition(model, "manifest.yaml")
    assert model.profile.runtime["airflow"]["execution"] == explicit


@pytest.mark.parametrize("field", ["runtime_target", "receipt"])
def test_projection_rechecks_full_report_after_external_builder(field):
    from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder

    report = _compile()

    class MutatingBuilder(DbtAirflowExecutionPackBuilder):
        def build(self, **kwargs):
            pack = super().build(**kwargs)
            if field == "runtime_target":
                report.models[0].profile.runtime["dbt_target"] = "changed_inside_builder"
            else:
                report.models[0].route_capability["evidence_refs"].append("sha256:" + "f" * 64)
            return pack

    with pytest.raises(ValueError, match="certification.*conflict"):
        _projector(dbt_pack_builder=MutatingBuilder()).project(report, project_root=DEMO)


def test_snapshot_compatibility_imports_are_the_same_contract_types():
    from dpone.contracts import dbt_project_artifacts as contracts
    from dpone.services.dbt_project_artifacts import DbtProjectArtifacts
    from dpone.services.dbt_project_runtime_payloads import DbtProjectRuntimePayloads, project_runtime_payloads
    from dpone.services.dbt_release_builder import DbtReleaseInputs

    assert DbtProjectArtifacts is contracts.DbtProjectArtifacts
    assert DbtProjectRuntimePayloads is contracts.DbtProjectRuntimePayloads
    assert DbtReleaseInputs is contracts.DbtReleaseInputs
    assert project_runtime_payloads is contracts.project_runtime_payloads


def test_singleton_cannot_publish_changed_report_after_release_verification(tmp_path, monkeypatch):
    from dpone.services import dbt_publish_artifact_writer as writer_module

    report = _compile()
    build = writer_module.build_release_set

    def mutate_after_verification(**kwargs):
        release = build(**kwargs)
        report.models[0].profile.runtime["dbt_target"] = "changed_after_verification"
        return release

    monkeypatch.setattr(writer_module, "build_release_set", mutate_after_verification)
    output = tmp_path / "singleton"
    result = _writer().write(report, output, project_root=DEMO)
    assert not result.passed
    assert not output.exists()
