"""Producer-backed parent inputs; offline evidence does not certify SQL execution."""

import json
import shutil
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.app.composition_activation_inputs import DeploymentCacheCompositionActivationInputs
from dpone.app.release_composition import build_composition_source_reader, build_release_composition_service
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
from tests.dbt_compact_wire_v2_helpers import IMAGE, SIDECAR
from tests.test_dbt_airflow_release_e2e import _config_map_ref, _write_environment
from tests.test_release_composition_delivery import composition_request as composition_request


class ResolverFactory:
    def __init__(self):
        self.calls = []

    def build(self, **values):
        self.calls.append(values)
        return SimpleNamespace(resolve=lambda ref: SimpleNamespace(connection_ref=ref))


@pytest.fixture
def installed(composition_request):
    request = composition_request
    report = build_release_composition_service().compose(request)
    assert report.passed, report.blockers
    base = request.output_dir.parent
    cache = base / ".dpone-cache"
    result = materialize_compact_pack_release(
        pack_root=request.output_dir, cache_root=cache, xcom_sidecar_image=SIDECAR
    )
    assert result.passed, result.blockers
    _write_environment(base)
    projection = AirflowDeploymentProjectionService(root=base).materialize(
        release_id=result.release_id,
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=IMAGE,
        runtime_image_digest=IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )
    root = cache / "activations" / "prod" / projection.deployment_dir.name
    shutil.copytree(projection.deployment_dir, root)
    factory = ResolverFactory()
    inputs = DeploymentCacheCompositionActivationInputs(
        cache_root=cache,
        source_reader=build_composition_source_reader(),
        resolver_factory=factory,
    )
    coordinates = dict(
        projection_root=root,
        activation_id=str(uuid4()),
        environment="prod",
        release_id=result.release_id,
        deployment_id=projection.deployment["deployment_id"],
        previous_deployment_id=None,
    )
    return inputs, coordinates, factory, cache


def test_complete_verified_sources_and_context_replay(installed):
    inputs, coords, factory, _ = installed
    sources = inputs.load_sources(projection_root=coords["projection_root"], release_id=coords["release_id"])
    assert set(dict(sources.workload_pins)) == {key for key, _ in sources.native.required_workloads} | {"orders"}
    assert set(dict(sources.transfer_manifests)) == {
        write.resource_id for write in sources.relation_writes if write.kind == "transfer"
    }
    assert "orders" in dict(sources.transfer_manifests)
    first = inputs.load_context(**coords)
    assert inputs.load_context(**coords) == first
    assert inputs.resolve_connection(first, "warehouse").connection_ref == "warehouse"
    assert len(factory.calls) == 2
    with pytest.raises(CompositionAdmissionError, match="runtime_context_not_loaded"):
        inputs.resolve_connection(replace(first, activation_id=str(uuid4())), "warehouse")


@pytest.mark.parametrize(
    "name",
    ["deployment.json", "airflow-index.json", "binding-set.json", "connection-registry.ref", "credential-runtime.ref"],
)
def test_changed_context_rejects_resolution_and_reload(installed, name):
    inputs, coords, factory, _ = installed
    context = inputs.load_context(**coords)
    path = coords["projection_root"] / name
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(CompositionAdmissionError, match="runtime_context"):
        inputs.resolve_connection(context, "warehouse")
    with pytest.raises(CompositionAdmissionError, match="runtime_context"):
        inputs.load_context(**coords)
    assert len(factory.calls) == 1


@pytest.mark.parametrize(
    "field,value",
    [("environment", "other"), ("release_id", "sha256:" + "0" * 64), ("deployment_id", "sha256:" + "0" * 64)],
)
def test_foreign_coordinates_fail_before_resolver(installed, field, value):
    inputs, coords, factory, _ = installed
    with pytest.raises(CompositionAdmissionError):
        inputs.load_context(**{**coords, field: value})
    assert not factory.calls


@pytest.mark.parametrize("mutation", ["missing_ordinary", "missing_native", "native_only", "changed_pack"])
def test_source_closure_rejects_missing_or_changed_constituent(installed, mutation):
    inputs, coords, _, cache = installed
    root = cache / "releases" / coords["release_id"].replace(":", "-")
    release = json.loads((root / "release-set.json").read_bytes())
    if mutation == "missing_ordinary":
        shutil.rmtree(root / "_composition" / "standalone")
    elif mutation == "missing_native":
        (root / "_composition" / "native" / "release-set.json").unlink()
    elif mutation == "native_only":
        native = next(row["release"] for row in release["constituents"] if row["id"] == "native")
        (root / "release-set.json").write_text(json.dumps(native))
    else:
        row = next(row for row in release["artifacts"]["workload_packs"] if row["id"] == "orders")
        (root / row["path"]).write_text("{}")
    with pytest.raises(CompositionAdmissionError, match="source_inventory"):
        inputs.load_sources(projection_root=coords["projection_root"], release_id=coords["release_id"])


def test_projection_symlink_and_escape_rejected(installed, tmp_path):
    inputs, coords, _, _ = installed
    linked = coords["projection_root"].with_name("linked")
    linked.symlink_to(coords["projection_root"], target_is_directory=True)
    for path in (linked, tmp_path, coords["projection_root"] / ".." / coords["projection_root"].name):
        with pytest.raises(CompositionAdmissionError):
            inputs.load_sources(projection_root=path, release_id=coords["release_id"])


@pytest.mark.parametrize("mutation", ["missing_workload", "changed_pack", "runtime_mirror"])
def test_index_membership_and_runtime_mirrors_are_verified(installed, mutation):
    inputs, coords, factory, _ = installed
    path = coords["projection_root"] / "airflow-index.json"
    index = json.loads(path.read_bytes())
    if mutation == "missing_workload":
        index["workload_packs"] = index["workload_packs"][:-1]
    elif mutation == "changed_pack":
        index["workload_packs"][0]["sha256"] = "sha256:" + "0" * 64
    else:
        index["binding_set"]["sha256"] = "sha256:" + "0" * 64
    path.write_text(json.dumps(index))
    with pytest.raises(CompositionAdmissionError, match="runtime_context"):
        inputs.load_context(**coords)
    assert not factory.calls


def test_native_only_release_cannot_enter_parent_input_reader(composition_request):
    inputs = DeploymentCacheCompositionActivationInputs(
        cache_root=composition_request.native_root.parent.parent,
        source_reader=build_composition_source_reader(),
        resolver_factory=ResolverFactory(),
    )
    projection = composition_request.native_root.parent.parent / "activations" / "prod" / "candidate"
    projection.mkdir(parents=True)
    with pytest.raises(CompositionAdmissionError, match="source_inventory"):
        inputs.load_sources(projection_root=projection, release_id=composition_request.expected_release_id)
