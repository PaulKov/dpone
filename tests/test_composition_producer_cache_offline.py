"""Offline producer-to-cache contract: NOT live route certification.

All SQL/backend observations, dbt CLI responses, and protected-store responses
are test doubles. No real credentials, database sessions, or receipts are used.
The native artifact writer still needs a typed certified capability-shaped
input; its synthetic evidence digest is confined to this pytest temporary tree.
Never publish these fixture artifacts as certification or production authority.

All generated source and deployment artifacts remain confined to tmp_path.

The test uses the real logical-outlet producer and closure verifier. It does not
patch verification or strip source fields before planning and cache activation.
"""

import json
import shutil
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from dpone_airflow_pack.deployment_index_contract import load_airflow_deployment_index

from dpone.app.release_composition import (
    build_composition_source_reader,
    build_release_composition_service,
)
from dpone.contracts.airflow_deployment import canonical_fingerprint, deployment_id
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionOccurrenceContext,
)
from dpone.contracts.composition_physical import CompositionDomainObservation, CompositionPhysicalDomain
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject
from dpone.contracts.release_composition import ReleaseCompositionRequest
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.manifest.composition_execution_plan import plan_composition_execution
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from dpone.readiness.airflow_deployment_artifacts import bytes_descriptor, digest_dir, json_bytes
from dpone.readiness.airflow_deployment_projection import (
    AirflowDeploymentProjection,
    AirflowDeploymentProjectionService,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_materializer import DeploymentCacheMaterializer
from dpone.services.composition_activation_coordinator import CompositionActivationCoordinator
from dpone.services.composition_activation_preparation import CompositionActivationPreparation
from dpone.services.composition_physical_admission import CompositionPhysicalAdmissionService
from tests import dbt_compact_wire_v2_helpers as native_helpers
from tests.test_dbt_airflow_release_e2e import _config_map_ref, _vault_connection, _write_environment
from tests.test_release_composition_ordinary import ordinary_root

MAX_SOURCE_BYTES = 1_000_000
ACTIVATION_ID = "10000000-0000-4000-8000-000000000001"
OFFLINE_EVIDENCE = canonical_fingerprint({"test_double": "NOT_LIVE_CERTIFICATION"})
CELLS = frozenset({"sqlserver_dbt_v1", "mssql_clickhouse_full_refresh_v1", "postgres_mssql_full_refresh_v1"})
SUPERVISOR = {
    "schema": "dpone.composition-supervisor.v1",
    "persistent_volume_claim": "dpone-composition-supervisor",
    "child_uid_start": 1_000_000_000,
    "child_gid_start": 1_000_000_000,
    "child_identity_count": 1_000_000,
}


def _full_refresh_snapshot():
    """Adapt an existing typed unit fixture; this is not a certification receipt."""
    snapshot = _ORIGINAL_SNAPSHOT()
    route = snapshot.routes[0]
    route_id = "mssql:clickhouse:full_refresh"
    variant = replace(
        route.certification.variants[0],
        route_id=route_id,
        id=f"{route_id}|native_bcp_to_clickhouse|widening|kpo",
        evidence_refs=(OFFLINE_EVIDENCE,),
    )
    return replace(
        snapshot,
        routes=(
            replace(
                route,
                id=route_id,
                strategy="full_refresh",
                certification=replace(route.certification, variants=(variant,)),
            ),
        ),
    )


_ORIGINAL_SNAPSHOT = native_helpers.route_snapshot


def _native_release(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    native_helpers.prepare_projects(root)
    for name in native_helpers.PROJECTS:
        project = root / name
        policy_path = project / "dpone/dbt-publish-profiles.yml"
        policy = yaml.safe_load(policy_path.read_bytes())
        profile = next(iter(policy["profiles"].values()))
        profile.pop("state")
        profile["strategy_policy"] = {
            "allowed_strategies": ["full_refresh"],
            "full_refresh": {"authorized": True, "max_source_bytes": MAX_SOURCE_BYTES},
        }
        policy_path.write_text(yaml.safe_dump(policy, sort_keys=False))
        manifest_path = project / "target/manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        for node in manifest["nodes"].values():
            if node.get("resource_type") == "model":
                for meta in (node["meta"], node["config"]["meta"]):
                    meta["dpone"]["publish"]["strategy"] = {"mode": "full_refresh"}
        body = json.dumps(manifest)
        manifest_path.write_text(body)
        (project / "fixtures/manifest.v12.json").write_text(body)
    # Replace only the synthetic discovery input, never production policy or
    # source/transport verification. Real compiler and artifact writer run.
    monkeypatch.setattr(native_helpers, "route_snapshot", _full_refresh_snapshot)
    compiled = tmp_path / "compiled"
    report = native_helpers.workspace_service(tmp_path / "profiles").compile(root, output_dir=compiled)
    assert report.passed, [(row.project.project_name, row.report.blockers) for row in report.check.projects]
    native = materialize_compact_pack_release(
        pack_root=compiled, cache_root=tmp_path / "native-cache", xcom_sidecar_image=native_helpers.SIDECAR
    )
    assert native.passed, native.blockers
    return native


def _ordinary_release(tmp_path):
    root = ordinary_root(tmp_path)
    author = tmp_path / "author"
    manifest = {
        "name": "orders",
        "source": {
            "type": "postgres",
            "connection_ref": "ordinary_reader",
            "table": {"schema": "public", "name": "orders"},
        },
        "sink": {
            "type": "mssql",
            "connection_ref": "ordinary_writer",
            "table": {"database": "DWH_Stage", "schema": "ordinary", "name": "orders"},
            "strategy": {"mode": "full_refresh"},
        },
        "state": {
            "type": "mssql",
            "connection_ref": "ordinary_writer",
            "table": {"schema": "dpone_state", "name": "unused_xmin_state"},
            "atomicity": "target_atomic",
            "provisioning": "external",
        },
    }
    (author / "transfer.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False))
    pack_path = root / "orders/airflow-pack.json"
    previous = json.loads(pack_path.read_bytes())["workload"]
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest="transfer.yaml",
        domain=previous["domain"],
        catalog_path=previous["catalog_path"],
        effective_config=previous["effective_config"],
        provenance={},
    )
    # Uses the existing real producer's environment-neutral mode. The ordinary
    # closure rebuilder selects the same mode for logical MSSQL outlets while
    # preserving the complete producer comparison and original fingerprints.
    pack = AirflowCompactPackBuilder().build(
        workload=workload, output_path="orders/airflow-pack.json", repo_root=author, outlet_binding="logical"
    )
    assert not pack.blockers, pack.blockers
    pack_path.write_text(pack.to_json())
    return root


@pytest.fixture
def producer_composition(tmp_path, monkeypatch):
    native = _native_release(tmp_path, monkeypatch)
    ordinary = _ordinary_release(tmp_path)
    service = build_release_composition_service()
    inventory = service.inventory(ordinary, xcom_sidecar_image=native_helpers.SIDECAR)
    request = ReleaseCompositionRequest(
        native_root=Path(native.release_dir),
        expected_release_id=native.release_id,
        standalone_root=ordinary,
        expected_inventory_sha256=inventory["inventory_sha256"],
        output_dir=tmp_path / "composed",
        xcom_sidecar_image=native_helpers.SIDECAR,
    )
    report = service.compose(request)
    assert report.passed, report.blockers
    installed = materialize_compact_pack_release(
        pack_root=request.output_dir, cache_root=tmp_path / ".dpone-cache", xcom_sidecar_image=native_helpers.SIDECAR
    )
    assert installed.passed, installed.blockers
    return Path(installed.release_dir), installed.release_id


def test_supervised_composition_without_mssql_outlets_uses_v3_wire(tmp_path, monkeypatch):
    native = _native_release(tmp_path, monkeypatch)
    ordinary = ordinary_root(tmp_path)
    service = build_release_composition_service()
    inventory = service.inventory(ordinary, xcom_sidecar_image=native_helpers.SIDECAR)
    request = ReleaseCompositionRequest(
        native_root=Path(native.release_dir),
        expected_release_id=native.release_id,
        standalone_root=ordinary,
        expected_inventory_sha256=inventory["inventory_sha256"],
        output_dir=tmp_path / "composed",
        xcom_sidecar_image=native_helpers.SIDECAR,
    )
    report = service.compose(request)
    assert report.passed, report.blockers
    installed = materialize_compact_pack_release(
        pack_root=request.output_dir,
        cache_root=tmp_path / ".dpone-cache",
        xcom_sidecar_image=native_helpers.SIDECAR,
    )
    assert installed.passed, installed.blockers
    _write_environment(tmp_path)

    projection = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=installed.release_id,
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=native_helpers.IMAGE,
        runtime_image_digest=native_helpers.IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
        composition_supervisor=SUPERVISOR,
    )

    assert projection.deployment["schema"] == "dpone.deployment-set.v3"
    assert projection.airflow_index["schema"] == "dpone.airflow-deployment-index.v3"
    assert "mssql_asset_outlet_projection" not in projection.deployment
    assert "mssql_asset_outlet_projection" not in projection.airflow_index
    loaded_index = load_airflow_deployment_index(
        projection.deployment_dir / "airflow-index.json",
        cache_root=tmp_path / ".dpone-cache",
    )
    assert loaded_index.composition_supervisor == SUPERVISOR
    jsonschema = pytest.importorskip("jsonschema")
    for filename, schema_filename in (
        ("deployment.json", "deployment-set-v3.schema.json"),
        ("airflow-index.json", "airflow-deployment-index-v3.schema.json"),
    ):
        payload = json.loads((projection.deployment_dir / filename).read_bytes())
        schema = json.loads((Path("docs/schemas/gitops") / schema_filename).read_bytes())
        jsonschema.Draft202012Validator(schema).validate(payload)


class OfflineBackend:
    """Fake enrolled MSSQL and ClickHouse; NOT a SQL capability or live proof."""

    execution_cells = CELLS

    def __init__(self, events):
        self.events, self.observations = events, []
        self.domains = {
            connector: CompositionPhysicalDomain(
                connector,
                f"10000000-0000-4000-8000-00000000000{index}",
                canonical_fingerprint({"offline_domain": connector}),
            )
            for index, connector in enumerate(("mssql", "clickhouse"), start=2)
        }

    def require_execution(self, plan, context):
        plan.require_installed_cells(self.execution_cells)
        assert context.release_id == plan.sources.release_id

    def resolve_domain(self, write, context):
        # Different MSSQL connection aliases resolve to the same fake domain.
        return self.domains[write.connector]

    def observe_domain(self, domain, writes, context):
        self.events.append("observe:" + domain.connector)
        self.observations.append((domain.connector, writes))
        return CompositionDomainObservation(
            domain,
            tuple((dbt_relation_write_subject(write), index) for index, write in enumerate(writes)),
            canonical_fingerprint({"offline_catalog": domain.connector}),
        )


class OfflineInputs:
    """Use the real source reader; context comes from real sealed cache bytes."""

    def __init__(self, cache):
        self.cache = cache
        self.reader = build_composition_source_reader()

    def load_sources(self, *, projection_root, release_id):
        return self.reader.read_sources(
            self.cache / "releases" / release_id.replace(":", "-"), expected_release_id=release_id
        )

    def load_context(self, *, projection_root, **coordinates):
        index = json.loads((projection_root / "airflow-index.json").read_bytes())
        assert index["release_id"] == coordinates["release_id"]
        assert index["deployment_id"] == coordinates["deployment_id"]
        return CompositionOccurrenceContext(**coordinates, runtime_context_sha256=canonical_fingerprint(index))


class OfflineProtectedStore:
    """In-memory protocol double only; cannot prove SQL protection or quiescence."""

    def __init__(self, cache, events):
        self.cache, self.events, self.current = cache, events, None

    def build(self, *, projection_root, context):
        assert projection_root.parent.parent == self.cache / "activations"
        return self

    def read(self, activation_id):
        assert activation_id == ACTIVATION_ID
        return self.current

    def prepare(self, request):
        assert not (self.cache / "current").exists()
        self.events.append("prepare")
        self.current = self._receipt(request, "PREPARED")
        return self.current

    def activate(self, request):
        assert (self.cache / "current").is_symlink()
        assert self.current.request == request
        self.events.append("activate")
        self.current = self._receipt(request, "ACTIVE")
        return self.current

    def _receipt(self, request, state):
        return CompositionActivationOccurrence(
            request,
            CompositionActivationReceipt(
                request.request_sha256, state, tuple((row.guard_id, 1) for row in request.resources)
            ),
        )

    def begin_retirement(self, request):
        raise AssertionError("retirement is outside this offline test")

    def finalize_retirement(self, request):
        raise AssertionError("retirement is outside this offline test")


def _supervised_projection(producer_composition, tmp_path) -> AirflowDeploymentProjection:
    _, release_id = producer_composition
    _write_environment(tmp_path)
    environment = tmp_path / "environments/prod/binding-set.yaml"
    bindings = yaml.safe_load(environment.read_bytes())
    bindings["bindings"].update({name: {"connection_ref": name} for name in ("ordinary_reader", "ordinary_writer")})
    environment.write_text(yaml.safe_dump(bindings, sort_keys=False))
    registry_path = tmp_path / "platform/connection-registries/prod.yaml"
    registry = yaml.safe_load(registry_path.read_bytes())
    registry["connections"].update(
        {"ordinary_reader": _vault_connection("postgres", 5432), "ordinary_writer": _vault_connection("mssql", 1433)}
    )
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    return AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release_id,
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=native_helpers.IMAGE,
        runtime_image_digest=native_helpers.IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
        composition_supervisor=SUPERVISOR,
    )


def _without_supervisor(projection: AirflowDeploymentProjection) -> Path:
    deployment = json.loads(json.dumps(projection.deployment))
    deployment.pop("composition_supervisor")
    deployment["deployment_id"] = ""
    forged_deployment_id = deployment_id(deployment)
    deployment["deployment_id"] = forged_deployment_id
    deployment_bytes = json_bytes(deployment)
    index = json.loads(json.dumps(projection.airflow_index))
    index.pop("composition_supervisor")
    index["deployment_id"] = forged_deployment_id
    index["deployment"] = bytes_descriptor(
        artifact_ref=(
            f"cache://deployments/{deployment['environment']}/{digest_dir(forged_deployment_id)}/deployment.json"
        ),
        payload=deployment_bytes,
    )
    target = projection.deployment_dir.parent / digest_dir(forged_deployment_id)
    shutil.copytree(projection.deployment_dir, target)
    (target / "deployment.json").write_bytes(deployment_bytes)
    (target / "airflow-index.json").write_bytes(json_bytes(index))
    return target


def test_local_promotion_rejects_composition_without_supervisor_before_activation(
    producer_composition,
    tmp_path,
) -> None:
    projection = _supervised_projection(producer_composition, tmp_path)
    forged = _without_supervisor(projection)
    events: list[str] = []
    backend = OfflineBackend(events)
    store = OfflineProtectedStore(tmp_path / ".dpone-cache", events)
    coordinator = CompositionActivationCoordinator(
        inputs=OfflineInputs(tmp_path / ".dpone-cache"),
        preparation=CompositionActivationPreparation(physical=CompositionPhysicalAdmissionService(backend=backend)),
        stores=store,
    )

    with pytest.raises(DeploymentCacheError) as exc_info:
        DeploymentCacheMaterializer(
            tmp_path / ".dpone-cache",
            composition_activation_coordinator=coordinator,
        ).promote(
            forged,
            environment="prod",
            expect_current_absent=True,
            activation_id=ACTIVATION_ID,
        )

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_REQUIRED"
    assert store.current is None
    assert events == []
    assert not (tmp_path / ".dpone-cache" / "current").exists()


def test_real_producer_to_v3_cache_with_explicit_offline_admission(producer_composition, tmp_path):
    root, release_id = producer_composition
    sources = build_composition_source_reader().read_sources(root, expected_release_id=release_id)
    plan = plan_composition_execution(sources)
    assert {row.execution_cell for row in plan.workloads} == CELLS
    assert {row.workload_id for row in plan.workloads} == {key for key, _ in sources.workload_pins}
    assert {row.constituent_id for row in plan.workloads} == {"native", "standalone"}
    for workload, body in sources.transfer_manifests:
        manifest = yaml.safe_load(body)
        if workload != "orders":
            assert "state" not in manifest
            assert manifest["sink"]["strategy"] == {"mode": "full_refresh", "max_source_bytes": MAX_SOURCE_BYTES}

    # Descriptors are synthetic authoring metadata. No resolver is invoked.
    projection = _supervised_projection(producer_composition, tmp_path)
    assert projection.deployment["schema"] == "dpone.deployment-set.v3"
    assert projection.airflow_index["schema"] == "dpone.airflow-deployment-index.v3"
    cache, events = tmp_path / ".dpone-cache", []
    backend = OfflineBackend(events)
    store = OfflineProtectedStore(cache, events)
    coordinator = CompositionActivationCoordinator(
        inputs=OfflineInputs(cache),
        preparation=CompositionActivationPreparation(physical=CompositionPhysicalAdmissionService(backend=backend)),
        stores=store,
    )
    materializer = DeploymentCacheMaterializer(cache, composition_activation_coordinator=coordinator)
    backend.execution_cells = CELLS - {"mssql_clickhouse_full_refresh_v1"}
    with pytest.raises(DeploymentCacheError) as missing:
        materializer.promote(
            projection.deployment_dir,
            environment="prod",
            expect_current_absent=True,
            activation_id=ACTIVATION_ID,
        )
    assert missing.value.code == "DPONE_COMPOSITION_ADMISSION_UNAVAILABLE"
    assert store.current is None and not backend.observations
    assert not (cache / "current").exists()
    backend.execution_cells = CELLS
    current = materializer.promote(
        projection.deployment_dir,
        environment="prod",
        expect_current_absent=True,
        activation_id=ACTIVATION_ID,
    )
    assert current.release_id == release_id
    assert store.current.receipt.state == "ACTIVE"
    assert store.current.request.source_subject_sha256 == sources.subject_sha256
    assert store.current.request.workloads == plan.workloads
    assert events[-2:] == ["prepare", "activate"]
    assert Counter(connector for connector, _ in backend.observations) == {"mssql": 1, "clickhouse": 1}
    mssql_writes = next(writes for connector, writes in backend.observations if connector == "mssql")
    assert {write.connection_ref for write in mssql_writes} >= {"mssql_dwh_stage", "ordinary_writer"}
    assert any(write.kind != "transfer" for write in mssql_writes)
    assert any(write.resource_id == "orders" for write in mssql_writes)
    assert sorted(
        subject for resource in store.current.request.resources for subject in resource.write_subjects
    ) == sorted(map(dbt_relation_write_subject, sources.relation_writes))
