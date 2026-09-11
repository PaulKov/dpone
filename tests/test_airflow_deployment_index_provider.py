from __future__ import annotations

import hashlib
import inspect
import json
import sys
import types
import warnings
from collections.abc import Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import dpone_airflow_pack.dag_loader as dag_loader_module
import dpone_airflow_pack.dag_materializer as dag_materializer_module
import dpone_airflow_pack.dag_spec_loader as dag_spec_loader_module
import dpone_airflow_pack.deployment_index_contract as deployment_index_contract
import dpone_airflow_pack.provider as provider_module
import pytest
from dpone_airflow_pack.cache_artifact_contract import verify_cache_artifact
from dpone_airflow_pack.dag_loader import load_dpone_dags
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint, load_dag_spec_file
from dpone_airflow_pack.deployment_index import (
    DEFAULT_MAX_ARTIFACT_BYTES,
    AirflowDeploymentIndex,
    AirflowDeploymentIndexError,
    CacheResolver,
    LoadReport,
    load_airflow_deployment_index,
    resolve_cache_artifact,
)
from dpone_airflow_pack.semantic_refresh_dag_authority import (
    LocalSemanticRefreshDagProjectionAuthority,
    SemanticRefreshDagProjectionIdentity,
)
from jsonschema import Draft202012Validator

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
OTHER_RELEASE_ID = "sha256:" + "c" * 64
OTHER_DEPLOYMENT_ID = "sha256:" + "d" * 64
RELEASE_DIR_NAME = RELEASE_ID.replace(":", "-")
_OMITTED = object()


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _write_minimal_index(
    path: Path,
    *,
    environment: object = "prod",
    deployment_id: str = DEPLOYMENT_ID,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": RELEASE_ID,
        "deployment_id": deployment_id,
        "dag_specs": [],
        "workload_packs": [],
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }
    if environment is not _OMITTED:
        payload["environment"] = environment
    path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


def _config_map_ref(name: str, digit: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-{name}",
        "key": f"{name}.json",
        "sha256": "sha256:" + digit * 64,
    }


def _init_fetch_delivery(registry_ref: str = "dpone-prod-artifacts") -> dict[str, Any]:
    return {
        "mode": "init_fetch",
        "trust_tier": "production",
        "artifact_registry_ref": registry_ref,
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "data-platform",
        },
        "registry_config_ref": _config_map_ref("registry", "1"),
        "trust_policy_ref": _config_map_ref("policy", "2"),
        "source": {"artifact_registry_ref": registry_ref},
        "verify": {
            "checksums": "required",
            "attestations": "required_for_prod",
        },
    }


def _runtime_connection_artifact(name: str, digest_character: str) -> dict[str, str | int]:
    context_root = "cache://runtime-connection-contexts/sha256-" + "a" * 64
    return {
        "artifact_ref": f"{context_root}/{name}.json",
        "sha256": "sha256:" + digest_character * 64,
        "bytes": 1024,
    }


def _v2_init_fetch_index_payload(runtime_delivery: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema": "dpone.airflow-deployment-index.v2",
        "trust_tier": runtime_delivery["trust_tier"],
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "runtime_image_ref": "registry.example/dpone/runtime@sha256:" + "3" * 64,
        "runtime_image_digest": "sha256:" + "3" * 64,
        "binding_set_ref": "sha256:" + "8" * 64,
        "connection_registry_ref": "sha256:" + "9" * 64,
        "credential_runtime_ref": "sha256:" + "0" * 64,
        "binding_set": _runtime_connection_artifact("binding-set", "b"),
        "connection_registry": _runtime_connection_artifact("connection-registry", "c"),
        "credential_runtime": _runtime_connection_artifact("credential-runtime", "d"),
        "airflow_bundle_ref": None,
        "release": {
            "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/release-set.json",
            "sha256": "sha256:" + "4" * 64,
            "bytes": 4096,
        },
        "deployment": {
            "artifact_ref": (f"cache://deployments/prod/{DEPLOYMENT_ID.replace(':', '-')}/deployment.json"),
            "sha256": "sha256:" + "5" * 64,
            "bytes": 2048,
        },
        "dag_specs": [],
        "workload_packs": [
            {
                "id": "orders",
                "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/packs/orders.json",
                "sha256": "sha256:" + "6" * 64,
                "bytes": 8192,
                "pack_fingerprint": "sha256:" + "7" * 64,
            }
        ],
        "runtime_artifact_delivery": dict(runtime_delivery),
    }


def _supervisor_payload() -> dict[str, object]:
    return {
        "schema": "dpone.composition-supervisor.v1",
        "persistent_volume_claim": "dpone-composition-supervisor",
        "child_uid_start": 1_000_000_000,
        "child_gid_start": 1_000_000_000,
        "child_identity_count": 1_000_000,
    }


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(persistent_volume_claim=" dpone-composition-supervisor"),
        lambda value: value.pop("child_gid_start"),
    ],
)
def test_public_provider_reader_rejects_malformed_v3_supervisor(
    tmp_path: Path,
    mutation: Callable[[dict[str, object]], object],
) -> None:
    index_path = tmp_path / "airflow-index.json"
    payload = _v2_init_fetch_index_payload(_init_fetch_delivery())
    payload["schema"] = "dpone.airflow-deployment-index.v3"
    supervisor = _supervisor_payload()
    mutation(supervisor)
    payload["composition_supervisor"] = supervisor
    index_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path, cache_root=tmp_path)

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_INVALID"


@pytest.mark.parametrize(
    "schema",
    ["dpone.airflow-deployment-index.v1", "dpone.airflow-deployment-index.v2"],
)
def test_public_provider_reader_forbids_supervisor_on_v1_v2(
    tmp_path: Path,
    schema: str,
) -> None:
    index_path = tmp_path / "airflow-index.json"
    if schema.endswith(".v1"):
        _write_minimal_index(index_path)
        payload = json.loads(index_path.read_bytes())
    else:
        payload = _v2_init_fetch_index_payload(_init_fetch_delivery())
    payload["composition_supervisor"] = _supervisor_payload()
    index_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path, cache_root=tmp_path)

    assert exc_info.value.code == "DPONE_COMPOSITION_SUPERVISOR_FORBIDDEN"


def _semantic_refresh_projection_descriptor() -> dict[str, object]:
    identity = SemanticRefreshDagProjectionIdentity(
        dag_projection_sha256="sha256:" + "1" * 64,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        topology_sha256="sha256:" + "2" * 64,
        template_pack_fingerprint="sha256:" + "3" * 64,
        plan_bundle_sha256="sha256:" + "4" * 64,
        workflow_plan_sha256="sha256:" + "5" * 64,
        pre_release_bundle_sha256="sha256:" + "6" * 64,
        package_artifacts_sha256="sha256:" + "7" * 64,
    )
    return {
        "projection_id": "semantic_refresh_v2::daily_events",
        "workflow_name": "daily_events",
        "dag_id": "daily_events",
        "dag_projection_sha256": identity.dag_projection_sha256,
        "artifact_ref": (
            "cache://deployments/prod/"
            f"{DEPLOYMENT_ID.replace(':', '-')}/"
            f"semantic-refresh-{identity.dag_projection_sha256.removeprefix('sha256:')}"
            ".dag-projection.json"
        ),
        "artifact_sha256": "sha256:" + "8" * 64,
        "artifact_bytes": 18_626,
        "authority": LocalSemanticRefreshDagProjectionAuthority.build(identity).to_mapping(),
    }


def test_v2_index_projects_deployment_bound_semantic_refresh_sidecar(
    tmp_path: Path,
) -> None:
    deployment_dir = tmp_path / ".dpone-cache" / "deployments" / "prod" / DEPLOYMENT_ID.replace(":", "-")
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    payload = _v2_init_fetch_index_payload(_init_fetch_delivery())
    payload["semantic_refresh_dag_projections"] = [_semantic_refresh_projection_descriptor()]
    index_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = deployment_index_contract._load_airflow_deployment_index_descriptor(index_path)

    assert len(loaded.semantic_refresh_dag_projections) == 1
    artifact = loaded.semantic_refresh_dag_projections[0]
    assert artifact.projection_id == "semantic_refresh_v2::daily_events"
    assert artifact.authority.identity.deployment_id == DEPLOYMENT_ID
    assert artifact.path == (deployment_dir / ("semantic-refresh-" + "1" * 64 + ".dag-projection.json"))


@pytest.mark.parametrize("mutation", ["path", "authority"])
def test_v2_index_rejects_semantic_refresh_sidecar_identity_mismatch(
    tmp_path: Path,
    mutation: str,
) -> None:
    deployment_dir = tmp_path / ".dpone-cache" / "deployments" / "prod" / DEPLOYMENT_ID.replace(":", "-")
    deployment_dir.mkdir(parents=True)
    descriptor = _semantic_refresh_projection_descriptor()
    if mutation == "path":
        descriptor["artifact_ref"] = (
            "cache://deployments/prod/sha256-" + "9" * 64 + "/semantic-refresh-" + "1" * 64 + ".dag-projection.json"
        )
    else:
        authority = dict(descriptor["authority"])
        authority["deployment_id"] = OTHER_DEPLOYMENT_ID
        descriptor["authority"] = authority
    payload = _v2_init_fetch_index_payload(_init_fetch_delivery())
    payload["semantic_refresh_dag_projections"] = [descriptor]
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises((AirflowDeploymentIndexError, ValueError)):
        deployment_index_contract._load_airflow_deployment_index_descriptor(index_path)


def test_strict_v2_loader_installs_semantic_sidecar_in_atomic_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from dpone_airflow_pack.init_fetch_preflight import (
        load_preflighted_init_fetch_dags,
    )
    from dpone_airflow_pack.semantic_refresh_index_artifacts import (
        SemanticRefreshDagProjectionArtifact,
    )

    from tests.semantic_refresh_airflow_pack_fixtures import (
        semantic_refresh_airflow_callables,
        semantic_refresh_dag_projection,
    )

    _install_fake_airflow(monkeypatch)
    payload = semantic_refresh_dag_projection()
    from dpone_airflow_pack.semantic_refresh_dag_projection import (
        validate_semantic_refresh_dag_projection,
    )

    projection = validate_semantic_refresh_dag_projection(payload)
    content = projection.canonical_bytes()
    path = tmp_path / "semantic-refresh.dag-projection.json"
    path.write_bytes(content)
    descriptor = projection.descriptor()
    artifact = SemanticRefreshDagProjectionArtifact(
        projection_id=str(descriptor["projection_id"]),
        workflow_name=str(descriptor["workflow_name"]),
        dag_id=str(descriptor["dag_id"]),
        dag_projection_sha256=str(descriptor["dag_projection_sha256"]),
        artifact_ref="cache://deployments/prod/semantic-refresh.dag-projection.json",
        artifact_sha256=str(descriptor["artifact_sha256"]),
        artifact_bytes=int(descriptor["artifact_bytes"]),
        path=path,
        cache_root=tmp_path,
        authority=LocalSemanticRefreshDagProjectionAuthority.build(projection.identity),
    )
    index = AirflowDeploymentIndex(
        path=tmp_path / "airflow-index.json",
        cache_root=tmp_path,
        release_id=projection.identity.release_id,
        deployment_id=projection.identity.deployment_id,
        dag_specs=(),
        workload_packs=(),
        semantic_refresh_dag_projections=(artifact,),
        airflow_index_sha256=_digest("a"),
        activation_id="00000000-0000-4000-8000-000000000001",
        schema="dpone.airflow-deployment-index.v2",
        delivery_context=SimpleNamespace(),  # type: ignore[arg-type]
    )
    namespace: dict[str, object] = {}

    report = load_preflighted_init_fetch_dags(
        namespace,
        index=index,
        operator_overrides=None,
        duplicate_policy="fail_all",
        invalid_dag_policy="fail_all",
        started_at=0.0,
        load_dag_spec=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected")),
        resolve_node_pack_refs=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected")),
        materialize_dag_spec=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected")),
        semantic_refresh_callables=semantic_refresh_airflow_callables(),
    )

    assert report.loaded == (artifact.dag_id,)
    assert tuple(namespace) == (artifact.dag_id,)
    assert getattr(namespace[artifact.dag_id], "_dpone_spec_fingerprint") == artifact.dag_projection_sha256


def test_strict_v2_loader_never_silently_omits_semantic_sidecar(
    tmp_path: Path,
) -> None:
    from dpone_airflow_pack.init_fetch_preflight import preflight_init_fetch_index
    from dpone_airflow_pack.semantic_refresh_index_artifacts import (
        SemanticRefreshDagProjectionArtifact,
    )

    artifact = SemanticRefreshDagProjectionArtifact(
        projection_id="semantic_refresh_v2::daily_events",
        workflow_name="daily_events",
        dag_id="daily_events",
        dag_projection_sha256=_digest("1"),
        artifact_ref="cache://deployments/prod/sidecar.json",
        artifact_sha256=_digest("2"),
        artifact_bytes=1,
        path=tmp_path / "sidecar.json",
        cache_root=tmp_path,
        authority=LocalSemanticRefreshDagProjectionAuthority.build(
            SemanticRefreshDagProjectionIdentity(
                dag_projection_sha256=_digest("1"),
                release_id=RELEASE_ID,
                deployment_id=DEPLOYMENT_ID,
                topology_sha256=_digest("2"),
                template_pack_fingerprint=_digest("3"),
                plan_bundle_sha256=_digest("4"),
                workflow_plan_sha256=_digest("5"),
                pre_release_bundle_sha256=_digest("6"),
                package_artifacts_sha256=_digest("7"),
            )
        ),
    )
    index = AirflowDeploymentIndex(
        path=tmp_path / "airflow-index.json",
        cache_root=tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        dag_specs=(),
        workload_packs=(),
        semantic_refresh_dag_projections=(artifact,),
        schema="dpone.airflow-deployment-index.v2",
        delivery_context=SimpleNamespace(),  # type: ignore[arg-type]
    )

    with pytest.raises(
        AirflowDeploymentIndexError,
        match="protected worker runtime",
    ) as exc_info:
        preflight_init_fetch_index(
            index,
            operator_overrides={},
            load_dag_spec=lambda *_args, **_kwargs: ({}, ()),
            resolve_node_pack_refs=lambda payload, **_kwargs: payload,
            materialize_dag_spec=lambda *_args, **_kwargs: object(),
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_SEMANTIC_REFRESH_RUNTIME_REQUIRED"


def _install_fake_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = types.ModuleType("airflow")
    sdk = types.ModuleType("airflow.sdk")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    empty_mod = types.ModuleType("airflow.providers.standard.operators.empty")
    python_mod = types.ModuleType("airflow.providers.standard.operators.python")

    class DAG:
        def __init__(self, *, dag_id: str, schedule: object = None, **kwargs: object) -> None:
            self.kwargs = {"dag_id": dag_id, "schedule": schedule, **kwargs}

    class Asset:
        def __init__(self, uri: str) -> None:
            self.uri = uri

    class TaskGroup:
        def __init__(self, *, group_id: str, dag: object = None) -> None:
            self.group_id = group_id
            self.dag = dag

    class EmptyOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: object) -> object:
            return other

    class PythonOperator:
        def __init__(self, **kwargs: object) -> None:
            self.task_id = str(kwargs["task_id"])
            self.kwargs = kwargs

        def __rshift__(self, other: object) -> object:
            return other

    empty_mod.EmptyOperator = EmptyOperator
    airflow.DAG = DAG
    sdk.DAG = DAG
    sdk.Asset = Asset
    sdk.TaskGroup = TaskGroup
    python_mod.PythonOperator = PythonOperator
    for name, module in {
        "airflow": airflow,
        "airflow.sdk": sdk,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.empty": empty_mod,
        "airflow.providers.standard.operators.python": python_mod,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_resolve_cache_artifact_rejects_path_escape(tmp_path: Path) -> None:
    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        resolve_cache_artifact("cache://../outside.json", cache_root=tmp_path)

    assert exc_info.value.code == "DPONE_CACHE_ARTIFACT_REF_UNSAFE"


def test_resolve_cache_artifact_rejects_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "cache"
    root.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        resolve_cache_artifact("cache://link/secret.json", cache_root=root)

    assert exc_info.value.code == "DPONE_CACHE_ARTIFACT_REF_UNSAFE"


@pytest.mark.parametrize("alias", ("current", "CURRENT", "latest", "Latest"))
def test_resolve_cache_artifact_rejects_mutable_pointer_refs(tmp_path: Path, alias: str) -> None:
    root = tmp_path / ".dpone-cache"
    mutable = root / alias
    mutable.mkdir(parents=True)
    (mutable / "orders_daily.dag-spec.json").write_text("{}", encoding="utf-8")

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        resolve_cache_artifact(f"cache://{alias}/orders_daily.dag-spec.json", cache_root=root)

    assert exc_info.value.code == "DPONE_CACHE_UNPINNED_REFERENCE"


def test_verified_cache_artifact_rejects_parent_swapped_to_symlink(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    packs = cache / "releases" / RELEASE_DIR_NAME / "packs"
    packs.mkdir(parents=True)
    pack_path = packs / "orders.airflow-pack.json"
    pack_path.write_text('{"kind":"gitops.airflow_pack"}', encoding="utf-8")
    expected_sha256 = _sha256(pack_path)
    declared_bytes = pack_path.stat().st_size
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / pack_path.name).write_bytes(pack_path.read_bytes())
    releases = cache / "releases"
    releases.rename(cache / "original-releases")
    releases.symlink_to(outside, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        verify_cache_artifact(
            pack_path,
            expected_sha256=expected_sha256,
            declared_bytes=declared_bytes,
            max_artifact_bytes=1024,
            cache_root=cache,
        )

    assert exc_info.value.code == "DPONE_CACHE_ARTIFACT_REF_UNSAFE"


def test_load_airflow_deployment_index_rejects_current_pointer_escape(tmp_path: Path) -> None:
    cache = tmp_path / "custom-cache"
    cache.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "airflow-index.json").write_text("{}", encoding="utf-8")
    (cache / "current").symlink_to(outside, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_READ_FAILED"


def test_current_pointer_projects_workspace_authority_connection_ref(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    activation = cache / "activations" / "prod" / DEPLOYMENT_ID.replace(":", "-")
    index_path = activation / "airflow-index.json"
    _write_minimal_index(index_path, deployment_id=DEPLOYMENT_ID)
    cache.mkdir(exist_ok=True)
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)
    (cache / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "prod",
                "deployment_id": DEPLOYMENT_ID,
                "release_id": RELEASE_ID,
                "activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
                "promoted_by": "airflow-desired-state-watcher",
                "promoted_at": "2026-08-29T00:00:00Z",
                "workspace_authority_connection_ref": "dpone_control",
            }
        ),
        encoding="utf-8",
    )

    loaded = load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert loaded.workspace_authority_connection_ref == "dpone_control"


def test_load_airflow_deployment_index_reads_cache_refs_and_checksums(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    release_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    release_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    spec_path = release_dir / "orders_daily.dag-spec.json"
    spec_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": _sha256(spec_path),
                        "bytes": spec_path.stat().st_size,
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    loaded = load_airflow_deployment_index(index_path)

    assert loaded.release_id == RELEASE_ID
    assert loaded.deployment_id == DEPLOYMENT_ID
    assert loaded.dag_specs[0].path == spec_path.resolve()


def test_public_index_loader_has_no_artifact_verification_bypass() -> None:
    assert "verify_artifacts" not in inspect.signature(load_airflow_deployment_index).parameters


def test_load_airflow_deployment_index_rejects_declared_artifact_size_mismatch(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    release_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    release_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    spec_path = release_dir / "orders_daily.dag-spec.json"
    spec_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": _sha256(spec_path),
                        "bytes": spec_path.stat().st_size + 1,
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH"


def test_load_airflow_deployment_index_accepts_legacy_missing_bytes_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / ".dpone-cache"
    release_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    release_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    spec_path = release_dir / "orders_daily.dag-spec.json"
    spec_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": _sha256(spec_path),
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        deployment_index_contract,
        "_legacy_missing_bytes_warning_pid",
        None,
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        first = load_airflow_deployment_index(index_path)
        second = load_airflow_deployment_index(index_path)

    deprecations = [item for item in caught if issubclass(item.category, DeprecationWarning)]
    assert len(deprecations) == 1
    assert "without 'bytes' is deprecated" in str(deprecations[0].message)
    assert first.dag_specs[0].bytes == spec_path.stat().st_size
    assert second.dag_specs[0].bytes == spec_path.stat().st_size


def test_descriptor_defers_legacy_missing_bytes_for_per_dag_isolation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    broken_path = index_path.parents[3] / "releases" / RELEASE_DIR_NAME / "dags/broken.dag-spec.json"
    index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    broken_artifact = next(item for item in index_payload["dag_specs"] if item["id"] == "broken")
    broken_artifact.pop("bytes")
    index_path.write_text(json.dumps(index_payload, sort_keys=True), encoding="utf-8")
    broken_path.write_text('{"corrupt":true}', encoding="utf-8")

    with pytest.warns(DeprecationWarning, match="without 'bytes' is deprecated"):
        report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert report.fatal is False
    assert len(report.errors) == 1
    assert report.errors[0]["code"] == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert report.errors[0]["dag_id"] == "broken"


@pytest.mark.parametrize("declared_bytes", (0, -1, True, None, "10"))
def test_load_airflow_deployment_index_requires_positive_artifact_bytes(
    tmp_path: Path,
    declared_bytes: object,
) -> None:
    cache = tmp_path / ".dpone-cache"
    release_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    release_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    spec_path = release_dir / "orders_daily.dag-spec.json"
    spec_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": _sha256(spec_path),
                        "bytes": declared_bytes,
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert str(exc_info.value) == "bytes must be a positive integer"


def test_load_airflow_deployment_index_rejects_malformed_artifact_digest(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    release_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    release_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    spec_path = release_dir / "orders_daily.dag-spec.json"
    spec_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": "sha256:not-a-digest",
                        "bytes": spec_path.stat().st_size,
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_ID_INVALID"
    assert "sha256" in str(exc_info.value)


def test_load_airflow_deployment_index_infers_cache_root_through_current_symlink(tmp_path: Path) -> None:
    cache = tmp_path / "custom-cache"
    release_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    activation_dir = cache / "activations" / "prod" / DEPLOYMENT_ID.replace(":", "-")
    release_dir.mkdir(parents=True)
    activation_dir.mkdir(parents=True)
    spec_path = release_dir / "orders_daily.dag-spec.json"
    spec_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    index_path = activation_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "environment": "prod",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": _sha256(spec_path),
                        "bytes": spec_path.stat().st_size,
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    current = cache / "current"
    current.symlink_to(activation_dir.relative_to(cache), target_is_directory=True)

    loaded = load_airflow_deployment_index(current / "airflow-index.json")

    assert loaded.cache_root == cache.resolve()
    assert loaded.dag_specs[0].path == spec_path.resolve()


def test_load_airflow_deployment_index_infers_nested_smoke_cache_under_dpone_cache(
    tmp_path: Path,
) -> None:
    """Smoke-only v2 cache lives under pack-cache/.dpone-cache/smoke-v2-deployment."""

    pack_cache = tmp_path / ".dpone-cache"
    cache = pack_cache / "smoke-v2-deployment"
    release_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    activation_dir = cache / "activations" / "dev" / DEPLOYMENT_ID.replace(":", "-")
    release_dir.mkdir(parents=True)
    activation_dir.mkdir(parents=True)
    # Unrelated compact pack generation must not become the inferred root.
    (pack_cache / "generations" / "deadbeef").mkdir(parents=True)
    (pack_cache / "current").write_text("deadbeef", encoding="utf-8")
    spec_path = release_dir / "orders_daily.dag-spec.json"
    spec_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    index_path = activation_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "environment": "dev",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": _sha256(spec_path),
                        "bytes": spec_path.stat().st_size,
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    current = cache / "current"
    current.symlink_to(activation_dir.relative_to(cache), target_is_directory=True)

    loaded = load_airflow_deployment_index(current / "airflow-index.json")

    assert loaded.cache_root == cache.resolve()
    assert loaded.dag_specs[0].path == spec_path.resolve()


def test_load_airflow_deployment_index_rejects_direct_current_directory(tmp_path: Path) -> None:
    index_path = tmp_path / ".dpone-cache" / "current" / "airflow-index.json"
    _write_minimal_index(index_path)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_READ_FAILED"


@pytest.mark.parametrize(
    "target",
    (
        f"deployments/prod/{DEPLOYMENT_ID.replace(':', '-')}",
        f"activations/prod/{DEPLOYMENT_ID.replace(':', '-').upper()}",
        "activations/prod/sha256-deployment",
        f"activations/prod/../prod/{DEPLOYMENT_ID.replace(':', '-')}",
        f"./activations/prod/{DEPLOYMENT_ID.replace(':', '-')}",
    ),
)
def test_load_airflow_deployment_index_rejects_noncanonical_current_target(
    tmp_path: Path,
    target: str,
) -> None:
    cache = tmp_path / ".dpone-cache"
    index_path = cache / target / "airflow-index.json"
    _write_minimal_index(index_path)
    (cache / "current").symlink_to(target, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_READ_FAILED"


def test_load_airflow_deployment_index_rejects_absolute_current_target(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    activation = tmp_path / "activation"
    _write_minimal_index(activation / "airflow-index.json")
    cache.mkdir()
    (cache / "current").symlink_to(activation, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_READ_FAILED"


def test_load_airflow_deployment_index_rejects_current_activation_symlink_escape(
    tmp_path: Path,
) -> None:
    cache = tmp_path / ".dpone-cache"
    outside = tmp_path / "outside"
    target = Path("activations") / "prod" / DEPLOYMENT_ID.replace(":", "-")
    _write_minimal_index(outside / "prod" / DEPLOYMENT_ID.replace(":", "-") / "airflow-index.json")
    cache.mkdir()
    (cache / "activations").symlink_to(outside, target_is_directory=True)
    (cache / "current").symlink_to(target, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_READ_FAILED"


@pytest.mark.parametrize(
    ("environment", "deployment_id"),
    (
        ("staging", DEPLOYMENT_ID),
        ("prod", OTHER_DEPLOYMENT_ID),
    ),
)
def test_load_airflow_deployment_index_rejects_current_activation_identity_mismatch(
    tmp_path: Path,
    environment: str,
    deployment_id: str,
) -> None:
    cache = tmp_path / ".dpone-cache"
    target = Path("activations") / "prod" / DEPLOYMENT_ID.replace(":", "-")
    _write_minimal_index(
        cache / target / "airflow-index.json",
        environment=environment,
        deployment_id=deployment_id,
    )
    (cache / "current").symlink_to(target, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_CURRENT_IDENTITY_MISMATCH"


def test_load_strict_v2_current_index_requires_activation_occurrence(
    tmp_path: Path,
) -> None:
    cache = tmp_path / ".dpone-cache"
    target = Path("activations") / "prod" / DEPLOYMENT_ID.replace(":", "-")
    payload = _v2_init_fetch_index_payload(_init_fetch_delivery())
    index_path = cache / target / "airflow-index.json"
    index_path.parent.mkdir(parents=True)
    index_path.write_text(json.dumps(payload), encoding="utf-8")
    (cache / "current").symlink_to(target, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_CURRENT_IDENTITY_MISMATCH"


@pytest.mark.parametrize(
    "environment",
    [
        pytest.param(_OMITTED, id="omitted"),
        pytest.param(None, id="null"),
        pytest.param(42, id="non-string"),
    ],
)
def test_current_v1_index_requires_exact_environment(
    tmp_path: Path,
    environment: object,
) -> None:
    cache = tmp_path / ".dpone-cache"
    target = Path("activations") / "prod" / DEPLOYMENT_ID.replace(":", "-")
    _write_minimal_index(
        cache / target / "airflow-index.json",
        environment=environment,
    )
    (cache / "current").symlink_to(target, target_is_directory=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(cache / "current" / "airflow-index.json")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_CURRENT_IDENTITY_MISMATCH"


@pytest.mark.parametrize(
    "environment",
    [
        pytest.param(_OMITTED, id="omitted"),
        pytest.param(None, id="null"),
        pytest.param(42, id="non-string"),
    ],
)
def test_direct_pinned_v1_index_preserves_environment_compatibility(
    tmp_path: Path,
    environment: object,
) -> None:
    index_path = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment" / "airflow-index.json"
    _write_minimal_index(index_path, environment=environment)

    loaded = load_airflow_deployment_index(index_path)

    assert loaded.schema == "dpone.airflow-deployment-index.v1"
    assert loaded.deployment_id == DEPLOYMENT_ID


@pytest.mark.parametrize("duplicate_location", ["top-level", "nested"])
def test_load_airflow_deployment_index_rejects_duplicate_json_keys_before_artifact_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    duplicate_location: str,
) -> None:
    index_path = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment" / "airflow-index.json"
    _write_minimal_index(index_path)
    raw = index_path.read_text(encoding="utf-8")
    if duplicate_location == "top-level":
        raw = raw.removesuffix("}") + ', "opaque": "private-one", "opaque": "private-two"}'
    else:
        raw = raw.replace(
            '"runtime_artifact_delivery": {"mode": "local_preview"}',
            (
                '"runtime_artifact_delivery": {"mode": "local_preview", '
                '"opaque": "private-one", "opaque": "private-two"}'
            ),
            1,
        )
    index_path.write_text(raw, encoding="utf-8")
    artifact_projection_calls: list[str] = []

    def record_artifact_projection(*args: object, **kwargs: object) -> tuple[()]:
        del args, kwargs
        artifact_projection_calls.append("called")
        return ()

    monkeypatch.setattr(
        deployment_index_contract,
        "load_index_artifacts",
        record_artifact_projection,
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_JSON_INVALID"
    assert artifact_projection_calls == []
    assert "opaque" not in str(exc_info.value)
    assert "private-one" not in str(exc_info.value)
    assert "private-two" not in str(exc_info.value)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity", "1e999", "-1e999"])
def test_load_airflow_deployment_index_rejects_non_finite_json_before_artifact_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    constant: str,
) -> None:
    index_path = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment" / "airflow-index.json"
    _write_minimal_index(index_path)
    raw = index_path.read_text(encoding="utf-8")
    index_path.write_text(raw.removesuffix("}") + f', "observed": {constant}}}', encoding="utf-8")
    artifact_projection_calls: list[str] = []

    def record_artifact_projection(*args: object, **kwargs: object) -> tuple[()]:
        del args, kwargs
        artifact_projection_calls.append("called")
        return ()

    monkeypatch.setattr(
        deployment_index_contract,
        "load_index_artifacts",
        record_artifact_projection,
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_JSON_INVALID"
    assert artifact_projection_calls == []
    assert constant not in str(exc_info.value)


def test_load_airflow_deployment_index_preserves_finite_json_numbers(tmp_path: Path) -> None:
    index_path = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment" / "airflow-index.json"
    _write_minimal_index(index_path)
    raw = index_path.read_text(encoding="utf-8")
    index_path.write_text(raw.removesuffix("}") + ', "observed": 1.25e3}', encoding="utf-8")

    loaded = load_airflow_deployment_index(index_path)

    assert loaded.schema == "dpone.airflow-deployment-index.v1"


def test_load_airflow_deployment_index_rejects_json_recursion_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_path = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment" / "airflow-index.json"
    index_path.parent.mkdir(parents=True)
    index_path.write_text('{"nested":' * 10_000 + "0" + "}" * 10_000, encoding="utf-8")
    artifact_projection_calls: list[str] = []

    def record_artifact_projection(*args: object, **kwargs: object) -> tuple[()]:
        del args, kwargs
        artifact_projection_calls.append("called")
        return ()

    monkeypatch.setattr(
        deployment_index_contract,
        "load_index_artifacts",
        record_artifact_projection,
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_JSON_INVALID"
    assert artifact_projection_calls == []


@pytest.mark.parametrize(
    ("section", "artifact_id", "artifact_relative_path"),
    [
        ("dag_specs", "orders_daily", "dags/orders_daily.dag-spec.json"),
        ("workload_packs", "load_orders", "packs/load_orders.airflow-pack.json"),
    ],
)
def test_load_airflow_deployment_index_rejects_duplicate_artifact_ids_before_reads(
    tmp_path: Path,
    section: str,
    artifact_id: str,
    artifact_relative_path: str,
) -> None:
    cache = tmp_path / ".dpone-cache"
    artifact_path = cache / "releases" / RELEASE_DIR_NAME / artifact_relative_path
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    artifact_path.parent.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    artifact_path.write_text('{"kind":"immutable-artifact"}', encoding="utf-8")
    artifact = {
        "id": artifact_id,
        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/{artifact_relative_path}",
        "sha256": _sha256(artifact_path),
        "bytes": artifact_path.stat().st_size,
    }
    artifact_collections = {
        "dag_specs": [artifact, artifact] if section == "dag_specs" else [],
        "workload_packs": [artifact, artifact] if section == "workload_packs" else [],
    }
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                **artifact_collections,
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    artifact_path.unlink()

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_ARTIFACT_DUPLICATE"
    assert artifact_id not in str(exc_info.value)


def test_load_airflow_deployment_index_rejects_missing_dag_specs(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_MISSING"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()
    assert "dag_specs is required" in str(exc_info.value)


def test_load_airflow_deployment_index_rejects_invalid_dag_specs_type(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": "not-a-list",
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()
    assert "dag_specs must be a list" in str(exc_info.value)


def test_load_airflow_deployment_index_rejects_oversized_index_before_parsing(tmp_path: Path) -> None:
    index_path = tmp_path / "airflow-index.json"
    index_path.write_text('{"padding":"' + ("x" * 128) + '"}', encoding="utf-8")

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path, max_index_bytes=64)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_TOO_LARGE"
    assert exc_info.value.path == index_path.resolve().as_posix()


def test_load_airflow_deployment_index_rejects_missing_workload_packs(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_MISSING"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()
    assert "workload_packs is required" in str(exc_info.value)


def test_load_airflow_deployment_index_rejects_invalid_workload_packs_type(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": "not-a-list",
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()
    assert "workload_packs must be a list" in str(exc_info.value)


@pytest.mark.parametrize(
    ("mutation", "sensitive_values"),
    [
        (
            lambda raw: raw.replace(
                '"workload_id": "load_orders"',
                '"workload_id": "load_orders", "workload_id": "private-workload"',
                1,
            ),
            ("private-workload",),
        ),
        (
            lambda raw: raw.replace('"schedule": null', '"schedule": NaN', 1),
            ("NaN",),
        ),
        (
            lambda raw: raw.replace('"schedule": null', '"schedule": Infinity', 1),
            ("Infinity",),
        ),
        (
            lambda raw: raw.replace('"schedule": null', '"schedule": -Infinity', 1),
            ("-Infinity",),
        ),
        (
            lambda raw: raw.replace('"schedule": null', '"schedule": 1e999', 1),
            ("1e999",),
        ),
        (
            lambda raw: raw.replace('"schedule": null', '"schedule": -1e999', 1),
            ("-1e999",),
        ),
    ],
)
def test_dag_spec_rejects_ambiguous_json_before_fingerprint_evaluation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Callable[[str], str],
    sensitive_values: tuple[str, ...],
) -> None:
    payload = _valid_policy_spec_payload("cached://workloads/load_orders")
    path = tmp_path / "orders_daily.dag-spec.json"
    raw = mutation(json.dumps(payload, sort_keys=True))
    path.write_text(raw, encoding="utf-8")
    fingerprint_calls: list[str] = []

    def record_fingerprint(_payload: Mapping[str, Any]) -> str:
        fingerprint_calls.append("called")
        return "sha256:" + "0" * 64

    monkeypatch.setattr(
        dag_spec_loader_module,
        "compute_dag_spec_fingerprint",
        record_fingerprint,
    )

    loaded, issues = load_dag_spec_file(path)

    assert loaded is None
    assert [issue.code for issue in issues] == ["DPONE_AIRFLOW_DAG_SPEC_JSON_INVALID"]
    assert fingerprint_calls == []
    public_error = json.dumps([issue.to_jsonable() for issue in issues])
    for sensitive_value in sensitive_values:
        assert sensitive_value not in public_error


def test_dag_spec_preserves_finite_json_numbers(tmp_path: Path) -> None:
    payload = _valid_policy_spec_payload("cached://workloads/load_orders")
    payload["finite_number"] = 1.25e3
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    path = tmp_path / "orders_daily.dag-spec.json"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    loaded, issues = load_dag_spec_file(path)

    assert issues == ()
    assert loaded is not None
    assert loaded["finite_number"] == 1.25e3


def test_load_airflow_deployment_index_rejects_missing_runtime_artifact_delivery(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_MISSING"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()
    assert "runtime_artifact_delivery is required" in str(exc_info.value)


def test_load_airflow_deployment_index_rejects_invalid_runtime_artifact_delivery_type(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": "local_preview",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()
    assert "runtime_artifact_delivery must be an object" in str(exc_info.value)


@pytest.mark.parametrize("runtime_delivery", [{}, {"mode": "unknown"}])
def test_load_airflow_deployment_index_rejects_invalid_runtime_artifact_delivery_mode(
    tmp_path: Path,
    runtime_delivery: dict[str, object],
) -> None:
    deployment_dir = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": runtime_delivery,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    expected_code = "DPONE_AIRFLOW_INDEX_FIELD_MISSING" if not runtime_delivery else "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert exc_info.value.code == expected_code


def test_load_airflow_deployment_index_rejects_malformed_release_and_deployment_ids(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:release",
                "deployment_id": "sha256:deployment",
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_ID_INVALID"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()


def test_load_airflow_deployment_index_rejects_noncanonical_uppercase_identity(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "A" * 64,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_ID_INVALID"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()


def test_load_airflow_deployment_index_rejects_malformed_optional_digest_refs(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "binding_set_ref": "prod-bindings@sha256:bindings",
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_ID_INVALID"
    assert "binding_set_ref" in str(exc_info.value)


def test_load_airflow_deployment_index_rejects_non_string_optional_identity(tmp_path: Path) -> None:
    deployment_dir = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "binding_set_ref": 42,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert "binding_set_ref" in str(exc_info.value)


def test_load_airflow_deployment_index_requires_v2_migration_for_any_v1_init_fetch(tmp_path: Path) -> None:
    deployment_dir = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"


@pytest.mark.parametrize(
    ("mutation", "expected_message"),
    [
        (lambda payload: payload.pop("identity"), "identity is required"),
        (
            lambda payload: payload["identity"].update({"method": "static_credentials"}),
            "identity.method must be kubernetes_workload_identity",
        ),
        (lambda payload: payload["identity"].pop("service_account"), "service_account is required"),
        (lambda payload: payload.pop("source"), "source is required"),
        (
            lambda payload: payload["source"].update({"artifact_registry_ref": "other-registry"}),
            "source does not match registry reference",
        ),
        (lambda payload: payload.pop("verify"), "verify is required"),
        (
            lambda payload: payload["verify"].update({"checksums": "optional"}),
            "verify.checksums must be required",
        ),
        (
            lambda payload: payload["verify"].update({"attestations": "disabled"}),
            "verify.attestations is invalid",
        ),
        (
            lambda payload: payload.update({"artifact_registry_ref": "cache://registries/current/index.json"}),
            "artifact_registry_ref must be a bounded non-secret",
        ),
    ],
)
def test_load_airflow_deployment_index_rejects_incomplete_init_fetch_contract(
    tmp_path: Path,
    mutation: Callable[[dict[str, Any]], object],
    expected_message: str,
) -> None:
    deployment_dir = tmp_path / ".dpone-cache" / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    runtime_delivery = _init_fetch_delivery()
    mutation(runtime_delivery)
    index_path.write_text(
        json.dumps(_v2_init_fetch_index_payload(runtime_delivery)),
        encoding="utf-8",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_FIELD_INVALID"
    assert expected_message in str(exc_info.value)


def test_load_airflow_deployment_index_rejects_missing_index(tmp_path: Path) -> None:
    index_path = tmp_path / ".dpone-cache" / "current" / "airflow-index.json"

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_NOT_FOUND"
    assert exc_info.value.path == index_path.resolve(strict=False).as_posix()


def test_airflow_deployment_index_error_serializes_dpone_error_contract() -> None:
    error = AirflowDeploymentIndexError(
        "DPONE_CACHE_CHECKSUM_MISMATCH",
        "cache artifact checksum does not match index",
        path=f"cache://releases/{RELEASE_DIR_NAME}/packs/load_orders.airflow-pack.json",
    )

    assert error.to_jsonable() == {
        "schema": "dpone.error.v1",
        "code": "DPONE_CACHE_CHECKSUM_MISMATCH",
        "stage": "airflow_parse",
        "severity": "error",
        "message": "cache artifact checksum does not match index",
        "path": f"cache://releases/{RELEASE_DIR_NAME}/packs/load_orders.airflow-pack.json",
    }


def test_load_dpone_dags_reports_missing_index_without_breaking_parse(tmp_path: Path) -> None:
    index_path = tmp_path / ".dpone-cache" / "current" / "airflow-index.json"

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ()
    assert report.skipped == ()
    assert report.fatal is True
    assert report.errors == (
        {
            "schema": "dpone.error.v1",
            "code": "DPONE_AIRFLOW_INDEX_NOT_FOUND",
            "stage": "airflow_parse",
            "severity": "error",
            "message": "airflow deployment index does not exist",
            "path": index_path.resolve(strict=False).as_posix(),
        },
    )


def test_load_dpone_dags_fail_all_raises_for_missing_index(tmp_path: Path) -> None:
    index_path = tmp_path / ".dpone-cache" / "current" / "airflow-index.json"

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags({}, index_path=index_path, invalid_dag_policy="fail_all")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_NOT_FOUND"


def test_cache_resolver_resolves_dag_and_workload_refs_from_index(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    dag_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    pack_dir = cache / "releases" / RELEASE_DIR_NAME / "packs"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    dag_dir.mkdir(parents=True)
    pack_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    dag_path = dag_dir / "orders_daily.dag-spec.json"
    pack_path = pack_dir / "load_orders.airflow-pack.json"
    dag_path.write_text('{"kind":"gitops.airflow_dag_spec"}', encoding="utf-8")
    pack_path.write_text('{"kind":"gitops.airflow_pack"}', encoding="utf-8")
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "orders_daily",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
                        "sha256": _sha256(dag_path),
                        "bytes": dag_path.stat().st_size,
                    }
                ],
                "workload_packs": [
                    {
                        "id": "load_orders",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/packs/load_orders.airflow-pack.json",
                        "sha256": _sha256(pack_path),
                        "bytes": pack_path.stat().st_size,
                    }
                ],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    resolver = CacheResolver.from_index(index_path)

    workload = resolver.resolve(f"cached://workloads/load_orders?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}")
    strict_workload = resolver.resolve(f"cached://deployments/{DEPLOYMENT_ID}/workloads/load_orders")
    dag = resolver.resolve("cached://dags/orders_daily")
    strict_dag = resolver.resolve(f"cached://deployments/{DEPLOYMENT_ID}/dags/orders_daily")

    assert workload.to_jsonable() == {
        "kind": "workload",
        "logical_id": "load_orders",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/packs/load_orders.airflow-pack.json",
        "resolved_path": pack_path.resolve().as_posix(),
        "sha256": _sha256(pack_path),
        "bytes": pack_path.stat().st_size,
        "provenance": "local_cache",
    }
    assert strict_workload.to_jsonable() == workload.to_jsonable()
    assert dag.kind == "dag"
    assert dag.logical_id == "orders_daily"
    assert dag.resolved_path == dag_path.resolve()
    assert strict_dag.to_jsonable() == dag.to_jsonable()

    with pytest.raises(AirflowDeploymentIndexError) as unknown_pin:
        resolver.resolve("cached://workloads/load_orders?deployment_id=sha256:deployment")
    assert unknown_pin.value.code == "DPONE_CACHE_REF_PIN_INVALID"

    with pytest.raises(AirflowDeploymentIndexError) as duplicate_pin:
        resolver.resolve(
            "cached://workloads/load_orders?release=sha256:other&release=sha256:release&deployment=sha256:deployment"
        )
    assert duplicate_pin.value.code == "DPONE_CACHE_REF_PIN_INVALID"

    with pytest.raises(AirflowDeploymentIndexError) as malformed_query_pin:
        resolver.resolve("cached://workloads/load_orders?release=sha256:release&deployment=sha256:deployment")
    assert malformed_query_pin.value.code == "DPONE_CACHE_REF_PIN_INVALID"

    uppercase_release_id = "sha256:" + RELEASE_ID.split(":", 1)[1].upper()
    with pytest.raises(AirflowDeploymentIndexError) as noncanonical_query_pin:
        resolver.resolve(f"cached://workloads/load_orders?release={uppercase_release_id}&deployment={DEPLOYMENT_ID}")
    assert noncanonical_query_pin.value.code == "DPONE_CACHE_REF_PIN_INVALID"

    with pytest.raises(AirflowDeploymentIndexError) as malformed_deployment_path_pin:
        resolver.resolve("cached://deployments/sha256:deployment/workloads/load_orders")
    assert malformed_deployment_path_pin.value.code == "DPONE_CACHE_REF_PIN_INVALID"


def test_cache_resolver_rejects_unknown_or_mismatched_pinned_ref(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    resolver = CacheResolver.from_index(index_path)

    with pytest.raises(AirflowDeploymentIndexError) as mismatch:
        resolver.resolve(f"cached://workloads/load_orders?release={OTHER_RELEASE_ID}&deployment={DEPLOYMENT_ID}")
    assert mismatch.value.code == "DPONE_CACHE_REF_PIN_MISMATCH"

    with pytest.raises(AirflowDeploymentIndexError) as strict_mismatch:
        resolver.resolve(f"cached://deployments/{OTHER_DEPLOYMENT_ID}/workloads/load_orders")
    assert strict_mismatch.value.code == "DPONE_CACHE_REF_PIN_MISMATCH"

    with pytest.raises(AirflowDeploymentIndexError) as strict_query_mismatch:
        resolver.resolve(f"cached://deployments/{DEPLOYMENT_ID}/workloads/load_orders?deployment={OTHER_DEPLOYMENT_ID}")
    assert strict_query_mismatch.value.code == "DPONE_CACHE_REF_PIN_MISMATCH"

    with pytest.raises(AirflowDeploymentIndexError) as missing:
        resolver.resolve("cached://workloads/load_orders")
    assert missing.value.code == "DPONE_CACHE_REF_NOT_FOUND"


@pytest.mark.parametrize(
    ("collection", "artifact_dir", "artifact_name"),
    [
        ("dag_specs", "dags", "orders_daily.dag-spec.json"),
        ("workload_packs", "packs", "load_orders.airflow-pack.json"),
    ],
)
def test_deployment_index_binds_every_artifact_path_to_release_id(
    tmp_path: Path,
    collection: str,
    artifact_dir: str,
    artifact_name: str,
) -> None:
    cache = tmp_path / ".dpone-cache"
    other_release_dir = OTHER_RELEASE_ID.replace(":", "-")
    artifact_path = cache / "releases" / other_release_dir / artifact_dir / artifact_name
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_text('{"kind":"test"}', encoding="utf-8")
    deployment_dir = cache / "deployments" / "prod" / DEPLOYMENT_ID.replace(":", "-")
    deployment_dir.mkdir(parents=True)
    index_path = deployment_dir / "airflow-index.json"
    artifact_id = "orders_daily" if collection == "dag_specs" else "load_orders"
    payload = {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "dag_specs": [],
        "workload_packs": [],
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }
    payload[collection] = [
        {
            "id": artifact_id,
            "artifact_ref": f"cache://releases/{other_release_dir}/{artifact_dir}/{artifact_name}",
            "sha256": _sha256(artifact_path),
            "bytes": artifact_path.stat().st_size,
        }
    ]
    index_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_airflow_deployment_index(index_path)

    assert exc_info.value.code == "DPONE_RELEASE_ID_MISMATCH"
    assert exc_info.value.path == payload[collection][0]["artifact_ref"]


def test_dpone_task_group_resolves_cached_pack_through_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = tmp_path / ".dpone-cache"
    pack_dir = cache / "releases" / RELEASE_DIR_NAME / "packs"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    pack_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    pack_path = pack_dir / "load_orders.airflow-pack.json"
    pack_path.write_text('{"kind":"gitops.airflow_pack"}', encoding="utf-8")
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [
                    {
                        "id": "load_orders",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/packs/load_orders.airflow-pack.json",
                        "sha256": _sha256(pack_path),
                        "bytes": pack_path.stat().st_size,
                    }
                ],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    calls: list[tuple[Path, str | None, dict[str, object] | None]] = []
    runtime_task = object()

    def fake_builder(
        pack_ref: str | Path,
        *,
        dag: object = None,
        operator_overrides: dict[str, object] | None = None,
        expected_sha256: str | None = None,
        run_identity_context: dict[str, object] | None = None,
        confined_root: Path | None = None,
    ) -> dict[str, object]:
        del dag, operator_overrides, confined_root
        calls.append((Path(pack_ref), expected_sha256, run_identity_context))
        return {"dpone_runtime": runtime_task}

    monkeypatch.setattr(provider_module, "_airflow_task_group_class", lambda: None)
    monkeypatch.setattr(provider_module, "build_dpone_gitops_task_group_from_pack", fake_builder)

    result = provider_module.DponeTaskGroup.from_pack(
        f"cached://workloads/load_orders?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
        index_path=index_path,
    )

    assert result == {"dpone_runtime": runtime_task}
    assert calls == [
        (
            pack_path.resolve(),
            _sha256(pack_path),
            {
                "schema": "dpone.airflow-run-identity.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_spec": None,
                "runtime_image_digest": None,
                "binding_set_ref": None,
                "connection_registry_ref": None,
                "credential_runtime_ref": None,
                "airflow_bundle": None,
                "_workload_pack_sha256": {"load_orders": _sha256(pack_path)},
            },
        )
    ]


def test_dpone_task_group_keeps_cache_confinement_for_final_pack_consumer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    observed_roots: list[Path | None] = []

    def fake_builder(
        pack_ref: str | Path,
        *,
        dag: object = None,
        operator_overrides: dict[str, object] | None = None,
        expected_sha256: str | None = None,
        run_identity_context: dict[str, object] | None = None,
        confined_root: Path | None = None,
    ) -> dict[str, object]:
        del pack_ref, dag, operator_overrides, expected_sha256, run_identity_context
        observed_roots.append(confined_root)
        return {"dpone_runtime": object()}

    monkeypatch.setattr(provider_module, "_airflow_task_group_class", lambda: None)
    monkeypatch.setattr(provider_module, "build_dpone_gitops_task_group_from_pack", fake_builder)

    provider_module.DponeTaskGroup.from_pack(
        f"cached://deployments/{DEPLOYMENT_ID}/workloads/load_orders",
        index_path=index_path,
    )

    assert observed_roots == [index_path.parents[3]]


def test_dpone_task_group_uses_logical_group_id_for_deployment_scoped_cached_pack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    sdk_mod = types.ModuleType("airflow.sdk")

    class TaskGroup:
        def __init__(self, *, group_id: str, dag: object = None) -> None:
            self.group_id = group_id
            self.dag = dag

    sdk_mod.TaskGroup = TaskGroup
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_mod)
    calls: list[tuple[Path, dict[str, object] | None, str | None, dict[str, object] | None]] = []

    def fake_builder(
        pack_ref: str | Path,
        *,
        dag: object = None,
        operator_overrides: dict[str, object] | None = None,
        task_group: object = None,
        expected_sha256: str | None = None,
        run_identity_context: dict[str, object] | None = None,
        confined_root: Path | None = None,
    ) -> dict[str, object]:
        del dag, confined_root
        if task_group is not None:
            operator_overrides = {**(operator_overrides or {}), "task_group": task_group}
        calls.append((Path(pack_ref), operator_overrides, expected_sha256, run_identity_context))
        return {"dpone_runtime": object()}

    monkeypatch.setattr(provider_module, "build_dpone_gitops_task_group_from_pack", fake_builder)

    group = provider_module.DponeTaskGroup.from_pack(
        f"cached://deployments/{DEPLOYMENT_ID}/workloads/load_orders",
        index_path=index_path,
    )

    assert group.group_id == "load_orders"
    assert calls[0][0] == _policy_pack_path(index_path).resolve()
    assert calls[0][1] is not None
    assert calls[0][1]["task_group"] is group
    assert calls[0][2] == _sha256(_policy_pack_path(index_path))
    assert calls[0][3] is not None
    assert calls[0][3]["deployment_id"] == DEPLOYMENT_ID
    assert calls[0][3]["dag_spec"] is None


def test_dpone_task_group_rejects_deployment_id_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    def fail_if_called(*args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("deployment_id mismatch must fail before calling the pack builder")

    monkeypatch.setattr(provider_module, "build_dpone_gitops_task_group_from_pack", fail_if_called)

    ref = f"cached://workloads/load_orders?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}"
    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeTaskGroup.from_pack(
            ref,
            index_path=index_path,
            deployment_id=OTHER_DEPLOYMENT_ID,
        )

    assert exc_info.value.code == "DPONE_DEPLOYMENT_ID_MISMATCH"
    assert exc_info.value.path == ref
    assert OTHER_DEPLOYMENT_ID in str(exc_info.value)
    assert DEPLOYMENT_ID in str(exc_info.value)


def test_dpone_task_group_rejects_dag_cached_ref(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    def fail_if_called(*args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("DponeTaskGroup.from_pack must reject cached DAG refs before calling the pack builder")

    monkeypatch.setattr(provider_module, "build_dpone_gitops_task_group_from_pack", fail_if_called)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeTaskGroup.from_pack(
            f"cached://dags/orders_daily?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
            index_path=index_path,
        )

    assert exc_info.value.code == "DPONE_CACHE_REF_INVALID"


def test_dpone_task_group_requires_index_for_cached_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_if_called(*args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("cached workload refs must fail before calling the pack builder when index_path is missing")

    monkeypatch.setattr(provider_module, "build_dpone_gitops_task_group_from_pack", fail_if_called)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeTaskGroup.from_pack("cached://workloads/load_orders")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_REQUIRED"


def test_dpone_dag_resolves_pinned_cached_spec_through_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    dag = provider_module.DponeDag.from_spec(
        f"cached://dags/orders_daily?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
        index_path=index_path,
    )

    assert dag.kwargs["dag_id"] == "orders_daily"


def test_dpone_dag_uses_one_pinned_deployment_index_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    original = provider_module._load_airflow_deployment_index_descriptor
    calls: list[Path] = []

    def load_once(path: str | Path, **kwargs: object) -> AirflowDeploymentIndex:
        calls.append(Path(path))
        return original(path, **kwargs)

    monkeypatch.setattr(provider_module, "_load_airflow_deployment_index_descriptor", load_once)

    dag = provider_module.DponeDag.from_spec(
        f"cached://dags/orders_daily?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
        index_path=index_path,
    )

    assert dag.kwargs["dag_id"] == "orders_daily"
    assert calls == [index_path]


def test_dpone_dag_requires_index_path() -> None:
    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeDag.from_spec("cached://dags/orders_daily")

    assert exc_info.value.code == "DPONE_AIRFLOW_INDEX_REQUIRED"


def test_dpone_dag_returns_target_when_unrelated_index_spec_is_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)

    dag = provider_module.DponeDag.from_spec(
        f"cached://dags/orders_daily?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
        index_path=index_path,
    )

    assert dag.kwargs["dag_id"] == "orders_daily"


def test_dpone_dag_raises_structured_error_for_requested_invalid_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeDag.from_spec(
            f"cached://dags/broken?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
            index_path=index_path,
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_SPEC_NODES_MISSING"
    assert "broken.dag-spec.json" in str(exc_info.value.path)


def test_dpone_dag_preserves_requested_materialization_failure_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    def fail_materialize(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("simulated materialization failure")

    monkeypatch.setattr(dag_loader_module, "_materialize_dag_spec", fail_materialize)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeDag.from_spec(
            f"cached://dags/orders_daily?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
            index_path=index_path,
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED"
    assert "simulated materialization failure" in str(exc_info.value)
    assert "orders_daily.dag-spec.json" in str(exc_info.value.path)


def test_dpone_dag_raises_structured_error_for_missing_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeDag.from_spec("missing_daily", index_path=index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_SPEC_NOT_FOUND"
    assert "missing_daily" in str(exc_info.value)


def test_dpone_dag_missing_target_ignores_unrelated_invalid_dag_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeDag.from_spec("missing_daily", index_path=index_path)

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_SPEC_NOT_FOUND"
    assert exc_info.value.path == "missing_daily"


def test_dpone_dag_from_spec_replaces_stale_globals_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    existing = object()
    globals_dict = {"orders_daily": existing}

    dag = provider_module.DponeDag.from_spec(
        f"cached://dags/orders_daily?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
        index_path=index_path,
        globals_dict=globals_dict,
    )

    assert dag is not existing
    assert globals_dict["orders_daily"] is dag
    assert dag.kwargs["dag_id"] == "orders_daily"


def test_dpone_dag_rejects_mismatched_pinned_cached_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeDag.from_spec(
            f"cached://dags/orders_daily?release={RELEASE_ID}&deployment={OTHER_DEPLOYMENT_ID}",
            index_path=index_path,
        )

    assert exc_info.value.code == "DPONE_CACHE_REF_PIN_MISMATCH"


def test_dpone_dag_rejects_workload_cached_ref(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        provider_module.DponeDag.from_spec(
            f"cached://workloads/load_orders?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
            index_path=index_path,
        )

    assert exc_info.value.code == "DPONE_CACHE_REF_INVALID"


def test_deployment_index_loader_is_parse_safe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "vault_kv_client", None)

    index_path = tmp_path / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )

    report = load_dpone_dags({}, index_path=index_path)

    assert report.release_id == RELEASE_ID
    assert report.deployment_id == DEPLOYMENT_ID
    assert report.loaded == ()
    assert report.errors == ()


def test_load_dpone_dags_skips_invalid_index_spec_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert len(report.errors) == 1
    assert report.errors[0]["code"] == "DPONE_AIRFLOW_DAG_SPEC_NODES_MISSING"
    assert report.errors[0]["schema"] == "dpone.error.v1"
    assert report.errors[0]["stage"] == "airflow_parse"
    assert report.errors[0]["severity"] == "error"
    assert report.errors[0]["message"] == "nodes must be a non-empty list"
    assert report.errors[0]["path"].endswith("broken.dag-spec.json")


def test_load_dpone_dags_isolates_dag_spec_read_failure_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    original = dag_loader_module.load_dag_spec_file
    observed_max_bytes: list[int] = []

    def fail_one_spec(
        path: Path,
        *,
        expected_sha256: str | None = None,
        expected_bytes: int | None = None,
        max_bytes: int,
        confined_root: Path | None = None,
    ):
        observed_max_bytes.append(max_bytes)
        if path.name == "broken.dag-spec.json":
            raise OSError("simulated local cache read failure")
        return original(
            path,
            expected_sha256=expected_sha256,
            expected_bytes=expected_bytes,
            max_bytes=max_bytes,
            confined_root=confined_root,
        )

    monkeypatch.setattr(dag_loader_module, "load_dag_spec_file", fail_one_spec)

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert len(report.errors) == 1
    assert report.errors[0]["code"] == "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED"
    assert report.errors[0]["dag_id"] == "broken"
    assert report.errors[0]["path"].endswith("broken.dag-spec.json")
    assert set(observed_max_bytes) == {DEFAULT_MAX_ARTIFACT_BYTES}


def test_load_dpone_dags_isolates_physical_dag_spec_corruption_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    broken_path = index_path.parents[3] / "releases" / RELEASE_DIR_NAME / "dags/broken.dag-spec.json"
    original = broken_path.read_text(encoding="utf-8")
    broken_path.write_text(original.replace('"broken"', '"BROKEN"', 1), encoding="utf-8")

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert report.fatal is False
    assert len(report.errors) == 1
    assert report.errors[0]["code"] == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert report.errors[0]["dag_id"] == "broken"


def test_load_dpone_dags_isolates_cached_pack_ref_resolution_errors_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(
        tmp_path,
        malformed=True,
        broken_pack_ref="cached://workloads/missing_pack",
    )

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert len(report.errors) == 1
    assert report.errors[0]["schema"] == "dpone.error.v1"
    assert report.errors[0]["code"] == "DPONE_CACHE_REF_NOT_FOUND"
    assert report.errors[0]["stage"] == "airflow_parse"
    assert report.errors[0]["severity"] == "error"
    assert report.errors[0]["path"] == "cached://workloads/missing_pack"


@pytest.mark.parametrize("reference_mode", ["absolute_pack_ref", "non_cache_uri", "pack_path_only"])
def test_deployment_index_loader_rejects_non_cached_pack_references_before_consumption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reference_mode: str,
) -> None:
    _install_fake_airflow(monkeypatch)
    outside_pack = tmp_path / "outside-pack.json"
    outside_pack.write_text(json.dumps({"kind": "gitops.airflow_pack"}), encoding="utf-8")
    pack_ref = outside_pack.as_posix()
    pack_path_only: str | None = None
    if reference_mode == "non_cache_uri":
        pack_ref = outside_pack.as_uri()
    elif reference_mode == "pack_path_only":
        pack_ref = "cached://workloads/load_orders"
        pack_path_only = outside_pack.as_posix()
    index_path = _write_policy_index_fixture(
        tmp_path,
        malformed=False,
        pack_ref=pack_ref,
        pack_path_only=pack_path_only,
    )
    consumed: list[str] = []

    def fail_if_consumed(*args: object, **kwargs: object) -> None:
        del args, kwargs
        consumed.append(outside_pack.as_posix())
        raise AssertionError("non-cache pack reached the pack consumer")

    monkeypatch.setattr(dag_materializer_module, "wire_pack_workload", fail_if_consumed)

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ()
    assert report.errors[0]["code"] == "DPONE_CACHE_REF_INVALID"
    assert consumed == []


def test_load_dpone_dags_fail_all_policy_rejects_cached_pack_ref_resolution_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(
        tmp_path,
        malformed=True,
        broken_pack_ref="cached://workloads/missing_pack",
    )

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags({}, index_path=index_path, invalid_dag_policy="fail_all")

    assert exc_info.value.code == "DPONE_CACHE_REF_NOT_FOUND"
    assert exc_info.value.path == "cached://workloads/missing_pack"


def test_load_dpone_dags_fail_all_leaves_namespace_unchanged_after_late_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(
        tmp_path,
        malformed=True,
        broken_pack_ref="cached://workloads/missing_pack",
    )
    broken_path = index_path.parents[3] / "releases" / RELEASE_DIR_NAME / "dags/broken.dag-spec.json"
    broken_payload = json.loads(broken_path.read_text(encoding="utf-8"))
    broken_payload["dag_id"] = "zz_broken"
    broken_payload["spec_fingerprint"] = compute_dag_spec_fingerprint(broken_payload)
    broken_path.write_text(json.dumps(broken_payload, sort_keys=True), encoding="utf-8")
    index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    broken_artifact = next(item for item in index_payload["dag_specs"] if item["id"] == "broken")
    broken_artifact.update(
        {
            "id": "zz_broken",
            "sha256": _sha256(broken_path),
            "bytes": broken_path.stat().st_size,
        }
    )
    index_path.write_text(json.dumps(index_payload, sort_keys=True), encoding="utf-8")
    sentinel = object()
    globals_dict: dict[str, object] = {"sentinel": sentinel}

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags(
            globals_dict,
            index_path=index_path,
            invalid_dag_policy="fail_all",
        )

    assert exc_info.value.code == "DPONE_CACHE_REF_NOT_FOUND"
    assert globals_dict == {"sentinel": sentinel}
    assert globals_dict["sentinel"] is sentinel


def test_load_dpone_dags_reports_materialization_failure_with_structured_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    def fail_materialize(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("simulated materialization failure")

    monkeypatch.setattr(dag_loader_module, "_materialize_dag_spec", fail_materialize)

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ()
    assert len(report.errors) == 1
    assert report.errors[0]["schema"] == "dpone.error.v1"
    assert report.errors[0]["code"] == "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED"
    assert report.errors[0]["stage"] == "airflow_parse"
    assert report.errors[0]["severity"] == "error"
    assert report.errors[0]["dag_id"] == "orders_daily"
    assert report.errors[0]["message"] == "simulated materialization failure"
    assert report.errors[0]["path"].endswith("orders_daily.dag-spec.json")


def test_load_dpone_dags_redacts_exception_text_in_report_and_diagnostic_dag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    secret_message = (
        "failed uri=https://admin:super-secret@example.test/packs token=plain-token authorization=Bearer bearer-token"
    )

    def fail_materialize(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError(secret_message)

    monkeypatch.setattr(dag_loader_module, "_materialize_dag_spec", fail_materialize)
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        invalid_dag_policy="create_diagnostic_dag",
    )

    diagnostic_doc = globals_dict["orders_daily"].kwargs["doc_md"]
    public_text = json.dumps(report.to_jsonable()) + str(diagnostic_doc)
    assert "[REDACTED]" in public_text
    assert "super-secret" not in public_text
    assert "plain-token" not in public_text
    assert "bearer-token" not in public_text


def test_load_dpone_dags_fail_all_policy_rejects_invalid_index_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags({}, index_path=index_path, invalid_dag_policy="fail_all")

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_SPEC_INVALID"


def test_load_dpone_dags_rejects_unknown_invalid_dag_policy(tmp_path: Path) -> None:
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags({}, index_path=index_path, invalid_dag_policy="quarantine")

    assert exc_info.value.code == "DPONE_AIRFLOW_POLICY_INVALID"
    assert exc_info.value.path == "invalid_dag_policy"
    assert "invalid_dag_policy" in str(exc_info.value)
    assert "skip_and_report" in str(exc_info.value)


def test_load_dpone_dags_rejects_unknown_duplicate_policy(tmp_path: Path) -> None:
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags({}, index_path=index_path, duplicate_policy="replace")

    assert exc_info.value.code == "DPONE_AIRFLOW_POLICY_INVALID"
    assert exc_info.value.path == "duplicate_policy"
    assert "duplicate_policy" in str(exc_info.value)
    assert "replace_if_same_fingerprint" in str(exc_info.value)


def test_load_dpone_dags_creates_diagnostic_dag_only_when_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(globals_dict, index_path=index_path, invalid_dag_policy="create_diagnostic_dag")

    assert report.loaded == ("orders_daily",)
    assert "broken" in globals_dict
    diagnostic = globals_dict["broken"]
    assert diagnostic.kwargs["is_paused_upon_creation"] is True
    assert "dpone_spec_error" in diagnostic.kwargs["tags"]


def test_repeated_index_retry_is_stable_and_repair_replaces_stale_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    globals_dict: dict[str, object] = {}

    load_dpone_dags(
        globals_dict,
        index_path=index_path,
        invalid_dag_policy="create_diagnostic_dag",
    )
    retry_one = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        invalid_dag_policy="create_diagnostic_dag",
    )
    namespace_after_retry_one = {key: id(value) for key, value in globals_dict.items()}
    retry_two = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        invalid_dag_policy="create_diagnostic_dag",
    )

    assert _stable_report(retry_one) == _stable_report(retry_two)
    assert {key: id(value) for key, value in globals_dict.items()} == namespace_after_retry_one

    broken_path = index_path.parents[3] / "releases" / RELEASE_DIR_NAME / "dags/broken.dag-spec.json"
    repaired_payload = _valid_policy_spec_payload("cached://workloads/load_orders")
    repaired_payload["dag_id"] = "broken"
    repaired_payload["spec_fingerprint"] = compute_dag_spec_fingerprint(repaired_payload)
    broken_path.write_text(json.dumps(repaired_payload, sort_keys=True), encoding="utf-8")
    index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    broken_artifact = next(item for item in index_payload["dag_specs"] if item["id"] == "broken")
    broken_artifact.update(
        {
            "sha256": _sha256(broken_path),
            "bytes": broken_path.stat().st_size,
        }
    )
    index_path.write_text(json.dumps(index_payload, sort_keys=True), encoding="utf-8")

    repaired_report = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        invalid_dag_policy="create_diagnostic_dag",
    )

    assert "broken" in repaired_report.loaded
    assert not any(error.get("dag_id") == "broken" for error in repaired_report.errors)
    assert "dpone_spec_error" not in globals_dict["broken"].kwargs.get("tags", ())


def test_index_duplicate_is_decided_before_malformed_artifact_read_or_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    original = dag_loader_module.load_dag_spec_file
    accessed_specs: list[str] = []

    def record_access(
        path: Path,
        *,
        expected_sha256: str | None = None,
        expected_bytes: int | None = None,
        max_bytes: int,
        confined_root: Path | None = None,
    ):
        accessed_specs.append(path.name)
        return original(
            path,
            expected_sha256=expected_sha256,
            expected_bytes=expected_bytes,
            max_bytes=max_bytes,
            confined_root=confined_root,
        )

    monkeypatch.setattr(dag_loader_module, "load_dag_spec_file", record_access)
    existing = object()
    globals_dict: dict[str, object] = {"broken": existing}

    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert report.errors == ()
    assert report.skipped == ({"dag_id": "broken", "reason": "duplicate_dag_id"},)
    assert accessed_specs == ["orders_daily.dag-spec.json"]
    assert globals_dict["broken"] is existing


def test_public_diagnostic_tag_does_not_grant_loader_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    existing = SimpleNamespace(kwargs={"tags": ["dpone_spec_error"]})
    globals_dict: dict[str, object] = {"orders_daily": existing}

    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.loaded == ()
    assert report.errors == ()
    assert report.skipped == ({"dag_id": "orders_daily", "reason": "duplicate_dag_id"},)
    assert globals_dict["orders_daily"] is existing


def test_index_loader_reports_payload_dag_id_mismatch_against_artifact_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    spec_path = index_path.parents[3] / "releases" / RELEASE_DIR_NAME / "dags/orders_daily.dag-spec.json"
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    payload["dag_id"] = "payload_alias"
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    spec_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    index_payload["dag_specs"][0].update(
        {
            "sha256": _sha256(spec_path),
            "bytes": spec_path.stat().st_size,
        }
    )
    index_path.write_text(json.dumps(index_payload, sort_keys=True), encoding="utf-8")
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.loaded == ()
    assert report.skipped == ()
    assert globals_dict == {}
    assert report.errors[0]["code"] == "DPONE_AIRFLOW_DAG_SPEC_ID_MISMATCH"
    assert report.errors[0]["dag_id"] == "orders_daily"


def test_index_payload_dag_id_mismatch_preserves_fail_all_atomicity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    spec_path = index_path.parents[3] / "releases" / RELEASE_DIR_NAME / "dags/broken.dag-spec.json"
    payload = _valid_policy_spec_payload("cached://workloads/load_orders")
    payload["dag_id"] = "payload_alias"
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    spec_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    artifact = next(item for item in index_payload["dag_specs"] if item["id"] == "broken")
    artifact.update(
        {
            "sha256": _sha256(spec_path),
            "bytes": spec_path.stat().st_size,
        }
    )
    index_path.write_text(json.dumps(index_payload, sort_keys=True), encoding="utf-8")
    sentinel = object()
    globals_dict: dict[str, object] = {"sentinel": sentinel}

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags(
            globals_dict,
            index_path=index_path,
            invalid_dag_policy="fail_all",
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_SPEC_ID_MISMATCH"
    assert globals_dict == {"sentinel": sentinel}
    assert globals_dict["sentinel"] is sentinel


def test_diagnostic_policy_never_overwrites_occupied_invalid_dag_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    existing = object()
    globals_dict: dict[str, object] = {"broken": existing}

    report = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        duplicate_policy="replace_if_same_fingerprint",
        invalid_dag_policy="create_diagnostic_dag",
    )

    assert report.loaded == ("orders_daily",)
    assert report.errors
    assert globals_dict["broken"] is existing


def test_same_fingerprint_replacement_failure_preserves_existing_dag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    expected_fingerprint = _valid_policy_spec_fingerprint("cached://workloads/load_orders")
    existing = SimpleNamespace(_dpone_spec_fingerprint=expected_fingerprint)
    globals_dict: dict[str, object] = {"orders_daily": existing}

    def fail_materialize(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("simulated replacement failure")

    monkeypatch.setattr(dag_loader_module, "_materialize_dag_spec", fail_materialize)

    report = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        duplicate_policy="replace_if_same_fingerprint",
        invalid_dag_policy="create_diagnostic_dag",
    )

    assert report.loaded == ()
    assert report.errors[0]["code"] == "DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED"
    assert globals_dict["orders_daily"] is existing


def test_load_dpone_dags_fail_all_policy_rejects_duplicate_dag_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)

    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        load_dpone_dags({"orders_daily": object()}, index_path=index_path, duplicate_policy="fail_all")

    assert exc_info.value.code == "DPONE_AIRFLOW_DAG_DUPLICATE"


def test_load_dpone_dags_replaces_duplicate_with_same_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    expected_fingerprint = _valid_policy_spec_fingerprint("cached://workloads/load_orders")
    existing = SimpleNamespace(_dpone_spec_fingerprint=expected_fingerprint)
    globals_dict: dict[str, object] = {"orders_daily": existing}

    report = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        duplicate_policy="replace_if_same_fingerprint",
    )

    assert report.loaded == ("orders_daily",)
    assert report.skipped == ()
    assert globals_dict["orders_daily"] is not existing
    assert getattr(globals_dict["orders_daily"], "_dpone_spec_fingerprint") == expected_fingerprint


def test_load_dpone_dags_skips_duplicate_with_different_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=False)
    existing = SimpleNamespace(_dpone_spec_fingerprint="sha256:other")
    globals_dict: dict[str, object] = {"orders_daily": existing}

    report = load_dpone_dags(
        globals_dict,
        index_path=index_path,
        duplicate_policy="replace_if_same_fingerprint",
    )

    assert report.loaded == ()
    assert report.skipped == ({"dag_id": "orders_daily", "reason": "duplicate_dag_id_fingerprint_mismatch"},)
    assert globals_dict["orders_daily"] is existing


def test_load_dpone_dags_resolves_deployment_scoped_node_pack_ref(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(
        tmp_path,
        malformed=False,
        pack_ref=f"cached://deployments/{DEPLOYMENT_ID}/workloads/load_orders",
    )

    report = load_dpone_dags({}, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert report.errors == ()


def test_resolved_dag_node_retains_pack_digest_for_final_consumer(tmp_path: Path) -> None:
    index_path = _write_policy_index_fixture(
        tmp_path,
        malformed=False,
        pack_ref=f"cached://workloads/load_orders?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}",
    )
    resolver = CacheResolver.from_index(index_path)

    resolved = dag_materializer_module._resolve_node_pack_refs(
        _valid_policy_spec_payload(f"cached://workloads/load_orders?release={RELEASE_ID}&deployment={DEPLOYMENT_ID}"),
        cache_resolver=resolver,
    )

    node = resolved["nodes"][0]
    assert node["pack_ref"] == _policy_pack_path(index_path).resolve().as_posix()
    assert node["pack_sha256"] == _sha256(_policy_pack_path(index_path))


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("node_not_mapping", "DPONE_AIRFLOW_DAG_SPEC_NODE_INVALID"),
        ("duplicate_node", "DPONE_AIRFLOW_DAG_SPEC_NODE_ID_DUPLICATE"),
        ("unknown_edge_endpoint", "DPONE_AIRFLOW_DAG_SPEC_EDGE_ENDPOINT_UNKNOWN"),
        ("invalid_topological_order", "DPONE_AIRFLOW_DAG_SPEC_TOPOLOGICAL_ORDER_INVALID"),
        ("default_args_not_mapping", "DPONE_AIRFLOW_DAG_SPEC_DEFAULT_ARGS_INVALID"),
        ("retry_delay_invalid", "DPONE_AIRFLOW_DAG_SPEC_RETRY_DELAY_INVALID"),
        ("retry_delay_negative", "DPONE_AIRFLOW_DAG_SPEC_RETRY_DELAY_INVALID"),
        ("missing_fingerprint", "DPONE_AIRFLOW_DAG_SPEC_FINGERPRINT_MISSING"),
    ],
)
def test_scheduler_loader_rejects_structurally_invalid_dag_specs(
    tmp_path: Path,
    case: str,
    expected_code: str,
) -> None:
    payload = _valid_policy_spec_payload("cached://workloads/load_orders")
    if case == "node_not_mapping":
        payload["nodes"] = [*payload["nodes"], "not-a-mapping"]  # type: ignore[index]
    elif case == "duplicate_node":
        payload["nodes"] = [*payload["nodes"], dict(payload["nodes"][0])]  # type: ignore[index]
    elif case == "unknown_edge_endpoint":
        payload["edges"] = [
            {
                "upstream": "load_orders",
                "downstream": "missing",
                "reason": "declared",
                "origin": "depends_on",
            }
        ]
    elif case == "invalid_topological_order":
        payload["topological_order"] = ["missing"]
    elif case == "default_args_not_mapping":
        payload["default_args"] = "retries=1"
    elif case == "retry_delay_invalid":
        payload["default_args"] = {"retry_delay_minutes": "five"}
    elif case == "retry_delay_negative":
        payload["default_args"] = {"retry_delay_minutes": -1}
    if case == "missing_fingerprint":
        payload.pop("spec_fingerprint")
    else:
        payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    path = tmp_path / "orders_daily.dag-spec.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded, issues = load_dag_spec_file(path)

    assert loaded is None
    assert expected_code in {issue.code for issue in issues}


def test_load_report_errors_validate_against_canonical_error_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_airflow(monkeypatch)
    index_path = _write_policy_index_fixture(tmp_path, malformed=True)
    error_schema = json.loads(Path("docs/schemas/gitops/error.schema.json").read_text(encoding="utf-8"))

    report = load_dpone_dags({}, index_path=index_path)

    assert report.errors
    validator = Draft202012Validator(error_schema)
    for error in report.errors:
        assert not list(validator.iter_errors(error))


def _write_policy_index_fixture(
    tmp_path: Path,
    *,
    malformed: bool,
    pack_ref: str | None = None,
    pack_path_only: str | None = None,
    broken_pack_ref: str | None = None,
) -> Path:
    cache = tmp_path / ".dpone-cache"
    cache.mkdir(parents=True, exist_ok=True)
    (cache / ".promotion.lock").touch(mode=0o644, exist_ok=True)
    dags_dir = cache / "releases" / RELEASE_DIR_NAME / "dags"
    packs_dir = cache / "releases" / RELEASE_DIR_NAME / "packs"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    dags_dir.mkdir(parents=True)
    packs_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    pack_path = packs_dir / "load_orders.airflow-pack.json"
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "kpo_kwargs": {
                    "task_id": "load_orders__dpone_runtime",
                    "name": "load-orders",
                    "namespace": "airflow",
                },
                "runtime_command": "echo ok",
                "steps": [],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    valid_payload = _valid_policy_spec_payload(pack_ref or "cached://workloads/load_orders")
    if pack_path_only is not None:
        valid_node = valid_payload["nodes"][0]  # type: ignore[index]
        valid_node.pop("pack_ref", None)
        valid_node["pack_path"] = pack_path_only
        valid_payload["spec_fingerprint"] = compute_dag_spec_fingerprint(valid_payload)
    valid_path = dags_dir / "orders_daily.dag-spec.json"
    valid_path.write_text(json.dumps(valid_payload, sort_keys=True), encoding="utf-8")
    dag_specs = [
        {
            "id": "orders_daily",
            "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/orders_daily.dag-spec.json",
            "sha256": _sha256(valid_path),
            "bytes": valid_path.stat().st_size,
        }
    ]
    if malformed:
        invalid_path = dags_dir / "broken.dag-spec.json"
        if broken_pack_ref is None:
            invalid_payload = {
                "kind": "gitops.airflow_dag_spec",
                "schema_version": "1",
                "producer": "test",
                "dag_id": "broken",
                "schedule": None,
                "start_date": "2026-07-07",
                "nodes": [],
                "edges": [],
                "topological_order": [],
            }
        else:
            invalid_payload = _valid_policy_spec_payload(broken_pack_ref)
            invalid_payload["dag_id"] = "broken"
        invalid_payload["spec_fingerprint"] = compute_dag_spec_fingerprint(invalid_payload)
        invalid_path.write_text(json.dumps(invalid_payload, sort_keys=True), encoding="utf-8")
        dag_specs.append(
            {
                "id": "broken",
                "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/dags/broken.dag-spec.json",
                "sha256": _sha256(invalid_path),
                "bytes": invalid_path.stat().st_size,
            }
        )
    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": dag_specs,
                "workload_packs": [
                    {
                        "id": "load_orders",
                        "artifact_ref": f"cache://releases/{RELEASE_DIR_NAME}/packs/load_orders.airflow-pack.json",
                        "sha256": _sha256(pack_path),
                        "bytes": pack_path.stat().st_size,
                    }
                ],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return index_path


def _valid_policy_spec_payload(pack_ref: str | Path) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": "orders_daily",
        "schedule": None,
        "start_date": "2026-07-07",
        "nodes": [
            {
                "node_id": "load_orders",
                "workload_id": "load_orders",
                "pack_ref": str(pack_ref),
            }
        ],
        "edges": [],
        "topological_order": ["load_orders"],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    return payload


def _valid_policy_spec_fingerprint(pack_path: str | Path) -> str:
    return str(_valid_policy_spec_payload(pack_path)["spec_fingerprint"])


def _policy_pack_path(index_path: Path) -> Path:
    return index_path.parents[3] / "releases" / RELEASE_DIR_NAME / "packs" / "load_orders.airflow-pack.json"


def _stable_report(report: LoadReport) -> dict[str, object]:
    payload = report.to_jsonable()
    payload.pop("duration_ms")
    return payload
