from __future__ import annotations

import base64
import hashlib
import io
import json
import sys
import tarfile
import types
from dataclasses import replace
from pathlib import Path

import pytest
from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack
from dpone_airflow_pack.connection_env_operator import AirflowConnectionEnvKubernetesPodOperator
from dpone_airflow_pack.connection_secret_lifecycle import (
    ATTEMPT_REF_ANNOTATION,
    CLEANUP_POLICY_LABEL,
    LIFECYCLE_VERSION_LABEL,
    MANAGED_BY_LABEL,
    RESOURCE_KIND_LABEL,
    SECRET_REF_ANNOTATION,
    SECRET_RESOURCE_KIND,
)
from dpone_airflow_pack.operators import (
    AirflowConnectionSecretVolumeKubernetesPodOperator,
    PinnedXComSidecarKubernetesPodOperator,
    UnsafeAirflowConnectionEnvKubernetesPodOperator,
    stringify_env_vars,
)
from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

from dpone.gitops.airflow_compact_pack_bootstrap import (
    InlineWorkloadConfigurationError,
    runtime_workload_bootstrap,
)


def _closed_env_projection() -> dict:
    return {
        "mode": "env",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "connections": [
            {"connection_ref": "warehouse", "registry_connection_ref": "warehouse", "connection_id": "warehouse_reader"}
        ],
    }


@pytest.mark.parametrize("model_pod", [False, True])
def test_closed_env_operator_resolves_only_at_execute_into_base(monkeypatch, model_pod) -> None:
    import copy

    reads = []
    masked = []
    uri = "postgres://synthetic:sentinel-password@warehouse.example/source?token=sentinel-token"

    class Reader:
        def read_uri(self, connection_id):
            reads.append(connection_id)
            return uri

    pod = {
        "spec": {
            "containers": [{"name": "base"}, {"name": "airflow-xcom-sidecar"}],
            "initContainers": [
                {
                    "name": "dpone-runtime-init-fetch",
                    "env": [
                        {
                            "name": "AIRFLOW_CONN_REGISTRY",
                            "valueFrom": {"secretKeyRef": {"name": "registry", "key": "uri"}},
                        },
                    ],
                }
            ],
        }
    }
    if model_pod:
        from dpone_airflow_pack.pack_task_runtime import deserialize_pod_spec

        pod = deserialize_pod_spec(pod)
        if isinstance(pod, dict):
            pytest.skip("Kubernetes models unavailable")
    original = copy.deepcopy(pod)
    operator = AirflowConnectionEnvKubernetesPodOperator(
        task_id="runtime",
        airflow_connection_projection=_closed_env_projection(),
        airflow_connection_reader=Reader(),
        connection_secret_masker=masked.append,
        full_pod_spec=pod,
    )
    assert reads == []
    operator.build_pod_request_obj()
    assert reads == []

    def execute(self, context):
        built = self.build_pod_request_obj(context)
        payload = built if isinstance(built, dict) else built.to_dict()
        base_env = payload["spec"]["containers"][0]["env"]
        assert any(item["name"] == "AIRFLOW_CONN_WAREHOUSE_READER" and item["value"] == uri for item in base_env)
        assert "sentinel-password" not in repr(payload["spec"]["containers"][1:])
        init = payload["spec"].get("initContainers", payload["spec"].get("init_containers"))
        assert "sentinel-password" not in repr(init)
        assert "AIRFLOW_CONN_REGISTRY" in repr(init)
        assert "sentinel-password" not in repr(self.env_vars)
        assert "sentinel-password" not in repr(self.full_pod_spec)
        self.pod_request_obj = built
        return {"status": "success"}

    monkeypatch.setattr(PinnedXComSidecarKubernetesPodOperator, "execute", execute)
    assert operator.execute({}) == {"status": "success"}
    assert reads == ["warehouse_reader"]
    assert uri in masked and "sentinel-password" in masked and "sentinel-token" in masked
    assert operator._runtime_connection_env == {}
    assert "sentinel-password" not in repr(operator.pod_request_obj)
    assert "sentinel-password" not in repr(original)
    assert operator.kwargs["log_pod_spec_on_failure"] is False


@pytest.mark.parametrize(
    "change",
    [
        {"secret_name": "forbidden"},
        {"secret_values": True},
        {"database_overrides": {"unrelated": "warehouse"}},
        {
            "connections": [
                {"connection_ref": "one", "registry_connection_ref": "one", "connection_id": "reader.one"},
                {"connection_ref": "two", "registry_connection_ref": "two", "connection_id": "reader_one"},
            ]
        },
    ],
)
def test_closed_env_projection_rejects_unclosed_or_colliding_metadata(change) -> None:
    from dpone_airflow_pack.init_fetch_connection_bridge import require_closed_init_fetch_connection_bridge
    from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError

    with pytest.raises(InitFetchProviderError):
        require_closed_init_fetch_connection_bridge({**_closed_env_projection(), **change})


def test_closed_env_projection_selects_native_operator_without_secret_projection() -> None:
    from dpone_airflow_pack.pack_task_runtime import operator_class, operator_init_kwargs, operator_selection_pack

    pack = {"connection_projection": _closed_env_projection()}
    assert operator_class(operator_selection_pack(pack, strict=True)) is AirflowConnectionEnvKubernetesPodOperator
    assert operator_init_kwargs(pack, {})["airflow_connection_projection"] == pack["connection_projection"]


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "api_key",
        "access-token",
        "privateKey",
        "service_account_json",
        "keyfile_dict",
        "extra__google_cloud_platform__keyfile_dict",
    ],
)
def test_closed_env_projection_rejects_credential_overrides_without_echoing_values(key) -> None:
    from dpone_airflow_pack.connection_env_projection import require_connection_env_projection
    from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError

    projection = {**_closed_env_projection(), "query_overrides": {"warehouse_reader": {key: "sentinel-secret"}}}
    with pytest.raises(InitFetchProviderError) as error:
        require_connection_env_projection(projection)
    assert "sentinel-secret" not in str(error.value)


def test_closed_env_operator_redacts_resolution_failure_and_clears_values() -> None:
    class Reader:
        def read_uri(self, connection_id):
            raise ValueError("sentinel-secret-error")

    operator = AirflowConnectionEnvKubernetesPodOperator(
        task_id="runtime",
        airflow_connection_projection=_closed_env_projection(),
        airflow_connection_reader=Reader(),
        connection_secret_masker=lambda value: None,
    )
    with pytest.raises(RuntimeError, match="environment resolution failed") as error:
        operator.execute({})
    assert "sentinel-secret-error" not in str(error.value)
    assert error.value.__suppress_context__
    assert operator._runtime_connection_env == {}


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("deferred", [False, True])
def test_closed_env_security_lifecycle_cleans_failure_and_deferral(monkeypatch, resumed, deferred) -> None:
    import copy

    class Deferred(BaseException):
        pass

    uri = "postgres://reader:original-sentinel@warehouse.example/source"
    masked = []
    reads = []

    class Reader:
        def read_uri(self, connection_id):
            reads.append(connection_id)
            return uri

    pod = {
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "env": [
                        {"name": "AIRFLOW_CONN_WAREHOUSE_READER", "value": uri},
                    ],
                }
            ]
        }
    }

    class Hook:
        def get_pod(self, name, namespace):
            assert (name, namespace) == ("runtime", "test")
            return copy.deepcopy(pod)

    monkeypatch.setattr(PinnedXComSidecarKubernetesPodOperator, "hook", property(lambda self: Hook()), raising=False)
    operator = AirflowConnectionEnvKubernetesPodOperator(
        task_id="runtime",
        airflow_connection_projection=_closed_env_projection(),
        airflow_connection_reader=Reader(),
        connection_secret_masker=masked.append,
    )
    failure = Deferred if deferred else RuntimeError

    def parent_call(self, context, event=None, **kwargs):
        self.pod = self.hook.get_pod("runtime", "test")
        assert uri in masked and "original-sentinel" in masked
        assert self.log_pod_spec_on_failure is False
        self.pod_request_obj = copy.deepcopy(self.pod)
        raise failure("synthetic parent failure")

    method = "trigger_reentry" if resumed else "execute"
    monkeypatch.setattr(PinnedXComSidecarKubernetesPodOperator, method, parent_call)
    with pytest.raises(failure):
        if resumed:
            operator.trigger_reentry({}, {"name": "runtime", "namespace": "test"})
        else:
            operator.execute({})
    assert reads == ([] if resumed else ["warehouse_reader"])
    assert operator._runtime_connection_env == {}
    assert operator._active_connection_masker is None
    assert "original-sentinel" not in repr(operator.pod)
    assert "original-sentinel" not in repr(operator.pod_request_obj)
    assert "original-sentinel" in repr(pod)  # The remote response source is unchanged.


def test_closed_env_reattach_masks_original_credentials_after_rotation(monkeypatch) -> None:
    original_uri = "postgres://reader:original-reattach-sentinel@warehouse.example/source"
    rotated_uri = "postgres://reader:rotated-reattach-sentinel@warehouse.example/source"
    masked = []
    selected = {
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "env": [
                        {"name": "AIRFLOW_CONN_WAREHOUSE_READER", "value": original_uri},
                    ],
                }
            ]
        }
    }

    class Reader:
        def read_uri(self, connection_id):
            return rotated_uri

    def select_existing(self, pod_request_obj, context):
        assert rotated_uri in masked
        assert original_uri not in masked
        return selected

    def consume_selected(self, context):
        self.pod = self.get_or_create_pod(None, context)
        assert self.pod is selected
        assert original_uri in masked
        assert "original-reattach-sentinel" in masked
        raise RuntimeError("synthetic reattachment failure")

    monkeypatch.setattr(PinnedXComSidecarKubernetesPodOperator, "get_or_create_pod", select_existing)
    monkeypatch.setattr(PinnedXComSidecarKubernetesPodOperator, "execute", consume_selected)
    operator = AirflowConnectionEnvKubernetesPodOperator(
        task_id="runtime",
        airflow_connection_projection=_closed_env_projection(),
        airflow_connection_reader=Reader(),
        connection_secret_masker=masked.append,
    )
    with pytest.raises(RuntimeError, match="synthetic reattachment failure"):
        operator.execute({})
    assert operator._runtime_connection_env == {}
    assert "original-reattach-sentinel" not in repr(operator.pod)


def test_closed_env_real_provider_reentry_masks_original_pod_before_failure_logs(monkeypatch) -> None:
    """Exercise the installed native provider entry point without cluster I/O."""
    from dpone_airflow_pack.operators import KubernetesPodOperator

    if not KubernetesPodOperator.__module__.startswith("airflow."):
        pytest.skip("Real Kubernetes provider is not installed")
    from airflow.exceptions import AirflowException
    from airflow.sdk.log import redact
    from dpone_airflow_pack.connection_env_operator import _airflow_mask_secret
    from kubernetes.client import models as k8s

    original_uri = "postgres://reader:original-pod-sentinel@warehouse.example/source"
    masked = []
    native_masker = _airflow_mask_secret()

    def mask_secret(value):
        native_masker(value)
        masked.append(value)

    class Reader:
        def read_uri(self, connection_id):
            pytest.fail("Reentry must mask the immutable Pod rather than re-resolve rotated credentials")

    pod = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="runtime", namespace="test"),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    env=[
                        k8s.V1EnvVar(name="AIRFLOW_CONN_WAREHOUSE_READER", value=original_uri),
                    ],
                )
            ]
        ),
        status=k8s.V1PodStatus(phase="Failed"),
    )

    class Hook:
        def get_pod(self, name, namespace):
            return pod

    monkeypatch.setattr(KubernetesPodOperator, "hook", property(lambda self: Hook()))
    monkeypatch.setattr(KubernetesPodOperator, "client", property(lambda self: object()))

    def write_logs(self, pod, **kwargs):
        assert original_uri in masked and "original-pod-sentinel" in masked
        assert "original-pod-sentinel" not in redact(repr(pod))
        assert self.log_pod_spec_on_failure is False

    monkeypatch.setattr(KubernetesPodOperator, "_write_logs", write_logs)
    monkeypatch.setattr(KubernetesPodOperator, "_clean", lambda self, **kwargs: None)
    operator = AirflowConnectionEnvKubernetesPodOperator(
        task_id="runtime",
        airflow_connection_projection=_closed_env_projection(),
        airflow_connection_reader=Reader(),
        connection_secret_masker=mask_secret,
        deferrable=True,
    )
    with pytest.raises(AirflowException, match="synthetic failure"):
        operator.trigger_reentry(
            {}, {"name": "runtime", "namespace": "test", "status": "failed", "message": "synthetic failure"}
        )
    assert "original-pod-sentinel" not in repr(operator.pod)
    assert operator._active_connection_masker is None


@pytest.fixture(autouse=True)
def _stub_dependency_light_kpo_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    monkeypatch.setattr(
        base_operator,
        "execute",
        lambda self, context: {
            "kind": "test.kpo_execute",
            "task_id": getattr(self, "kwargs", {}).get("task_id"),
        },
    )


def _install_fake_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    standard_operators = types.ModuleType("airflow.providers.standard.operators")
    python = types.ModuleType("airflow.providers.standard.operators.python")
    cncf = types.ModuleType("airflow.providers.cncf")
    kubernetes = types.ModuleType("airflow.providers.cncf.kubernetes")
    operators = types.ModuleType("airflow.providers.cncf.kubernetes.operators")
    pod = types.ModuleType("airflow.providers.cncf.kubernetes.operators.pod")

    class KubernetesPodOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    class PythonOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    pod.KubernetesPodOperator = KubernetesPodOperator
    python.PythonOperator = PythonOperator
    for name, module in {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": standard_operators,
        "airflow.providers.standard.operators.python": python,
        "airflow.providers.cncf": cncf,
        "airflow.providers.cncf.kubernetes": kubernetes,
        "airflow.providers.cncf.kubernetes.operators": operators,
        "airflow.providers.cncf.kubernetes.operators.pod": pod,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _install_fake_kubernetes(monkeypatch: pytest.MonkeyPatch) -> None:
    kubernetes = types.ModuleType("kubernetes")
    client = types.ModuleType("kubernetes.client")
    models = types.ModuleType("kubernetes.client.models")

    class V1Pod:
        pass

    class ApiClient:
        def _ApiClient__deserialize_model(self, data: object, klass: type[object]) -> object:
            def convert(value: object) -> object:
                if isinstance(value, dict):
                    return types.SimpleNamespace(**{key: convert(item) for key, item in value.items()})
                if isinstance(value, list):
                    return [convert(item) for item in value]
                return value

            pod = klass()
            for key, value in dict(data).items():
                setattr(pod, key, convert(value))
            return pod

    client.ApiClient = ApiClient
    models.V1Pod = V1Pod
    client.models = models
    kubernetes.client = client
    monkeypatch.setitem(sys.modules, "kubernetes", kubernetes)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client)
    monkeypatch.setitem(sys.modules, "kubernetes.client.models", models)


def _airflow_task_context() -> dict[str, object]:
    return {
        "task_instance": types.SimpleNamespace(
            dag_id="orders_daily",
            task_id="orders__dpone_runtime",
            run_id="manual__2026-07-16T00:00:00+00:00",
            try_number=1,
            map_index=-1,
        )
    }


def test_structured_runtime_bootstrap_contains_argv_not_scheduler_shell() -> None:
    bootstrap = runtime_workload_bootstrap(
        runtime_manifest_path=".dpone/runtime/orders/manifest.json",
        workload_id="orders",
        process_selectors=("orders_daily",),
    )

    assert bootstrap == {
        "schema": "dpone.airflow-runtime-bootstrap.v1",
        "commands": {
            "orders": {
                "argv": [
                    "dpone",
                    "run",
                    ".dpone/runtime/orders/manifest.json",
                    "--format",
                    "json",
                    "--selector",
                    "orders",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "__workload__": {
                "argv": [
                    "dpone",
                    "run",
                    ".dpone/runtime/orders/manifest.json",
                    "--format",
                    "json",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "orders_daily": {
                "argv": [
                    "dpone",
                    "run",
                    ".dpone/runtime/orders/manifest.json",
                    "--format",
                    "json",
                    "--selector",
                    "orders_daily",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
        },
    }
    assert "/bin/sh" not in json.dumps(bootstrap)


def test_structured_runtime_bootstrap_keys_single_process_by_workload_id() -> None:
    bootstrap = runtime_workload_bootstrap(
        runtime_manifest_path="dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
        workload_id="mssql_sale_plan_type1_smoke",
        process_selectors=(None,),
    )

    assert bootstrap == {
        "schema": "dpone.airflow-runtime-bootstrap.v1",
        "commands": {
            "mssql_sale_plan_type1_smoke": {
                "argv": [
                    "dpone",
                    "run",
                    "dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
                    "--format",
                    "json",
                    "--selector",
                    "mssql_sale_plan_type1_smoke",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "__workload__": {
                "argv": [
                    "dpone",
                    "run",
                    "dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
                    "--format",
                    "json",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "__default_process__": {
                "argv": [
                    "dpone",
                    "run",
                    "dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
                    "--format",
                    "json",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
        },
    }


@pytest.mark.parametrize(
    ("workload_id", "process_selectors"),
    (
        ("__workload__", ()),
        ("__default_process__", ()),
        ("orders", ("__workload__",)),
        ("orders", ("__default_process__",)),
    ),
)
def test_structured_runtime_bootstrap_reserves_whole_workload_key(
    workload_id: str,
    process_selectors: tuple[str | None, ...],
) -> None:
    with pytest.raises(
        InlineWorkloadConfigurationError,
        match="runtime_bootstrap_selector_reserved",
    ):
        runtime_workload_bootstrap(
            runtime_manifest_path="runtime/orders.yaml",
            workload_id=workload_id,
            process_selectors=process_selectors,
        )


def test_structured_runtime_bootstrap_keeps_default_workload_id_distinct_from_default_process() -> None:
    bootstrap = runtime_workload_bootstrap(
        runtime_manifest_path="runtime/orders.yaml",
        workload_id="__default__",
        process_selectors=(None,),
    )

    commands = bootstrap["commands"]
    assert isinstance(commands, dict)
    assert commands["__default__"]["argv"][-2:] == ["--selector", "__default__"]
    assert commands["__default_process__"]["argv"] == [
        "dpone",
        "run",
        "runtime/orders.yaml",
        "--format",
        "json",
    ]


def test_runnable_pack_reuses_one_digest_pinned_archive_for_structured_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
    from dpone.runtime.runtime_payload_archive import runtime_payload_archive
    from tests.airflow_dag_spec_repo import write_manifest

    manifest = write_manifest(tmp_path, "orders")
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "registry.example/dpone:dev", "airflow": {}},
        provenance={},
    )
    reads: list[str] = []
    from dpone.gitops import airflow_compact_pack_bootstrap as bootstrap_module

    real_read = bootstrap_module.read_confined_file

    def counted_read(root: Path, relative: str, *, max_bytes: int) -> bytes:
        reads.append(relative)
        return real_read(root, relative, max_bytes=max_bytes)

    monkeypatch.setattr(bootstrap_module, "read_confined_file", counted_read)

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="packs/orders.airflow-pack.json",
        repo_root=tmp_path,
    )
    payload = report.to_jsonable()
    runtime_payload = payload["runtime_payload"]
    archive = runtime_payload["archive"]
    compressed = base64.b64decode(archive["data"], validate=True)

    assert runtime_payload["schema"] == "dpone.airflow-runtime-payload.v1"
    assert payload["pack_identity"] == {"schema": "dpone.airflow-pack-identity.v1"}
    assert verify_pack_fingerprint(payload) == payload["pack_fingerprint"]
    assert archive["encoding"] == "base64"
    assert archive["format"] == "tar+gzip"
    assert archive["bytes"] == len(compressed) > 0
    assert archive["sha256"] == "sha256:" + hashlib.sha256(compressed).hexdigest()
    assert base64.b64encode(compressed).decode("ascii") == archive["data"]
    decoded_by_runtime = runtime_payload_archive(payload)
    assert decoded_by_runtime.data == compressed
    assert decoded_by_runtime.sha256 == archive["sha256"]
    assert decoded_by_runtime.declared_bytes == archive["bytes"]
    assert reads.count(manifest) == 1
    init_script = payload["pod_spec"]["spec"]["initContainers"][0]["args"][0]
    assert archive["data"] in init_script
    assert "/bin/sh" not in json.dumps(payload["runtime_bootstrap"])

    with tarfile.open(fileobj=io.BytesIO(compressed), mode="r:gz") as unpacked:
        members = unpacked.getmembers()
    member_names = [member.name for member in members]
    assert members
    assert len(member_names) == len(set(member_names))
    assert all(member.isfile() for member in members)
    assert all(not name.startswith("/") and ".." not in Path(name).parts for name in member_names)

    schema = json.loads(Path("docs/schemas/gitops/airflow-pack.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(payload, schema)
    for invalid_payload in (
        {
            **runtime_payload,
            "secret": "must-not-enter-the-pack",
        },
        {
            **runtime_payload,
            "archive": {**archive, "bytes": 0},
        },
        {
            **runtime_payload,
            "archive": {**archive, "sha256": "sha256:" + "A" * 64},
        },
        {
            **runtime_payload,
            "archive": {**archive, "data": archive["data"] + "\\n"},
        },
    ):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({**payload, "runtime_payload": invalid_payload}, schema)

    without_payload = replace(report, runtime_payload=None).to_jsonable()
    assert without_payload["pack_fingerprint"] != payload["pack_fingerprint"]
    assert report.runtime_payload is not None
    changed_descriptor = replace(
        report,
        runtime_payload=replace(
            report.runtime_payload,
            bytes=report.runtime_payload.bytes + 1,
        ),
    ).to_jsonable()
    assert changed_descriptor["pack_fingerprint"] != payload["pack_fingerprint"]


def test_pack_tasks_deserializes_json_pod_spec_when_kubernetes_client_is_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack_path = tmp_path / ".dpone/gitops/airflow/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders"},
                "pod_spec": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "dpone-orders"},
                    "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
                },
                "kpo_kwargs": {
                    "task_id": "orders__dpone_runtime",
                    "name": "dpone-orders",
                    "namespace": "airflow-dev",
                    "image": "registry.example/dpone:dev",
                },
            }
        ),
        encoding="utf-8",
    )
    _install_fake_airflow(monkeypatch)
    _install_fake_kubernetes(monkeypatch)

    runtime = build_dpone_gitops_task_group_from_pack(pack_path, dag=object())["dpone_runtime"]

    assert runtime.kwargs["full_pod_spec"].metadata.name == "dpone-orders"
    assert runtime.kwargs["full_pod_spec"].spec.containers[0].image == "registry.example/dpone:dev"


def test_pack_tasks_selects_airflow_connection_secret_volume_operator(tmp_path: Path) -> None:
    pack_path = tmp_path / ".dpone/gitops/airflow/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    projection = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "connections": [
            {
                "connection_ref": "mssql_dev",
                "registry_connection_ref": "mssql_dev",
                "connection_id": "mssql_prod",
                "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                "fields": {"uri": "uri"},
            }
        ],
    }
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders"},
                "connection_projection": projection,
                "pod_spec": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "dpone-orders"},
                    "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
                },
                "kpo_kwargs": {
                    "task_id": "orders__dpone_runtime",
                    "name": "dpone-orders",
                    "namespace": "airflow-dev",
                    "image": "registry.example/dpone:dev",
                },
            }
        ),
        encoding="utf-8",
    )

    runtime = build_dpone_gitops_task_group_from_pack(pack_path, dag=object())["dpone_runtime"]

    assert isinstance(runtime, AirflowConnectionSecretVolumeKubernetesPodOperator)
    assert runtime.airflow_connection_projection == projection


def test_unsafe_airflow_connection_env_operator_injects_connection_overrides_at_execute_time() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return {
                "mssql_prod": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
                "ClickHouse": "http://user:pwd@clickhouse.example/default?secure=false",
            }[connection_id]

    reader = Reader()
    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        image="registry.example/dpone:dev",
        namespace="airflow-dev",
        unsafe_airflow_connection_ids=("mssql_prod", "ClickHouse"),
        unsafe_connection_reader=reader,
        unsafe_runtime_database_overrides={"ClickHouse": "DWH_Raw"},
        unsafe_runtime_scheme_overrides={"ClickHouse": "clickhouse"},
        unsafe_runtime_query_overrides={"mssql_prod": {"trust_server_certificate": "yes"}},
        env_vars={"KEEP_ME": "1"},
    )

    operator.execute(context={})

    assert reader.connection_ids == ["mssql_prod", "ClickHouse"]
    assert operator.env_vars == {
        "KEEP_ME": "1",
        "AIRFLOW_CONN_MSSQL_PROD": (
            "mssql+pymssql://user:pwd@mssql.example/analytics_reporting?trust_server_certificate=yes"
        ),
        "AIRFLOW_CONN_CLICKHOUSE": "clickhouse://user:pwd@clickhouse.example/DWH_Raw?secure=false",
    }


def test_airflow_connection_secret_volume_operator_projects_uri_files_without_env_leak() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return {
                "mssql_prod": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
                "clickhouse_prod": "clickhouse://user:pwd@clickhouse.example/default?secure=false",
            }[connection_id]

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "dpone-orders"},
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "image": "registry.example/dpone:dev",
                    "env": [{"name": "KEEP_ME", "value": "1"}],
                }
            ]
        },
    }
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        full_pod_spec=pod,
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                },
                {
                    "connection_ref": "clickhouse_dev",
                    "registry_connection_ref": "clickhouse_dev",
                    "connection_id": "clickhouse_prod",
                    "secret_key": "AIRFLOW_CONN_CLICKHOUSE_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/clickhouse_dev",
                    "fields": {"uri": "uri"},
                },
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
        env_vars={"DPONE_TRY_NUMBER": 1},
    )

    operator.execute(context=_airflow_task_context())

    assert reader.connection_ids == ["mssql_prod", "clickhouse_prod"]
    assert operator.env_vars == {"DPONE_TRY_NUMBER": 1}
    container = operator.full_pod_spec["spec"]["containers"][0]
    assert container["env"] == [{"name": "KEEP_ME", "value": "1"}]
    published_secret = projector.events[0][2]
    assert isinstance(published_secret, dict)
    published_name = published_secret["metadata"]["name"]
    assert published_name.startswith("dpone-airflow-connection-bridge-")
    assert container["volumeMounts"] == [
        {
            "name": "dpone-airflow-connection-bridge",
            "mountPath": "/run/secrets/dpone/airflow-connections",
            "readOnly": True,
        }
    ]
    assert operator.full_pod_spec["spec"]["volumes"] == [
        {
            "name": "dpone-airflow-connection-bridge",
            "secret": {
                "secretName": published_name,
                "items": [
                    {
                        "key": "AIRFLOW_CONN_MSSQL_PROD",
                        "path": "mssql_dev/uri",
                    },
                    {
                        "key": "AIRFLOW_CONN_CLICKHOUSE_PROD",
                        "path": "clickhouse_dev/uri",
                    },
                ],
            },
        }
    ]
    assert published_secret["apiVersion"] == "v1"
    assert published_secret["kind"] == "Secret"
    assert published_secret["type"] == "Opaque"
    assert published_secret["immutable"] is True
    assert published_secret["metadata"]["labels"] == {
        MANAGED_BY_LABEL: "dpone",
        RESOURCE_KIND_LABEL: SECRET_RESOURCE_KIND,
        LIFECYCLE_VERSION_LABEL: "v1",
        CLEANUP_POLICY_LABEL: "after_execute",
    }
    annotations = published_secret["metadata"]["annotations"]
    assert annotations[SECRET_REF_ANNOTATION] == operator.airflow_connection_projected_secret_ref["secret_ref"]
    assert annotations[ATTEMPT_REF_ANNOTATION].startswith("sha256:")
    assert published_secret["stringData"] == {
        "AIRFLOW_CONN_MSSQL_PROD": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
        "AIRFLOW_CONN_CLICKHOUSE_PROD": "clickhouse://user:pwd@clickhouse.example/default?secure=false",
    }
    assert operator.airflow_connection_projected_secret == operator.airflow_connection_projected_secret_ref
    assert operator.airflow_connection_projected_secret is not None
    assert set(operator.airflow_connection_projected_secret) == {
        "secret_ref",
        "attempt_ref",
        "cleanup_policy",
    }
    assert projector.events == [
        ("upsert", "airflow-example", published_secret),
        ("delete", "airflow-example", published_name),
    ]
    assert "mssql+pymssql://" not in json.dumps(operator.full_pod_spec)
    assert "clickhouse://user:pwd" not in json.dumps(operator.full_pod_spec)
    built = operator.build_pod_request_obj(context=_airflow_task_context())
    assert built["spec"]["volumes"][0]["secret"]["items"][0]["path"] == "mssql_dev/uri"


def test_airflow_connection_secret_volume_operator_rejects_uri_connection_id_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "postgres://etl:secret@pg.internal:5432/dwh",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context=_airflow_task_context())

    assert "logical Airflow Connection id" in str(exc_info.value)
    assert "etl:secret" not in str(exc_info.value)
    assert reader.connection_ids == []
    assert projector.events == []


def test_airflow_connection_secret_volume_operator_rejects_invalid_secret_key_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD\nPASSWORD=secret",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context=_airflow_task_context())

    assert "AIRFLOW_CONN_*" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert reader.connection_ids == []
    assert projector.events == []


def test_airflow_connection_secret_volume_operator_rejects_invalid_secret_name_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge\nPASSWORD=secret",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context=_airflow_task_context())

    assert "Kubernetes Secret name" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert reader.connection_ids == []
    assert projector.events == []


def test_airflow_connection_secret_volume_operator_rejects_deferrable_after_execute_cleanup() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "mssql+pymssql://user:pwd@mssql.example/analytics_reporting"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        deferrable=True,
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError, match="cleanup_policy: retain"):
        operator.execute(context={})

    assert reader.connection_ids == []
    assert projector.events == []


def test_stringify_env_vars_coerces_numeric_values_for_kubernetes() -> None:
    assert stringify_env_vars({"DPONE_TRY_NUMBER": 1, "DPONE_DAG_ID": "orders"}) == {
        "DPONE_TRY_NUMBER": "1",
        "DPONE_DAG_ID": "orders",
    }


def test_unsafe_airflow_connection_env_operator_stringifies_rendered_env_vars() -> None:
    class Reader:
        def read_uri(self, connection_id: str) -> str:
            return "mssql+pymssql://user:pwd@mssql.example/analytics_reporting"

    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        image="registry.example/dpone:dev",
        namespace="airflow-dev",
        unsafe_airflow_connection_ids=("mssql_prod",),
        unsafe_connection_reader=Reader(),
        env_vars={"DPONE_TRY_NUMBER": 1},
    )

    operator.execute(context={})

    assert operator.env_vars["DPONE_TRY_NUMBER"] == "1"
    assert operator.env_vars["AIRFLOW_CONN_MSSQL_PROD"].startswith("mssql+pymssql://")


def test_unsafe_airflow_connection_env_operator_rejects_uri_connection_id_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    reader = Reader()
    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        image="registry.example/dpone:dev",
        namespace="airflow-dev",
        unsafe_airflow_connection_ids=("postgres://etl:secret@pg.internal:5432/dwh",),
        unsafe_connection_reader=reader,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context={})

    assert "logical Airflow Connection id" in str(exc_info.value)
    assert "etl:secret" not in str(exc_info.value)
    assert reader.connection_ids == []


def test_unsafe_airflow_connection_env_operator_patches_full_pod_spec_env() -> None:
    from dpone_airflow_pack.operators import patch_pod_spec_env_vars

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
    }
    patched = patch_pod_spec_env_vars(
        pod,
        {
            "KEEP_ME": "1",
            "AIRFLOW_CONN_MSSQL_PROD": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
        },
    )
    assert patched["spec"]["containers"][0]["env"] == [
        {"name": "KEEP_ME", "value": "1"},
        {
            "name": "AIRFLOW_CONN_MSSQL_PROD",
            "value": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
        },
    ]


def test_patch_pod_spec_env_vars_updates_init_fetch_init_container() -> None:
    from dpone_airflow_pack.operators import patch_pod_spec_env_vars

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "initContainers": [
                {
                    "name": "dpone-runtime-init-fetch",
                    "image": "registry.example/dpone@sha256:" + "a" * 64,
                    "env": [
                        {
                            "name": "AWS_ACCESS_KEY_ID",
                            "value": "{{ conn.s3_dpone_artifacts_reader.login }}",
                        }
                    ],
                }
            ],
            "containers": [
                {
                    "name": "base",
                    "image": "registry.example/dpone@sha256:" + "a" * 64,
                    "env": [
                        {
                            "name": "AWS_ACCESS_KEY_ID",
                            "value": "{{ conn.s3_dpone_artifacts_reader.login }}",
                        }
                    ],
                }
            ],
        },
    }
    patched = patch_pod_spec_env_vars(
        pod,
        {
            "AWS_ACCESS_KEY_ID": "rendered-access-key",
            "AWS_SECRET_ACCESS_KEY": "rendered-secret",
            "DPONE_INIT_FETCH_PLAN_SHA256": "sha256:" + "b" * 64,
        },
    )
    expected = [
        {"name": "AWS_ACCESS_KEY_ID", "value": "rendered-access-key"},
        {"name": "AWS_SECRET_ACCESS_KEY", "value": "rendered-secret"},
        {"name": "DPONE_INIT_FETCH_PLAN_SHA256", "value": "sha256:" + "b" * 64},
    ]
    assert patched["spec"]["containers"][0]["env"] == expected
    assert patched["spec"]["initContainers"][0]["env"] == expected


def test_patch_pod_spec_env_vars_preserves_unrelated_secret_key_ref() -> None:
    from dpone_airflow_pack.operators import patch_pod_spec_env_vars

    registry_env = {
        "name": "AIRFLOW_CONN_ARTIFACT_REGISTRY_READER",
        "valueFrom": {
            "secretKeyRef": {
                "name": "artifact-registry-reader",
                "key": "AIRFLOW_CONN_ARTIFACT_REGISTRY_READER",
                "optional": False,
            }
        },
    }
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "initContainers": [
                {
                    "name": "dpone-runtime-init-fetch",
                    "image": "registry.example/dpone@sha256:" + "a" * 64,
                    "env": [registry_env],
                }
            ],
            "containers": [{"name": "base", "image": "registry.example/dpone@sha256:" + "a" * 64}],
        },
    }

    patched = patch_pod_spec_env_vars(
        pod,
        {"DPONE_INIT_FETCH_PLAN_SHA256": "sha256:" + "b" * 64},
    )

    init_env = {item["name"]: item for item in patched["spec"]["initContainers"][0]["env"]}
    assert init_env["AIRFLOW_CONN_ARTIFACT_REGISTRY_READER"] == registry_env
    assert init_env["DPONE_INIT_FETCH_PLAN_SHA256"] == {
        "name": "DPONE_INIT_FETCH_PLAN_SHA256",
        "value": "sha256:" + "b" * 64,
    }
    base_env = {item["name"]: item for item in patched["spec"]["containers"][0]["env"]}
    assert "AIRFLOW_CONN_ARTIFACT_REGISTRY_READER" not in base_env


def test_unsafe_airflow_connection_env_operator_materializes_full_pod_spec_before_execute() -> None:
    class Reader:
        def read_uri(self, connection_id: str) -> str:
            return {
                "clickhouse_example": "clickhouse://user:pwd@clickhouse.example/default?secure=true",
                "mssql_example": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
            }[connection_id]

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "dpone-marketing"},
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "image": "registry.example/dpone:dev",
                    "command": ["/bin/sh"],
                    "args": ["echo hi"],
                }
            ]
        },
    }
    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="marketing__dpone_runtime",
        name="dpone-marketing",
        namespace="airflow-example",
        full_pod_spec=pod,
        unsafe_airflow_connection_ids=("clickhouse_example", "mssql_example"),
        unsafe_connection_reader=Reader(),
        env_vars={"DPONE_TRY_NUMBER": "1"},
    )

    operator.execute(context={})

    container_env = operator.full_pod_spec["spec"]["containers"][0]["env"]
    env_names = {item["name"] for item in container_env}
    assert "AIRFLOW_CONN_CLICKHOUSE_EXAMPLE" in env_names
    assert "AIRFLOW_CONN_MSSQL_EXAMPLE" in env_names
    assert "DPONE_TRY_NUMBER" in env_names
    built = operator.build_pod_request_obj(context={})
    if isinstance(built, dict):
        built_env = built["spec"]["containers"][0]["env"]
    else:
        built_env = built.spec.containers[0].env or []
        built_env = [{"name": item.name, "value": item.value} for item in built_env]
    assert {item["name"] for item in built_env} >= env_names
