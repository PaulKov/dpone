from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
from types import SimpleNamespace
from typing import Any

import pytest
from dpone_airflow_pack.connection_secret_identity import (
    AirflowTaskAttemptIdentity,
    AirflowTaskAttemptIdentityError,
    derive_airflow_connection_secret_attempt,
    require_airflow_task_attempt_identity,
)
from dpone_airflow_pack.connection_secret_lifecycle import (
    ATTEMPT_REF_ANNOTATION,
    LIFECYCLE_VERSION_LABEL,
    MANAGED_BY_LABEL,
    POD_RESOURCE_KIND,
    RESOURCE_KIND_LABEL,
    SECRET_REF_ANNOTATION,
)
from dpone_airflow_pack.operators import (
    AirflowConnectionSecretConflictError,
    AirflowConnectionSecretDependencyError,
    AirflowConnectionSecretProjectionError,
    AirflowConnectionSecretVolumeKubernetesPodOperator,
    KubernetesApiAirflowConnectionSecretProjector,
    PinnedXComSidecarKubernetesPodOperator,
)


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


def _context(
    *,
    dag_id: str = "orders_daily",
    task_id: str = "load_orders",
    run_id: str = "scheduled__2026-07-16T00:00:00+00:00",
    try_number: int = 1,
    map_index: int = -1,
) -> dict[str, object]:
    return {
        "task_instance": SimpleNamespace(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            try_number=try_number,
            map_index=map_index,
        )
    }


def _projection(*, base_name: str = "dpone-airflow-connection-bridge") -> dict[str, object]:
    return {
        "mode": "kubernetes_secret_volume",
        "secret_name": base_name,
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


class _Reader:
    def __init__(self) -> None:
        self.connection_ids: list[str] = []

    def read_uri(self, connection_id: str) -> str:
        self.connection_ids.append(connection_id)
        return "mssql+pymssql://user:password@mssql.internal/DWH"


class _RecordingProjector:
    def __init__(self) -> None:
        self.published: list[tuple[str, Mapping[str, Any]]] = []
        self.deleted: list[tuple[str, str]] = []

    def upsert(self, *, namespace: str, secret: Mapping[str, Any]) -> None:
        self.published.append((namespace, secret))

    def delete(self, *, namespace: str, name: str) -> None:
        self.deleted.append((namespace, name))


def _operator(*, reader: _Reader, projector: _RecordingProjector) -> AirflowConnectionSecretVolumeKubernetesPodOperator:
    return AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="load_orders",
        name="dpone-orders",
        namespace="airflow-example",
        full_pod_spec={
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "dpone-orders"},
            "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
        },
        airflow_connection_projection=_projection(),
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )


def test_attempt_identity_uses_airflow_public_task_instance_key_fields() -> None:
    identity = require_airflow_task_attempt_identity(_context(run_id="manual__one", try_number=3, map_index=7))

    assert identity == AirflowTaskAttemptIdentity(
        dag_id="orders_daily",
        task_id="load_orders",
        run_id="manual__one",
        try_number=3,
        map_index=7,
    )


@pytest.mark.parametrize(
    "context",
    [
        {},
        {"task_instance": SimpleNamespace(dag_id="orders", task_id="load", run_id="run")},
        {
            "ti": SimpleNamespace(
                dag_id="orders",
                task_id="load",
                run_id="run",
                try_number=True,
                map_index=-1,
            )
        },
    ],
)
def test_attempt_identity_rejects_incomplete_or_ambiguous_context(context: object) -> None:
    with pytest.raises(AirflowTaskAttemptIdentityError) as exc_info:
        require_airflow_task_attempt_identity(context)

    assert exc_info.value.code == "DPONE_AIRFLOW_TASK_ATTEMPT_IDENTITY_INVALID"
    assert "orders" not in str(exc_info.value)
    assert "run" not in str(exc_info.value)


def test_attempt_secret_name_is_deterministic_bounded_and_sensitive_to_full_attempt_key() -> None:
    base = "a" * 63
    first_identity = require_airflow_task_attempt_identity(_context())
    first = derive_airflow_connection_secret_attempt(base, first_identity)
    repeated = derive_airflow_connection_secret_attempt(base, first_identity)
    retry = derive_airflow_connection_secret_attempt(
        base,
        require_airflow_task_attempt_identity(_context(try_number=2)),
    )
    mapped = derive_airflow_connection_secret_attempt(
        base,
        require_airflow_task_attempt_identity(_context(map_index=0)),
    )
    other_run = derive_airflow_connection_secret_attempt(
        base,
        require_airflow_task_attempt_identity(_context(run_id="manual__other")),
    )
    other_dag = derive_airflow_connection_secret_attempt(
        base,
        require_airflow_task_attempt_identity(_context(dag_id="other_daily")),
    )
    other_task = derive_airflow_connection_secret_attempt(
        base,
        require_airflow_task_attempt_identity(_context(task_id="check_orders")),
    )

    assert first == repeated
    assert len(first.secret_name) <= 63
    assert first.secret_name.startswith("a" * 20)
    assert first.secret_name.rsplit("-", 1)[1].isalnum()
    assert (
        len(
            {
                first.secret_name,
                retry.secret_name,
                mapped.secret_name,
                other_run.secret_name,
                other_dag.secret_name,
                other_task.secret_name,
            }
        )
        == 6
    )
    assert first.secret_ref.startswith("sha256:")
    assert first.attempt_ref.startswith("sha256:")
    assert "orders_daily" not in first.secret_name
    assert "manual" not in first.secret_name


def test_closed_init_fetch_bridge_preserves_uri_overrides() -> None:
    from dpone_airflow_pack.init_fetch_connection_bridge import (
        require_closed_init_fetch_connection_bridge,
    )

    closed = require_closed_init_fetch_connection_bridge(
        {
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "cleanup_policy": "after_execute",
            "scheme_overrides": {"ClickHouse": "clickhouse"},
            "database_overrides": {"ClickHouse": "DWH_OLAP"},
            "query_overrides": {
                "mssql_example": {"trust_server_certificate": "yes"},
            },
            "connections": [
                {
                    "connection_ref": "mssql_example",
                    "registry_connection_ref": "mssql_example",
                    "connection_id": "mssql_example",
                    "secret_key": "AIRFLOW_CONN_MSSQL_EXAMPLE",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_example",
                    "fields": {"uri": "uri"},
                }
            ],
        }
    )
    assert closed["query_overrides"]["mssql_example"]["trust_server_certificate"] == "yes"
    assert closed["scheme_overrides"]["ClickHouse"] == "clickhouse"
    assert closed["database_overrides"]["ClickHouse"] == "DWH_OLAP"


def test_secret_volume_operator_applies_projection_uri_overrides_when_publishing() -> None:
    """Closed bridge must rewrite ClickHouse-style URIs before Secret publish."""

    class _HttpClickHouseReader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "http://ch-user:ch-secret@clickhouse.internal:8123/default"

    projection = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "cleanup_policy": "after_execute",
        "scheme_overrides": {"ClickHouse": "clickhouse"},
        "database_overrides": {"ClickHouse": "DWH_OLAP"},
        "query_overrides": {"ClickHouse": {"secure": "true"}},
        "connections": [
            {
                "connection_ref": "ClickHouse",
                "registry_connection_ref": "ClickHouse",
                "connection_id": "ClickHouse",
                "secret_key": "AIRFLOW_CONN_CLICKHOUSE",
                "mount_path": "/run/secrets/dpone/airflow-connections/ClickHouse",
                "fields": {"uri": "uri"},
            }
        ],
    }
    reader = _HttpClickHouseReader()
    projector = _RecordingProjector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="load_orders",
        name="dpone-orders",
        namespace="airflow-example",
        full_pod_spec={
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "dpone-orders"},
            "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
        },
        airflow_connection_projection=projection,
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    operator.execute(_context(run_id="manual__clickhouse_overrides"))

    assert reader.connection_ids == ["ClickHouse"]
    published_uri = projector.published[0][1]["stringData"]["AIRFLOW_CONN_CLICKHOUSE"]
    assert published_uri.startswith("clickhouse://")
    assert published_uri.endswith("/DWH_OLAP?secure=true") or (
        "/DWH_OLAP" in published_uri and "secure=true" in published_uri
    )
    assert "http://" not in published_uri
    assert "ch-secret" not in repr(operator.__dict__)


def test_operator_isolates_publish_mount_and_cleanup_for_concurrent_attempts() -> None:
    class OverlapProjector(_RecordingProjector):
        def __init__(self) -> None:
            super().__init__()
            self._published_barrier = Barrier(2)
            self._events_lock = Lock()

        def upsert(self, *, namespace: str, secret: Mapping[str, Any]) -> None:
            with self._events_lock:
                self.published.append((namespace, secret))
            self._published_barrier.wait(timeout=5)

        def delete(self, *, namespace: str, name: str) -> None:
            with self._events_lock:
                self.deleted.append((namespace, name))

    first_reader = _Reader()
    second_reader = _Reader()
    projector = OverlapProjector()
    first = _operator(reader=first_reader, projector=projector)
    second = _operator(reader=second_reader, projector=projector)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first.execute, _context(run_id="manual__first"))
        second_future = executor.submit(second.execute, _context(run_id="manual__second"))
        first_future.result(timeout=5)
        second_future.result(timeout=5)

    published_names = [event[1]["metadata"]["name"] for event in projector.published]
    deleted_names = [event[1] for event in projector.deleted]
    first_name = first.full_pod_spec["spec"]["volumes"][0]["secret"]["secretName"]
    second_name = second.full_pod_spec["spec"]["volumes"][0]["secret"]["secretName"]
    assert set(published_names) == {first_name, second_name}
    assert set(deleted_names) == {first_name, second_name}
    assert first_reader.connection_ids == ["mssql_prod"]
    assert second_reader.connection_ids == ["mssql_prod"]
    assert projector.published[0][1]["immutable"] is True
    assert first.airflow_connection_projected_secret_ref is not None
    assert second.airflow_connection_projected_secret_ref is not None
    assert "stringData" not in json.dumps(first.airflow_connection_projected_secret)
    assert "password" not in repr(first.__dict__)
    assert "password" not in repr(second.__dict__)


def test_operator_rejects_missing_attempt_identity_before_connection_or_kubernetes_io() -> None:
    reader = _Reader()
    projector = _RecordingProjector()
    operator = _operator(reader=reader, projector=projector)

    with pytest.raises(AirflowTaskAttemptIdentityError):
        operator.execute({})

    assert reader.connection_ids == []
    assert projector.published == []
    assert projector.deleted == []


def test_operator_cleans_exact_attempt_secret_when_pod_execution_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _Reader()
    projector = _RecordingProjector()
    operator = _operator(reader=reader, projector=projector)

    def fail_execute(_: object, context: object) -> object:
        raise RuntimeError("pod failed")

    monkeypatch.setattr(PinnedXComSidecarKubernetesPodOperator, "execute", fail_execute)

    with pytest.raises(RuntimeError, match="pod failed"):
        operator.execute(_context())

    published_name = projector.published[0][1]["metadata"]["name"]
    assert projector.deleted == [("airflow-example", published_name)]


def test_reused_operator_replaces_the_stable_volume_slot_for_each_attempt() -> None:
    reader = _Reader()
    projector = _RecordingProjector()
    operator = _operator(reader=reader, projector=projector)

    operator.execute(_context(run_id="manual__first"))
    first_secret_name = operator.full_pod_spec["spec"]["volumes"][0]["secret"]["secretName"]
    operator.execute(_context(run_id="manual__second"))

    volumes = operator.full_pod_spec["spec"]["volumes"]
    mounts = operator.full_pod_spec["spec"]["containers"][0]["volumeMounts"]
    assert len(volumes) == 1
    assert len(mounts) == 1
    assert volumes[0]["name"] == "dpone-airflow-connection-bridge"
    assert mounts[0]["name"] == "dpone-airflow-connection-bridge"
    assert volumes[0]["secret"]["secretName"] != first_secret_name
    assert projector.deleted == [
        ("airflow-example", first_secret_name),
        ("airflow-example", volumes[0]["secret"]["secretName"]),
    ]


def test_reused_operator_replaces_object_pod_fallback_volume_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "kubernetes.client", None)
    monkeypatch.setitem(sys.modules, "kubernetes.client.models", None)
    pod = SimpleNamespace(
        spec=SimpleNamespace(
            volumes=[],
            containers=[SimpleNamespace(volume_mounts=[])],
        )
    )
    reader = _Reader()
    projector = _RecordingProjector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="load_orders",
        name="dpone-orders",
        namespace="airflow-example",
        full_pod_spec=pod,
        airflow_connection_projection=_projection(),
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    operator.execute(_context(run_id="manual__first"))
    operator.execute(_context(run_id="manual__second"))

    assert len(pod.spec.volumes) == 1
    assert len(pod.spec.containers[0].volume_mounts) == 1
    assert pod.spec.volumes[0]["name"] == "dpone-airflow-connection-bridge"
    assert pod.spec.containers[0].volume_mounts[0]["name"] == "dpone-airflow-connection-bridge"
    assert pod.spec.volumes[0]["secret"]["secretName"] == projector.published[1][1]["metadata"]["name"]
    assert pod.metadata.labels == {
        MANAGED_BY_LABEL: "dpone",
        RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
        LIFECYCLE_VERSION_LABEL: "v1",
    }
    assert pod.metadata.annotations == {
        SECRET_REF_ANNOTATION: operator.airflow_connection_projected_secret_ref["secret_ref"],
        ATTEMPT_REF_ANNOTATION: operator.airflow_connection_projected_secret_ref["attempt_ref"],
    }


class _ApiFailure(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class _FakeCoreV1Api:
    def __init__(self, *, create_status: int | None = None, delete_status: int | None = None) -> None:
        self.create_status = create_status
        self.delete_status = delete_status
        self.created: list[tuple[str, Mapping[str, object]]] = []
        self.deleted: list[tuple[str, str]] = []
        self.replace_calls = 0

    def create_namespaced_secret(self, *, namespace: str, body: Mapping[str, object]) -> object:
        self.created.append((namespace, body))
        if self.create_status is not None:
            raise _ApiFailure(self.create_status, "postgres://admin:raw-secret@internal")
        return object()

    def replace_namespaced_secret(self, **_: object) -> object:
        self.replace_calls += 1
        return object()

    def delete_namespaced_secret(self, *, namespace: str, name: str) -> object:
        self.deleted.append((namespace, name))
        if self.delete_status is not None:
            raise _ApiFailure(self.delete_status, "token=raw-secret namespace=airflow-example")
        return object()


class _InjectedKubernetesProjector(KubernetesApiAirflowConnectionSecretProjector):
    def __init__(self, api: _FakeCoreV1Api) -> None:
        self.api = api

    def _core_v1_api(self) -> _FakeCoreV1Api:
        return self.api


class _UnavailableKubernetesProjector(KubernetesApiAirflowConnectionSecretProjector):
    def _core_v1_api(self) -> _FakeCoreV1Api:
        raise RuntimeError("token=raw-secret kubeconfig=/sensitive/path")


def _secret(name: str = "dpone-bridge-a1b2c3d4e5f60708") -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name},
        "type": "Opaque",
        "immutable": True,
        "stringData": {"AIRFLOW_CONN_MSSQL_PROD": "mssql://user:raw-secret@internal"},
    }


def test_kubernetes_projector_fails_closed_on_conflict_without_replace() -> None:
    api = _FakeCoreV1Api(create_status=409)
    projector = _InjectedKubernetesProjector(api)

    with pytest.raises(AirflowConnectionSecretConflictError) as exc_info:
        projector.upsert(namespace="airflow-example", secret=_secret())

    assert exc_info.value.code == "DPONE_AIRFLOW_CONNECTION_SECRET_CONFLICT"
    assert exc_info.value.operation == "create"
    assert exc_info.value.status == 409
    assert exc_info.value.secret_ref.startswith("sha256:")
    assert api.replace_calls == 0
    assert "raw-secret" not in str(exc_info.value)
    assert "airflow-example" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_kubernetes_projector_redacts_unavailable_client_dependency() -> None:
    with pytest.raises(AirflowConnectionSecretDependencyError) as exc_info:
        _UnavailableKubernetesProjector().upsert(namespace="airflow-example", secret=_secret())

    assert exc_info.value.code == "DPONE_AIRFLOW_CONNECTION_SECRET_DEPENDENCY_UNAVAILABLE"
    assert exc_info.value.operation == "client_init"
    assert exc_info.value.status is None
    assert "raw-secret" not in str(exc_info.value)
    assert "/sensitive/path" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.parametrize(
    ("operation", "status", "expected_code"),
    [
        ("create", 500, "DPONE_AIRFLOW_CONNECTION_SECRET_PUBLISH_FAILED"),
        ("delete", 403, "DPONE_AIRFLOW_CONNECTION_SECRET_CLEANUP_FAILED"),
    ],
)
def test_kubernetes_projector_redacts_vendor_failures(
    operation: str,
    status: int,
    expected_code: str,
) -> None:
    api = _FakeCoreV1Api(
        create_status=status if operation == "create" else None,
        delete_status=status if operation == "delete" else None,
    )
    projector = _InjectedKubernetesProjector(api)

    with pytest.raises(AirflowConnectionSecretProjectionError) as exc_info:
        if operation == "create":
            projector.upsert(namespace="airflow-example", secret=_secret())
        else:
            projector.delete(namespace="airflow-example", name="dpone-bridge-a1b2c3d4e5f60708")

    assert exc_info.value.code == expected_code
    assert exc_info.value.operation == operation
    assert exc_info.value.status == status
    assert "raw-secret" not in str(exc_info.value)
    assert "airflow-example" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_kubernetes_projector_treats_missing_cleanup_secret_as_idempotent_success() -> None:
    api = _FakeCoreV1Api(delete_status=404)

    _InjectedKubernetesProjector(api).delete(
        namespace="airflow-example",
        name="dpone-bridge-a1b2c3d4e5f60708",
    )

    assert api.deleted == [("airflow-example", "dpone-bridge-a1b2c3d4e5f60708")]


def test_build_core_v1_api_loads_incluster_config_before_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task pods must not talk to localhost:80 with an unconfigured client."""

    from types import ModuleType

    from dpone_airflow_pack.kubernetes_runtime_client import build_core_v1_api

    calls: list[str] = []
    fake_api = object()

    class _ConfigException(Exception):
        pass

    config_mod = ModuleType("kubernetes.config")
    config_mod.ConfigException = _ConfigException  # type: ignore[attr-defined]

    def _load_incluster() -> None:
        calls.append("incluster")

    def _load_kube(*, context: str | None = None) -> None:
        calls.append(f"kubeconfig:{context}")

    config_mod.load_incluster_config = _load_incluster  # type: ignore[attr-defined]
    config_mod.load_kube_config = _load_kube  # type: ignore[attr-defined]

    client_mod = ModuleType("kubernetes.client")

    def _core_v1() -> object:
        calls.append("CoreV1Api")
        return fake_api

    client_mod.CoreV1Api = _core_v1  # type: ignore[attr-defined]

    root = ModuleType("kubernetes")
    root.config = config_mod  # type: ignore[attr-defined]
    root.client = client_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "kubernetes", root)
    monkeypatch.setitem(sys.modules, "kubernetes.config", config_mod)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client_mod)
    monkeypatch.setitem(
        sys.modules,
        "kubernetes.config.config_exception",
        SimpleNamespace(ConfigException=_ConfigException),
    )

    assert build_core_v1_api() is fake_api
    assert calls == ["incluster", "CoreV1Api"]


def test_build_core_v1_api_falls_back_to_kubeconfig(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import ModuleType

    from dpone_airflow_pack.kubernetes_runtime_client import build_core_v1_api

    calls: list[str] = []
    fake_api = object()

    class _ConfigException(Exception):
        pass

    config_mod = ModuleType("kubernetes.config")
    config_mod.ConfigException = _ConfigException  # type: ignore[attr-defined]

    def _load_incluster() -> None:
        calls.append("incluster")
        raise _ConfigException("not in cluster")

    def _load_kube(*, context: str | None = None) -> None:
        calls.append(f"kubeconfig:{context}")

    config_mod.load_incluster_config = _load_incluster  # type: ignore[attr-defined]
    config_mod.load_kube_config = _load_kube  # type: ignore[attr-defined]

    client_mod = ModuleType("kubernetes.client")
    client_mod.CoreV1Api = lambda: fake_api  # type: ignore[attr-defined,misc]

    root = ModuleType("kubernetes")
    root.config = config_mod  # type: ignore[attr-defined]
    root.client = client_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "kubernetes", root)
    monkeypatch.setitem(sys.modules, "kubernetes.config", config_mod)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client_mod)
    monkeypatch.setitem(
        sys.modules,
        "kubernetes.config.config_exception",
        SimpleNamespace(ConfigException=_ConfigException),
    )

    assert build_core_v1_api() is fake_api
    assert calls == ["incluster", "kubeconfig:None"]
