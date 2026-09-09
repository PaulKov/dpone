"""Contract tests for the Airflow-native launch-pin backend."""

from __future__ import annotations

import types
from collections.abc import Mapping
from copy import deepcopy
from inspect import signature

import pytest
from dpone_airflow_pack.launch_pin import ensure_launch_envelope_on_pod, resolve_launch_pin
from dpone_airflow_pack.launch_pin_codes import PIN_INVALID, PIN_RECOVERY_REQUIRED, PIN_UNAVAILABLE
from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref
from dpone_airflow_pack.launch_pin_envelope import AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION
from dpone_airflow_pack.launch_pin_locator import (
    AIRFLOW_TASK_STATE_BACKEND,
    KUBERNETES_CONFIGMAP_BACKEND,
    LaunchPinStoreLocator,
    launch_pin_store_locator_to_mapping,
    materialize_launch_pin_store_locator,
)
from dpone_airflow_pack.launch_pin_pod import InMemoryLaunchPinPodReader, set_launch_pin_pod_reader
from dpone_airflow_pack.launch_pin_resolve import AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY
from dpone_airflow_pack.launch_pin_store import set_launch_pin_store
from dpone_airflow_pack.launch_pin_task_state import (
    commit_task_state_launch_pin_for_selected_pod,
    configure_durable_kpo_kwargs,
)
from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator


class _FailIfUsedStore:
    def get(self, **_: object) -> object:
        raise AssertionError("ConfigMap/injected store must not be used")

    def create_once(self, _: Mapping[str, object]) -> object:
        raise AssertionError("ConfigMap/injected store must not be used")


class _TaskState:
    def __init__(self, value: object) -> None:
        self.value = value
        self.reads: list[str] = []

    def get(self, key: str) -> object:
        self.reads.append(key)
        return self.value

    def set(self, key: str, value: object) -> None:
        self.value = value


class _TaskInstance:
    dag_id = "DAG__domain__process__sync"
    run_id = "manual__2026-08-15T00:00:00+00:00"
    task_id = "orders__dpone_runtime"
    map_index = -1
    try_number = 1

    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def xcom_push(self, *, key: str, value: object) -> None:
        self.values[key] = value

    def xcom_pull(self, *, task_ids: str | None = None, key: str = "return_value") -> object:
        del task_ids
        return self.values.get(key)


@pytest.fixture(autouse=True)
def _isolated_authorities() -> None:
    reader = InMemoryLaunchPinPodReader()
    set_launch_pin_pod_reader(reader)
    set_launch_pin_store(_FailIfUsedStore())  # type: ignore[arg-type]
    yield reader
    set_launch_pin_store(None)
    set_launch_pin_pod_reader(None)


def _run_identity() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "dag_spec": {"id": "orders", "sha256": "sha256:" + "c" * 64},
        "workload_pack": {"id": "orders", "sha256": "sha256:" + "d" * 64},
        "runtime_image_digest": "sha256:" + "e" * 64,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:abcdef0",
            "versioned": True,
            "version": "abcdef0",
            "snapshot_ref": None,
        },
    }


def _deployment_identity() -> dict[str, str]:
    return {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    }


def _pod() -> dict[str, object]:
    pod: dict[str, object] = {
        "metadata": {
            "name": "orders-runtime-abc123",
            "namespace": "airflow",
            "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "annotations": {},
        },
        "spec": {"containers": [{"name": "base", "env": []}]},
    }
    ensure_launch_envelope_on_pod(
        pod,
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256="sha256:" + "f" * 64,
    )
    return pod


def _store_mapping() -> dict[str, str | None]:
    return launch_pin_store_locator_to_mapping(
        LaunchPinStoreLocator(
            backend=AIRFLOW_TASK_STATE_BACKEND,
            kubernetes_conn_id=None,
            namespace="airflow",
        )
    )


def test_backend_selection_is_explicit_frozen_and_backward_compatible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DPONE_LAUNCH_PIN_STORE_BACKEND", AIRFLOW_TASK_STATE_BACKEND)

    selected = materialize_launch_pin_store_locator({})
    explicit = materialize_launch_pin_store_locator({"launch_pin_store": {"backend": KUBERNETES_CONFIGMAP_BACKEND}})

    assert selected["backend"] == AIRFLOW_TASK_STATE_BACKEND
    assert explicit["backend"] == KUBERNETES_CONFIGMAP_BACKEND


def test_unknown_backend_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_LAUNCH_PIN_STORE_BACKEND", "automatic_fallback")

    with pytest.raises(RuntimeError, match=PIN_INVALID):
        materialize_launch_pin_store_locator({})


@pytest.mark.parametrize("backend", [None, ""])
def test_explicit_null_or_empty_backend_is_rejected(backend: object) -> None:
    with pytest.raises(RuntimeError, match=PIN_INVALID):
        materialize_launch_pin_store_locator({"launch_pin_store": {"backend": backend}})


def test_durable_capability_is_required_at_construction() -> None:
    class LegacyKubernetesPodOperator:
        def __init__(self, **_: object) -> None:
            pass

    with pytest.raises(RuntimeError, match=PIN_UNAVAILABLE):
        configure_durable_kpo_kwargs(
            kpo_class=LegacyKubernetesPodOperator,
            kwargs={},
        )


def test_installed_provider_capability_is_enforced_without_version_guessing() -> None:
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]

    if "durable" in signature(base_operator.__init__).parameters:
        assert configure_durable_kpo_kwargs(kpo_class=base_operator, kwargs={})["durable"] is True
        return

    with pytest.raises(RuntimeError, match=PIN_UNAVAILABLE):
        configure_durable_kpo_kwargs(kpo_class=base_operator, kwargs={})


def test_task_state_commit_binds_kpo_identity_and_never_uses_configmap() -> None:
    ti = _TaskInstance()
    task_state = _TaskState({"name": "orders-runtime-abc123", "namespace": "airflow"})
    operator = types.SimpleNamespace(durable=True)

    retained = commit_task_state_launch_pin_for_selected_pod(
        context={"ti": ti, "task_state_store": task_state},
        pod=_pod(),
        operator=operator,
        launch_pin_store=_store_mapping(),
    )

    assert task_state.reads == ["pod_identifier"]
    assert retained["store_backend"] == AIRFLOW_TASK_STATE_BACKEND
    assert str(retained["pointer_resource_version"]).startswith("airflow-task-state:sha256:")
    locator = ti.values[AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY]
    assert isinstance(locator, Mapping)
    assert locator["store_backend"] == AIRFLOW_TASK_STATE_BACKEND
    assert locator["pod_uid"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


@pytest.mark.parametrize(
    "task_state, expected_code",
    [
        (None, PIN_UNAVAILABLE),
        (_TaskState(None), PIN_RECOVERY_REQUIRED),
        (_TaskState({}), PIN_RECOVERY_REQUIRED),
        (_TaskState({"name": 42, "namespace": "airflow"}), PIN_RECOVERY_REQUIRED),
        (_TaskState({"name": "orders-runtime-abc123", "namespace": ""}), PIN_RECOVERY_REQUIRED),
        (
            _TaskState({"name": "another-pod", "namespace": "airflow"}),
            PIN_RECOVERY_REQUIRED,
        ),
    ],
)
def test_task_state_commit_fails_closed_on_missing_or_mismatched_identity(
    task_state: object,
    expected_code: str,
) -> None:
    context: dict[str, object] = {"ti": _TaskInstance()}
    if task_state is not None:
        context["task_state_store"] = task_state

    with pytest.raises(RuntimeError, match=expected_code):
        commit_task_state_launch_pin_for_selected_pod(
            context=context,
            pod=_pod(),
            operator=types.SimpleNamespace(durable=True),
            launch_pin_store=_store_mapping(),
        )


def test_gate_rebuilds_task_state_pointer_from_live_pod() -> None:
    ti = _TaskInstance()
    pod = _pod()
    retained = commit_task_state_launch_pin_for_selected_pod(
        context={
            "ti": ti,
            "task_state_store": _TaskState({"name": "orders-runtime-abc123", "namespace": "airflow"}),
        },
        pod=pod,
        operator=types.SimpleNamespace(durable=True),
        launch_pin_store=_store_mapping(),
    )
    summary = {
        "kind": "gitops.airflow_xcom_summary",
        "schema_version": "1",
        "producer": "test",
        "status": "passed",
        "run_spec_path": "run-spec.json",
        "runtime_evidence_path": "runtime-evidence.json",
        "run_identity": _run_identity(),
        "deployment_identity": _deployment_identity(),
        "launch_pin_ref": build_launch_pin_ref(retained),
    }
    gate_ti = _TaskInstance()
    gate_ti.values = dict(ti.values)

    resolution = resolve_launch_pin(
        ti=gate_ti,
        upstream_task_id="orders__dpone_runtime",
        required=True,
        launch_pin_store=_store_mapping(),
        runtime_summary=summary,
    )

    assert resolution.status == "PRESENT"
    assert resolution.pin is not None
    assert resolution.pin["store_backend"] == AIRFLOW_TASK_STATE_BACKEND
    assert resolution.pin["pod_uid"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def test_gate_rejects_recreated_pod_occurrence_by_uid() -> None:
    ti = _TaskInstance()
    pod = _pod()
    retained = commit_task_state_launch_pin_for_selected_pod(
        context={
            "ti": ti,
            "task_state_store": _TaskState({"name": "orders-runtime-abc123", "namespace": "airflow"}),
        },
        pod=pod,
        operator=types.SimpleNamespace(durable=True),
        launch_pin_store=_store_mapping(),
    )
    summary = {
        "launch_pin_ref": build_launch_pin_ref(retained),
    }
    metadata = pod["metadata"]
    assert isinstance(metadata, dict)
    metadata["uid"] = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

    resolution = resolve_launch_pin(
        ti=ti,
        upstream_task_id="orders__dpone_runtime",
        required=True,
        launch_pin_store=_store_mapping(),
        runtime_summary=summary,
    )

    assert resolution.status == "UNAVAILABLE"
    assert "uid mismatch" in resolution.detail


def test_gate_rejects_mutated_live_pod_envelope() -> None:
    ti = _TaskInstance()
    pod = _pod()
    retained = commit_task_state_launch_pin_for_selected_pod(
        context={
            "ti": ti,
            "task_state_store": _TaskState({"name": "orders-runtime-abc123", "namespace": "airflow"}),
        },
        pod=pod,
        operator=types.SimpleNamespace(durable=True),
        launch_pin_store=_store_mapping(),
    )
    summary = {
        "launch_pin_ref": build_launch_pin_ref(retained),
    }
    metadata = pod["metadata"]
    assert isinstance(metadata, dict)
    annotations = metadata["annotations"]
    assert isinstance(annotations, dict)
    annotations[AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION] = "{}"

    resolution = resolve_launch_pin(
        ti=ti,
        upstream_task_id="orders__dpone_runtime",
        required=True,
        launch_pin_store=_store_mapping(),
        runtime_summary=summary,
    )

    assert resolution.status == "INVALID"
    assert PIN_INVALID in resolution.detail


def test_active_pointer_publish_failure_requires_recovery() -> None:
    class FailingTaskInstance(_TaskInstance):
        def xcom_push(self, *, key: str, value: object) -> None:
            del key, value
            raise RuntimeError("metadata service unavailable")

    with pytest.raises(RuntimeError, match=PIN_RECOVERY_REQUIRED):
        commit_task_state_launch_pin_for_selected_pod(
            context={
                "ti": FailingTaskInstance(),
                "task_state_store": _TaskState({"name": "orders-runtime-abc123", "namespace": "airflow"}),
            },
            pod=_pod(),
            operator=types.SimpleNamespace(durable=True),
            launch_pin_store=_store_mapping(),
        )


def test_cleanup_exact_deletes_pod_without_configmap_io() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import (
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
        cleanup_launch_pin_pod,
    )
    from dpone_airflow_pack.launch_pin_cleanup_handle import (
        build_cleanup_handle_from_pin,
    )

    ti = _TaskInstance()
    retained = commit_task_state_launch_pin_for_selected_pod(
        context={
            "ti": ti,
            "task_state_store": _TaskState({"name": "orders-runtime-abc123", "namespace": "airflow"}),
        },
        pod=_pod(),
        operator=types.SimpleNamespace(durable=True),
        launch_pin_store=_store_mapping(),
    )
    ti.values[AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY] = build_cleanup_handle_from_pin(
        retained,
        store_backend=AIRFLOW_TASK_STATE_BACKEND,
        store_namespace="airflow",
    )
    ti.values[AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY] = {"passed": True}

    result = cleanup_launch_pin_pod(
        upstream_task_id="orders__dpone_runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=ti,
    )

    assert result["status"] == "deleted"
    assert result["head_transition"] == {
        "status": "not_applicable",
        "backend": AIRFLOW_TASK_STATE_BACKEND,
    }
    assert result["per_try_delete"] == {
        "status": "not_applicable",
        "backend": AIRFLOW_TASK_STATE_BACKEND,
    }


def test_operator_uses_durable_and_omits_configmap_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    if "durable" not in signature(base_operator.__init__).parameters:
        pytest.skip("task-state backend intentionally requires CNCF provider 10.20+")
    base_pod = _pod()
    monkeypatch.setattr(
        base_operator,
        "build_pod_request_obj",
        lambda _self, context=None: deepcopy(base_pod),
    )
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="orders-runtime",
        namespace="airflow",
        image="dpone-runtime:test",
        do_xcom_push=False,
        full_pod_spec=base_pod,
        pin_deployment_identity_for_separate_outcome_gate=True,
        launch_pin_store=_store_mapping(),
        params={
            "dpone_run_identity": _run_identity(),
            "dpone_deployment_identity": _deployment_identity(),
            "dpone_runtime_evidence_sha256": "sha256:" + "f" * 64,
        },
    )

    request = operator.build_pod_request_obj(context={"ti": _TaskInstance(), "run_id": _TaskInstance.run_id})
    spec = request["spec"]
    assert isinstance(spec, Mapping)
    init_names = {
        str(container.get("name")) for container in spec.get("initContainers", []) if isinstance(container, Mapping)
    }
    assert operator.durable is True
    assert "dpone-launch-pin-barrier" not in init_names


def test_operator_commits_only_after_kpo_persists_selected_pod_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    if "durable" not in signature(base_operator.__init__).parameters:
        pytest.skip("task-state backend intentionally requires CNCF provider 10.20+")
    pod = _pod()

    def select_and_persist(_self: object, pod_request_obj: object, context: dict[str, object]) -> object:
        del pod_request_obj
        task_state = context["task_state_store"]
        assert isinstance(task_state, _TaskState)
        task_state.set(
            "pod_identifier",
            {"name": "orders-runtime-abc123", "namespace": "airflow"},
        )
        return pod

    monkeypatch.setattr(base_operator, "get_or_create_pod", select_and_persist)
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="orders-runtime",
        namespace="airflow",
        image="dpone-runtime:test",
        do_xcom_push=False,
        pin_deployment_identity_for_separate_outcome_gate=True,
        launch_pin_store=_store_mapping(),
        params={
            "dpone_run_identity": _run_identity(),
            "dpone_deployment_identity": _deployment_identity(),
            "dpone_runtime_evidence_sha256": "sha256:" + "f" * 64,
        },
    )
    ti = _TaskInstance()
    task_state = _TaskState(None)

    selected = operator.get_or_create_pod(
        pod,
        {"ti": ti, "task_state_store": task_state},
    )

    assert selected is pod
    assert task_state.reads == ["pod_identifier"]
    assert AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY in ti.values
