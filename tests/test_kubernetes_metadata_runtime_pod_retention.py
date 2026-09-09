from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Literal, cast

import pytest

from dpone.adapters.kubernetes_airflow_runtime_pod_retention import (
    KubernetesAirflowRuntimePodRetentionAdapter,
)
from dpone.adapters.kubernetes_metadata import KubernetesApiMetadataTransport
from dpone.contracts.airflow_runtime_pod_metadata import AirflowRuntimePodOwnershipContract
from dpone.contracts.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApplyRequest,
    AirflowRuntimePodRetentionError,
)
from dpone.services.airflow_runtime_pod_retention import AirflowRuntimePodRetentionService

_NAMESPACE = "airflow-example"
_SELECTOR = "dpone.dev/managed-by=airflow-provider,dpone.dev/runtime-contract=init-fetch-v2,dpone.dev/workload-id"
_NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)  # noqa: UP017 - mypy baseline targets Python 3.10.
_SECRET_DETAIL = "Bearer secret-token private-endpoint"


class _ConnectTimeout(TimeoutError):
    pass


class _ReadTimeout(TimeoutError):
    pass


class _Evidence:
    durability: Literal["process_ordered"] = "process_ordered"

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish(self, event: dict[str, object]) -> None:
        self.events.append(dict(event))


class _DeleteOptions:
    def __init__(self, *, preconditions: _Preconditions) -> None:
        self.preconditions = preconditions


class _Preconditions:
    def __init__(self, *, uid: str, resource_version: str) -> None:
        self.uid = uid
        self.resource_version = resource_version


def _install_kubernetes_delete_types(monkeypatch: pytest.MonkeyPatch) -> None:
    client_module = ModuleType("kubernetes.client")
    setattr(client_module, "V1DeleteOptions", _DeleteOptions)
    setattr(client_module, "V1Preconditions", _Preconditions)
    kubernetes_module = ModuleType("kubernetes")
    setattr(kubernetes_module, "client", client_module)
    monkeypatch.setitem(sys.modules, "kubernetes", kubernetes_module)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client_module)


def _page(*, phase: str, uid: str, resource_version: str) -> dict[str, object]:
    items: list[dict[str, object]] = []
    if phase == "Failed":
        items.append(
            {
                "kind": "PartialObjectMetadata",
                "metadata": {
                    "name": "runtime-pod",
                    "uid": uid,
                    "resourceVersion": resource_version,
                    "creationTimestamp": "2026-08-01T00:00:00Z",
                    "labels": {
                        "dpone.dev/managed-by": "airflow-provider",
                        "dpone.dev/runtime-contract": "init-fetch-v2",
                        "dpone.dev/workload-id": "orders_load",
                        "dag_id": "DAG__sales__orders__refresh",
                        "task_id": "orders_load",
                        "run_id": "scheduled__2026-08-01",
                    },
                },
            }
        )
    return {"kind": "PartialObjectMetadataList", "metadata": {"continue": ""}, "items": items}


def _phase(kwargs: dict[str, object]) -> str:
    query = dict(cast(list[tuple[str, object]], kwargs["query_params"]))
    return str(query["fieldSelector"]).split("=", 1)[1]


def _adapter(*, api_client: Any, core_api: Any) -> KubernetesAirflowRuntimePodRetentionAdapter:
    transport = KubernetesApiMetadataTransport(
        api_client=api_client,
        core_api=core_api,
        credential_mode="kubeconfig",
    )
    return KubernetesAirflowRuntimePodRetentionAdapter(
        transport=transport,
        credential_mode="kubeconfig",
        credential_context="reviewed-dev",
        label_selector=_SELECTOR,
    )


def _service(
    adapter: KubernetesAirflowRuntimePodRetentionAdapter,
    evidence: _Evidence,
) -> AirflowRuntimePodRetentionService:
    ownership = AirflowRuntimePodOwnershipContract(
        managed_by_key="dpone.dev/managed-by",
        managed_by_value="airflow-provider",
        runtime_contract_key="dpone.dev/runtime-contract",
        runtime_contract_value="init-fetch-v2",
        workload_id_key="dpone.dev/workload-id",
    )
    return AirflowRuntimePodRetentionService(
        inventory=adapter,
        deletion=adapter,
        credentials=adapter,
        evidence=evidence,
        ownership=ownership,
        now=lambda: _NOW,
    )


def _request() -> AirflowRuntimePodRetentionApplyRequest:
    actor = "serviceaccount://airflow-example/dpone-runtime-pod-retention"
    return AirflowRuntimePodRetentionApplyRequest(
        namespace=_NAMESPACE,
        minimum_age_seconds=86_400,
        page_size=500,
        max_delete_count=100,
        actor=actor,
        allowed_actors=(actor,),
        confirm_delete=True,
        kube_auth_mode="kubeconfig",
    )


@pytest.mark.parametrize("timeout_type", [_ConnectTimeout, _ReadTimeout], ids=["connect-timeout", "read-timeout"])
def test_list_timeout_is_redacted_and_cannot_complete_or_delete(timeout_type: type[Exception]) -> None:
    class ApiClient:
        calls = 0

        def call_api(self, _path: str, _method: str, **kwargs: object) -> object:
            self.calls += 1
            assert kwargs["_request_timeout"] == (5, 30)
            raise timeout_type(_SECRET_DETAIL)

    class CoreApi:
        deletes = 0

        def delete_namespaced_pod(self, **_kwargs: object) -> object:
            self.deletes += 1
            return object()

    api = ApiClient()
    core = CoreApi()
    evidence = _Evidence()

    with pytest.raises(AirflowRuntimePodRetentionError) as exc_info:
        _service(_adapter(api_client=api, core_api=core), evidence).apply(_request())

    assert exc_info.value.code == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DEPENDENCY_UNAVAILABLE"
    assert _SECRET_DETAIL not in str(exc_info.value)
    assert api.calls == 1
    assert core.deletes == 0
    assert evidence.events == []


def test_delete_timeout_is_failed_with_complete_redacted_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_kubernetes_delete_types(monkeypatch)

    class ApiClient:
        def call_api(self, _path: str, _method: str, **kwargs: object) -> object:
            assert kwargs["_request_timeout"] == (5, 30)
            return _page(phase=_phase(kwargs), uid="uid-old", resource_version="rv-old")

    class CoreApi:
        calls = 0

        def delete_namespaced_pod(self, **kwargs: object) -> object:
            self.calls += 1
            assert kwargs["_request_timeout"] == (5, 30)
            raise TimeoutError(_SECRET_DETAIL)

    core = CoreApi()
    evidence = _Evidence()

    report = _service(_adapter(api_client=ApiClient(), core_api=core), evidence).apply(_request())

    assert report["status"] == "failed"
    assert report["delete_accepted_pod_names"] == []
    assert report["failed_pod_names"] == ["runtime-pod"]
    assert report["items"][0]["error_code"] == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DELETE_FAILED"
    assert [event["event"] for event in evidence.events] == [
        "operation_started",
        "delete_intent",
        "delete_outcome",
        "operation_completed",
    ]
    assert evidence.events[-2]["outcome"] == "delete_failed"
    assert evidence.events[-1]["outcome"] == "failed"
    assert core.calls == 1
    assert _SECRET_DETAIL not in json.dumps({"report": report, "events": evidence.events})


def test_retry_relists_and_deletes_only_new_pod_preconditions(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_kubernetes_delete_types(monkeypatch)

    class ClusterState:
        generation = 1
        list_calls: list[tuple[str, int]] = []
        delete_preconditions: list[tuple[str, str]] = []

    state = ClusterState()

    class ApiClient:
        def call_api(self, _path: str, _method: str, **kwargs: object) -> object:
            phase = _phase(kwargs)
            state.list_calls.append((phase, state.generation))
            assert kwargs["_request_timeout"] == (5, 30)
            suffix = "old" if state.generation == 1 else "new"
            return _page(phase=phase, uid=f"uid-{suffix}", resource_version=f"rv-{suffix}")

    class CoreApi:
        def delete_namespaced_pod(self, **kwargs: object) -> object:
            assert kwargs["_request_timeout"] == (5, 30)
            body = kwargs["body"]
            assert isinstance(body, _DeleteOptions)
            preconditions = body.preconditions
            state.delete_preconditions.append((preconditions.uid, preconditions.resource_version))
            if len(state.delete_preconditions) == 1:
                state.generation = 2
                raise TimeoutError(_SECRET_DETAIL)
            return object()

    evidence = _Evidence()
    service = _service(_adapter(api_client=ApiClient(), core_api=CoreApi()), evidence)

    first = service.apply(_request())
    second = service.apply(_request())

    assert first["status"] == "failed"
    assert first["delete_accepted_pod_names"] == []
    assert second["status"] == "ok"
    assert second["delete_accepted_pod_names"] == ["runtime-pod"]
    assert state.list_calls == [
        ("Failed", 1),
        ("Succeeded", 1),
        ("Failed", 2),
        ("Succeeded", 2),
    ]
    assert state.delete_preconditions == [("uid-old", "rv-old"), ("uid-new", "rv-new")]
    assert first["items"][0]["precondition_ref"] != second["items"][0]["precondition_ref"]
    assert _SECRET_DETAIL not in json.dumps({"first": first, "second": second, "events": evidence.events})
