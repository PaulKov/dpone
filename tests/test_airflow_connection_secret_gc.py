from __future__ import annotations

import builtins
import json
import sys
import tomllib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest
from dpone_airflow_pack.connection_secret_identity import (
    AirflowTaskAttemptIdentity,
    derive_airflow_connection_secret_attempt,
)
from dpone_airflow_pack.connection_secret_lifecycle import (
    ATTEMPT_REF_ANNOTATION,
    CLEANUP_POLICY_LABEL,
    LIFECYCLE_VERSION_LABEL,
    MANAGED_BY_LABEL,
    POD_RESOURCE_KIND,
    RESOURCE_KIND_LABEL,
    SECRET_LABEL_SELECTOR,
    SECRET_REF_ANNOTATION,
    SECRET_RESOURCE_KIND,
    AirflowConnectionSecretLifecycle,
)
from dpone_airflow_pack.operator_runtime import (
    build_airflow_connection_projected_secret,
    materialize_airflow_connection_secret_volume,
    patch_pod_spec_secret_volume,
)

import dpone.adapters.kubernetes_airflow_connection_secret_gc as gc_adapter_module
from dpone.adapters.kubernetes_airflow_connection_secret_gc import (
    KubernetesAirflowConnectionSecretGcAdapter,
)
from dpone.cli import main as cli_main
from dpone.commands.airflow_connection_secret_gc_rendering import (
    connection_gc_apply_text,
    connection_gc_plan_text,
    connection_secret_gc_apply_text,
    connection_secret_gc_plan_text,
)
from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.ports.airflow_connection_secret_gc import (
    AirflowConnectionSecretGcApiError,
    KubernetesMetadataSnapshot,
)
from dpone.services.airflow_connection_secret_gc import (
    AirflowConnectionSecretGcApplyRequest,
    AirflowConnectionSecretGcError,
    AirflowConnectionSecretGcPlanRequest,
    AirflowConnectionSecretGcService,
)

_NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
_NAMESPACE = "airflow-example"


def _attempt(*, run_id: str = "scheduled__2026-07-16"):
    return derive_airflow_connection_secret_attempt(
        "dpone-airflow-connection-bridge",
        AirflowTaskAttemptIdentity(
            dag_id="orders_daily",
            task_id="load_orders",
            run_id=run_id,
            try_number=1,
            map_index=-1,
        ),
    )


def _lifecycle(*, cleanup_policy: str = "retain") -> AirflowConnectionSecretLifecycle:
    attempt = _attempt()
    return AirflowConnectionSecretLifecycle(
        secret_ref=attempt.secret_ref,
        attempt_ref=attempt.attempt_ref,
        cleanup_policy=cleanup_policy,
    )


def _projection() -> dict[str, object]:
    return {
        "mode": "kubernetes_secret_volume",
        "secret_name": _attempt().secret_name,
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "connections": [
            {
                "connection_id": "mssql_prod",
                "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                "mount_path": "/run/secrets/dpone/airflow-connections/mssql",
                "fields": {"uri": "uri"},
            }
        ],
    }


class _Reader:
    def read_uri(self, connection_id: str) -> str:
        assert connection_id == "mssql_prod"
        return "mssql+pymssql://user:password@mssql.internal/DWH"


def test_lifecycle_metadata_is_digest_only_and_preserves_user_pod_metadata() -> None:
    lifecycle = _lifecycle()
    secret = build_airflow_connection_projected_secret(_projection(), reader=_Reader(), lifecycle=lifecycle)

    assert secret["metadata"] == {
        "name": _attempt().secret_name,
        "labels": {
            MANAGED_BY_LABEL: "dpone",
            RESOURCE_KIND_LABEL: SECRET_RESOURCE_KIND,
            LIFECYCLE_VERSION_LABEL: "v1",
            CLEANUP_POLICY_LABEL: "retain",
        },
        "annotations": {
            SECRET_REF_ANNOTATION: _attempt().secret_ref,
            ATTEMPT_REF_ANNOTATION: _attempt().attempt_ref,
        },
    }
    assert secret["immutable"] is True
    assert "scheduled__" not in json.dumps(secret["metadata"])

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "orders-runtime",
            "labels": {"team": "sales"},
            "annotations": {"runbook": "orders"},
        },
        "spec": {"containers": [{"name": "base", "image": "dpone:test"}]},
    }
    patched = patch_pod_spec_secret_volume(pod, _projection(), lifecycle=lifecycle)

    assert patched["metadata"]["labels"] == {
        "team": "sales",
        MANAGED_BY_LABEL: "dpone",
        RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
        LIFECYCLE_VERSION_LABEL: "v1",
    }
    assert patched["metadata"]["annotations"] == {
        "runbook": "orders",
        SECRET_REF_ANNOTATION: _attempt().secret_ref,
        ATTEMPT_REF_ANNOTATION: _attempt().attempt_ref,
    }
    assert "password" not in json.dumps(patched)


def test_operator_level_metadata_cannot_override_lifecycle_lease() -> None:
    lifecycle = _lifecycle()
    operator = type(
        "Operator",
        (),
        {
            "full_pod_spec": {
                "metadata": {},
                "spec": {"containers": [{"name": "base", "image": "dpone:test"}]},
            },
            "pod_template_dict": None,
            "kwargs": {},
            "labels": {"team": "sales", MANAGED_BY_LABEL: "not-dpone"},
            "annotations": {"runbook": "orders", SECRET_REF_ANNOTATION: "sha256:" + "0" * 64},
        },
    )()

    materialize_airflow_connection_secret_volume(operator, _projection(), lifecycle=lifecycle)

    assert operator.labels == {
        "team": "sales",
        MANAGED_BY_LABEL: "dpone",
        RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
        LIFECYCLE_VERSION_LABEL: "v1",
    }
    assert operator.annotations == {
        "runbook": "orders",
        SECRET_REF_ANNOTATION: lifecycle.secret_ref,
        ATTEMPT_REF_ANNOTATION: lifecycle.attempt_ref,
    }


def _secret(
    *,
    run_id: str,
    age_seconds: int,
    cleanup_policy: str = "retain",
    labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
    uid: str | None = None,
    resource_version: str = "10",
) -> KubernetesMetadataSnapshot:
    attempt = _attempt(run_id=run_id)
    lifecycle = AirflowConnectionSecretLifecycle(
        secret_ref=attempt.secret_ref,
        attempt_ref=attempt.attempt_ref,
        cleanup_policy=cleanup_policy,
    )
    metadata = lifecycle.secret_metadata()
    return KubernetesMetadataSnapshot(
        resource="secrets",
        name=attempt.secret_name,
        uid=uid or f"uid-{run_id}",
        resource_version=resource_version,
        creation_timestamp=_NOW - timedelta(seconds=age_seconds),
        labels=dict(labels if labels is not None else metadata["labels"]),
        annotations=dict(annotations if annotations is not None else metadata["annotations"]),
    )


def _pod(secret: KubernetesMetadataSnapshot) -> KubernetesMetadataSnapshot:
    return KubernetesMetadataSnapshot(
        resource="pods",
        name="pod-name-is-internal-only",
        uid="pod-uid",
        resource_version="20",
        creation_timestamp=_NOW - timedelta(hours=2),
        labels={
            MANAGED_BY_LABEL: "dpone",
            RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
            LIFECYCLE_VERSION_LABEL: "v1",
        },
        annotations={
            SECRET_REF_ANNOTATION: secret.annotations[SECRET_REF_ANNOTATION],
            ATTEMPT_REF_ANNOTATION: secret.annotations[ATTEMPT_REF_ANNOTATION],
        },
    )


class _FakeClient:
    def __init__(
        self,
        *,
        secrets: tuple[KubernetesMetadataSnapshot, ...] = (),
        pods: tuple[KubernetesMetadataSnapshot, ...] = (),
        delete_errors: dict[str, AirflowConnectionSecretGcApiError] | None = None,
    ) -> None:
        self.secrets = secrets
        self.pods = pods
        self.delete_errors = delete_errors or {}
        self.inventory_calls: list[tuple[str, str, int]] = []
        self.delete_calls: list[tuple[str, str, str, str]] = []

    def list_secret_metadata(self, *, namespace: str, page_size: int) -> tuple[KubernetesMetadataSnapshot, ...]:
        self.inventory_calls.append(("secrets", namespace, page_size))
        return self.secrets

    def list_pod_metadata(self, *, namespace: str, page_size: int) -> tuple[KubernetesMetadataSnapshot, ...]:
        self.inventory_calls.append(("pods", namespace, page_size))
        return self.pods

    def delete_secret(
        self,
        *,
        namespace: str,
        name: str,
        uid: str,
        resource_version: str,
    ) -> None:
        self.delete_calls.append((namespace, name, uid, resource_version))
        error = self.delete_errors.get(name)
        if error is not None:
            raise error


def _service(client: _FakeClient) -> AirflowConnectionSecretGcService:
    return AirflowConnectionSecretGcService(inventory=client, deletion=client, now=lambda: _NOW)


def _plan_request(*, minimum_age_seconds: int = 86400) -> AirflowConnectionSecretGcPlanRequest:
    return AirflowConnectionSecretGcPlanRequest(
        namespace=_NAMESPACE,
        minimum_age_seconds=minimum_age_seconds,
        page_size=500,
    )


def _apply_request(*, max_delete_count: int = 100, actor: str = "ci://gc") -> AirflowConnectionSecretGcApplyRequest:
    return AirflowConnectionSecretGcApplyRequest(
        namespace=_NAMESPACE,
        minimum_age_seconds=86400,
        page_size=500,
        max_delete_count=max_delete_count,
        actor=actor,
        allowed_actors=("ci://gc",),
        confirm_delete=True,
    )


def test_plan_protects_active_and_young_secrets_and_classifies_both_orphan_reasons() -> None:
    active = _secret(run_id="active", age_seconds=200000)
    young = _secret(run_id="young", age_seconds=3600)
    retained = _secret(run_id="retained", age_seconds=200000)
    synchronous = _secret(run_id="sync", age_seconds=200000, cleanup_policy="after_execute")
    client = _FakeClient(secrets=(synchronous, retained, young, active), pods=(_pod(active),))

    report = _service(client).plan(_plan_request())

    assert report["schema"] == "dpone.airflow-connection-secret-gc-plan.v1"
    assert report["status"] == "needs_cleanup"
    assert report["inventory"] == {
        "managed_secrets": 4,
        "managed_pods": 1,
        "active_references": 1,
        "orphan_pod_references": 0,
        "quarantined": 0,
    }
    by_ref = {item["secret_ref"]: item for item in report["items"]}
    assert by_ref[active.annotations[SECRET_REF_ANNOTATION]]["reason"] == "active_pod"
    assert by_ref[young.annotations[SECRET_REF_ANNOTATION]]["reason"] == "minimum_age"
    assert by_ref[retained.annotations[SECRET_REF_ANNOTATION]]["reason"] == "retained_expired"
    assert by_ref[synchronous.annotations[SECRET_REF_ANNOTATION]]["reason"] == "synchronous_cleanup_orphaned"
    assert report["delete_candidates"] == sorted(
        [retained.annotations[SECRET_REF_ANNOTATION], synchronous.annotations[SECRET_REF_ANNOTATION]]
    )
    assert client.delete_calls == []


def test_plan_quarantines_bad_secret_but_ambiguous_managed_pod_blocks_all_deletion() -> None:
    malformed = _secret(run_id="bad-secret", age_seconds=200000, annotations={})
    client = _FakeClient(secrets=(malformed,))

    report = _service(client).plan(_plan_request())

    assert report["status"] == "needs_attention"
    assert report["items"][0]["action"] == "quarantine"
    assert report["items"][0]["reason"] == "invalid_metadata"
    assert report["inventory"]["quarantined"] == 1
    assert "bad-secret" not in json.dumps(report)

    bad_pod = KubernetesMetadataSnapshot(
        resource="pods",
        name="must-not-leak",
        uid="pod-uid",
        resource_version="1",
        creation_timestamp=_NOW,
        labels={
            MANAGED_BY_LABEL: "dpone",
            RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
            LIFECYCLE_VERSION_LABEL: "v1",
        },
        annotations={},
    )
    blocked = _FakeClient(secrets=(_secret(run_id="candidate", age_seconds=200000),), pods=(bad_pod,))
    with pytest.raises(AirflowConnectionSecretGcError) as exc_info:
        _service(blocked).plan(_plan_request())

    assert exc_info.value.code == "DPONE_AIRFLOW_SECRET_GC_POD_METADATA_INVALID"
    assert "must-not-leak" not in str(exc_info.value)


@pytest.mark.parametrize(
    "malformed",
    [
        lambda item: replace(item, uid=""),
        lambda item: replace(item, resource_version=""),
        lambda item: replace(item, creation_timestamp=None),
        lambda item: replace(item, name="INVALID_NAME"),
    ],
)
def test_plan_quarantines_incomplete_secret_identity_fields(malformed) -> None:
    secret = malformed(_secret(run_id="malformed-field", age_seconds=200000))
    client = _FakeClient(secrets=(secret,))

    report = _service(client).apply(_apply_request())

    assert report["status"] == "partial"
    assert report["items"][0]["reason"] == "invalid_metadata"
    assert client.delete_calls == []


def test_plan_protects_small_future_skew_and_quarantines_larger_future_timestamp() -> None:
    within_skew = _secret(run_id="within-skew", age_seconds=-300)
    beyond_skew = _secret(run_id="beyond-skew", age_seconds=-301)

    report = _service(_FakeClient(secrets=(within_skew, beyond_skew))).plan(_plan_request())
    by_ref = {item["secret_ref"]: item for item in report["items"]}

    assert by_ref[within_skew.annotations[SECRET_REF_ANNOTATION]]["reason"] == "minimum_age"
    assert by_ref[within_skew.annotations[SECRET_REF_ANNOTATION]]["age_seconds"] == 0
    assert by_ref[beyond_skew.annotations[SECRET_REF_ANNOTATION]]["reason"] == "future_timestamp"
    assert report["status"] == "needs_attention"


def test_unlabelled_legacy_secret_is_never_adopted_or_deleted() -> None:
    legacy = _secret(run_id="legacy", age_seconds=200000, labels={}, annotations={})
    client = _FakeClient(secrets=(legacy,))

    report = _service(client).apply(_apply_request())

    assert report["status"] == "partial"
    assert report["deleted_secret_refs"] == []
    assert report["items"][0]["reason"] == "invalid_metadata"
    assert client.delete_calls == []


def test_duplicate_secret_digest_blocks_inventory() -> None:
    duplicate = _secret(run_id="duplicate", age_seconds=200000)

    with pytest.raises(AirflowConnectionSecretGcError) as exc_info:
        _service(_FakeClient(secrets=(duplicate, duplicate))).plan(_plan_request())

    assert exc_info.value.code == "DPONE_AIRFLOW_SECRET_GC_INVENTORY_INVALID"


def test_orphan_pod_reference_is_counted_without_protecting_another_secret() -> None:
    candidate = _secret(run_id="candidate-with-orphan", age_seconds=200000)
    orphan_target = _secret(run_id="already-absent", age_seconds=200000)

    report = _service(_FakeClient(secrets=(candidate,), pods=(_pod(orphan_target),))).plan(_plan_request())

    assert report["inventory"]["orphan_pod_references"] == 1
    assert report["inventory"]["active_references"] == 0
    assert report["delete_candidates"] == [candidate.annotations[SECRET_REF_ANNOTATION]]


@pytest.mark.parametrize(
    ("minimum_age_seconds", "page_size"),
    [(299, 500), (2592001, 500), (86400, 0), (86400, 1001)],
)
def test_plan_request_rejects_unsafe_bounds(minimum_age_seconds: int, page_size: int) -> None:
    with pytest.raises(AirflowConnectionSecretGcError) as exc_info:
        AirflowConnectionSecretGcPlanRequest(
            namespace=_NAMESPACE,
            minimum_age_seconds=minimum_age_seconds,
            page_size=page_size,
        )

    assert exc_info.value.code == "DPONE_AIRFLOW_SECRET_GC_INPUT_INVALID"


def test_apply_requires_confirmation_and_authorized_actor_before_inventory() -> None:
    client = _FakeClient(secrets=(_secret(run_id="candidate", age_seconds=200000),))
    service = _service(client)

    with pytest.raises(AirflowConnectionSecretGcError) as unconfirmed:
        service.apply(
            AirflowConnectionSecretGcApplyRequest(
                namespace=_NAMESPACE,
                minimum_age_seconds=86400,
                page_size=500,
                max_delete_count=100,
                actor="ci://gc",
                allowed_actors=("ci://gc",),
                confirm_delete=False,
            )
        )
    assert unconfirmed.value.code == "DPONE_AIRFLOW_SECRET_GC_CONFIRMATION_REQUIRED"
    assert client.inventory_calls == []

    with pytest.raises(AirflowConnectionSecretGcError) as unauthorized:
        service.apply(_apply_request(actor="ci://other"))
    assert unauthorized.value.code == "DPONE_AIRFLOW_SECRET_GC_ACTOR_UNAUTHORIZED"
    assert client.inventory_calls == []


def test_apply_deletes_with_uid_and_resource_version_and_handles_absence_and_change() -> None:
    deleted = _secret(run_id="deleted", age_seconds=200000)
    absent = _secret(run_id="absent", age_seconds=200000)
    changed = _secret(run_id="changed", age_seconds=200000)
    client = _FakeClient(
        secrets=(deleted, absent, changed),
        delete_errors={
            absent.name: AirflowConnectionSecretGcApiError(operation="delete", status=404, reason="not_found"),
            changed.name: AirflowConnectionSecretGcApiError(operation="delete", status=409, reason="conflict"),
        },
    )

    report = _service(client).apply(_apply_request())

    assert report["status"] == "partial"
    assert report["deleted_secret_refs"] == [deleted.annotations[SECRET_REF_ANNOTATION]]
    assert report["skipped_secret_refs"] == sorted(
        [absent.annotations[SECRET_REF_ANNOTATION], changed.annotations[SECRET_REF_ANNOTATION]]
    )
    assert report["failed_secret_refs"] == []
    assert sorted(client.delete_calls) == sorted(
        [(_NAMESPACE, item.name, item.uid, item.resource_version) for item in (deleted, absent, changed)]
    )


def test_apply_rebuilds_inventory_and_protects_candidate_that_became_active() -> None:
    candidate = _secret(run_id="became-active", age_seconds=200000)
    client = _FakeClient(secrets=(candidate,))
    service = _service(client)

    initial = service.plan(_plan_request())
    client.pods = (_pod(candidate),)
    applied = service.apply(_apply_request())

    assert initial["delete_candidates"] == [candidate.annotations[SECRET_REF_ANNOTATION]]
    assert applied["status"] == "ok"
    assert applied["deleted_secret_refs"] == []
    assert client.delete_calls == []


def test_apply_stops_after_dependency_failure_and_reports_batch_limit_truthfully() -> None:
    candidates = tuple(_secret(run_id=f"candidate-{index}", age_seconds=200000) for index in range(4))
    ordered = sorted(candidates, key=lambda item: item.annotations[SECRET_REF_ANNOTATION])
    client = _FakeClient(
        secrets=candidates,
        delete_errors={
            ordered[1].name: AirflowConnectionSecretGcApiError(operation="delete", status=500, reason="api_failure")
        },
    )

    failed = _service(client).apply(_apply_request())

    assert failed["status"] == "failed"
    assert len(client.delete_calls) == 2
    assert failed["failed_secret_refs"] == [ordered[1].annotations[SECRET_REF_ANNOTATION]]
    assert any(item["reason"] == "not_attempted_after_failure" for item in failed["items"])
    assert "api_failure" not in json.dumps(failed)

    bounded_client = _FakeClient(secrets=candidates)
    bounded = _service(bounded_client).apply(_apply_request(max_delete_count=2))
    assert bounded["status"] == "partial"
    assert len(bounded_client.delete_calls) == 2
    assert sum(item["reason"] == "batch_limit" for item in bounded["items"]) == 2


def test_apply_omits_non_http_vendor_status_from_schema_backed_report() -> None:
    candidate = _secret(run_id="invalid-status", age_seconds=200000)
    client = _FakeClient(
        secrets=(candidate,),
        delete_errors={
            candidate.name: AirflowConnectionSecretGcApiError(
                operation="delete",
                status=999,
                reason="api_failure",
            )
        },
    )

    report = _service(client).apply(_apply_request())

    assert report["status"] == "failed"
    assert "http_status" not in report["items"][0]


class _FakeMetadataTransport:
    def __init__(self, responses: dict[str, list[dict[str, object]]]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []
        self.deletes: list[dict[str, str]] = []

    def list_metadata_page(
        self,
        *,
        resource: str,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
        accept: str,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "resource": resource,
                "namespace": namespace,
                "label_selector": label_selector,
                "limit": limit,
                "continue_token": continue_token,
                "accept": accept,
            }
        )
        return self.responses[resource].pop(0)

    def delete_secret(
        self,
        *,
        namespace: str,
        name: str,
        uid: str,
        resource_version: str,
    ) -> None:
        self.deletes.append(
            {
                "namespace": namespace,
                "name": name,
                "uid": uid,
                "resource_version": resource_version,
            }
        )


def _metadata_page(items: list[dict[str, object]], *, continue_token: str = "") -> dict[str, object]:
    return {
        "apiVersion": "meta.k8s.io/v1",
        "kind": "PartialObjectMetadataList",
        "metadata": {"resourceVersion": "100", "continue": continue_token},
        "items": items,
    }


def _metadata_item(snapshot: KubernetesMetadataSnapshot) -> dict[str, object]:
    return {
        "apiVersion": "meta.k8s.io/v1",
        "kind": "PartialObjectMetadata",
        "metadata": {
            "name": snapshot.name,
            "uid": snapshot.uid,
            "resourceVersion": snapshot.resource_version,
            "creationTimestamp": snapshot.creation_timestamp.isoformat().replace("+00:00", "Z"),
            "labels": snapshot.labels,
            "annotations": snapshot.annotations,
        },
    }


def test_adapter_requires_metadata_only_pages_and_follows_bounded_continuation() -> None:
    first = _secret(run_id="first", age_seconds=200000)
    second = _secret(run_id="second", age_seconds=200000)
    transport = _FakeMetadataTransport(
        {
            "secrets": [
                _metadata_page([_metadata_item(first)], continue_token="next"),
                _metadata_page([_metadata_item(second)]),
            ],
            "pods": [_metadata_page([])],
        }
    )
    adapter = KubernetesAirflowConnectionSecretGcAdapter(transport=transport)

    assert adapter.list_secret_metadata(namespace=_NAMESPACE, page_size=1) == (first, second)
    assert [call["continue_token"] for call in transport.calls] == [None, "next"]
    assert all("PartialObjectMetadataList" in str(call["accept"]) for call in transport.calls)
    assert [call["label_selector"] for call in transport.calls] == [
        SECRET_LABEL_SELECTOR,
        SECRET_LABEL_SELECTOR,
    ]


@pytest.mark.parametrize(
    "page",
    [
        {"apiVersion": "v1", "kind": "SecretList", "metadata": {}, "items": []},
        _metadata_page([{"kind": "PartialObjectMetadata", "metadata": {}, "data": {"password": "secret"}}]),
        _metadata_page([{"kind": "PartialObjectMetadata", "metadata": {}, "spec": {"env": "secret"}}]),
    ],
)
def test_adapter_rejects_full_or_forbidden_payloads_without_echoing_values(page: dict[str, object]) -> None:
    transport = _FakeMetadataTransport({"secrets": [page], "pods": [_metadata_page([])]})
    adapter = KubernetesAirflowConnectionSecretGcAdapter(transport=transport)

    with pytest.raises(AirflowConnectionSecretGcApiError) as exc_info:
        adapter.list_secret_metadata(namespace=_NAMESPACE, page_size=500)

    assert exc_info.value.reason == "invalid_metadata_response"
    assert "password" not in str(exc_info.value)
    assert "secret" not in str(exc_info.value).lower().replace("airflow_connection_secret", "")


def test_adapter_allows_user_metadata_keys_named_like_forbidden_object_fields() -> None:
    snapshot = _secret(run_id="metadata-label", age_seconds=200000)
    item = _metadata_item(snapshot)
    metadata = item["metadata"]
    assert isinstance(metadata, dict)
    metadata["labels"] = {**snapshot.labels, "status": "owned-by-platform"}
    metadata["annotations"] = {**snapshot.annotations, "data": "classification-only"}
    transport = _FakeMetadataTransport({"secrets": [_metadata_page([item])], "pods": [_metadata_page([])]})

    listed = KubernetesAirflowConnectionSecretGcAdapter(transport=transport).list_secret_metadata(
        namespace=_NAMESPACE,
        page_size=500,
    )

    assert listed[0].labels["status"] == "owned-by-platform"
    assert listed[0].annotations["data"] == "classification-only"


def test_adapter_rejects_repeated_continuation_token() -> None:
    transport = _FakeMetadataTransport(
        {
            "secrets": [
                _metadata_page([], continue_token="same"),
                _metadata_page([], continue_token="same"),
            ],
            "pods": [_metadata_page([])],
        }
    )

    with pytest.raises(AirflowConnectionSecretGcApiError) as exc_info:
        KubernetesAirflowConnectionSecretGcAdapter(transport=transport).list_secret_metadata(
            namespace=_NAMESPACE,
            page_size=500,
        )

    assert exc_info.value.reason == "invalid_metadata_response"


def test_kubernetes_transport_deletes_with_uid_and_resource_version_preconditions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Preconditions:
        def __init__(self, *, uid: str, resource_version: str) -> None:
            self.uid = uid
            self.resource_version = resource_version

    class DeleteOptions:
        def __init__(self, *, preconditions: Preconditions) -> None:
            self.preconditions = preconditions

    client_module = ModuleType("kubernetes.client")
    client_module.V1DeleteOptions = DeleteOptions
    client_module.V1Preconditions = Preconditions
    kubernetes_module = ModuleType("kubernetes")
    kubernetes_module.client = client_module
    monkeypatch.setitem(sys.modules, "kubernetes", kubernetes_module)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client_module)

    class ApiClient:
        def call_api(self, resource_path: str, method: str, **kwargs: object) -> object:
            del resource_path, method, kwargs
            return {}

    class CoreApi:
        def __init__(self) -> None:
            self.body: DeleteOptions | None = None

        def delete_namespaced_secret(
            self,
            *,
            name: str,
            namespace: str,
            body: object,
            **kwargs: object,
        ) -> object:
            assert name == "attempt-secret"
            assert namespace == _NAMESPACE
            assert isinstance(body, DeleteOptions)
            assert kwargs == {"_request_timeout": (5, 30)}
            self.body = body
            return object()

    core_api = CoreApi()
    transport = gc_adapter_module._KubernetesApiTransport(api_client=ApiClient(), core_api=core_api)

    transport.delete_secret(
        namespace=_NAMESPACE,
        name="attempt-secret",
        uid="uid-123",
        resource_version="rv-456",
    )

    assert core_api.body is not None
    assert core_api.body.preconditions.uid == "uid-123"
    assert core_api.body.preconditions.resource_version == "rv-456"


def test_kubernetes_platform_extra_declares_supported_sdk_range() -> None:
    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8"))
    extras = project["project"]["optional-dependencies"]

    assert extras["kubernetes"] == ["kubernetes>=32.0.1,!=36.0.0,<37"]
    assert extras["kubernetes"][0] in extras["full"]


def test_supported_real_kubernetes_sdk_models_preserve_delete_preconditions() -> None:
    client = pytest.importorskip("kubernetes.client")

    preconditions = client.V1Preconditions(uid="uid-real", resource_version="rv-real")
    body = client.V1DeleteOptions(preconditions=preconditions)

    assert body.preconditions.uid == "uid-real"
    assert body.preconditions.resource_version == "rv-real"


def test_adapter_reports_missing_kubernetes_sdk_as_actionable_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def blocked_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "kubernetes" or name.startswith("kubernetes."):
            raise ModuleNotFoundError("No module named 'kubernetes'", name="kubernetes")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    with pytest.raises(AirflowConnectionSecretGcApiError) as exc_info:
        gc_adapter_module.build_kubernetes_airflow_connection_secret_gc_adapter()

    assert exc_info.value.reason == "sdk_unavailable"
    assert exc_info.value.status is None


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (401, "access_denied"),
        (403, "access_denied"),
        (406, "metadata_only_unsupported"),
        (410, "inventory_expired"),
    ],
)
def test_adapter_redacts_vendor_errors_and_preserves_safe_status_classification(
    status: int,
    reason: str,
) -> None:
    class VendorFailure(RuntimeError):
        def __init__(self) -> None:
            super().__init__("token=raw-secret https://kubernetes.internal/client.pem")
            self.status = status

    class FailingTransport(_FakeMetadataTransport):
        def list_metadata_page(self, **kwargs: object) -> dict[str, object]:
            del kwargs
            raise VendorFailure

    adapter = KubernetesAirflowConnectionSecretGcAdapter(transport=FailingTransport({"secrets": [], "pods": []}))

    with pytest.raises(AirflowConnectionSecretGcApiError) as exc_info:
        adapter.list_secret_metadata(namespace=_NAMESPACE, page_size=500)

    assert exc_info.value.status == status
    assert exc_info.value.reason == reason
    assert "raw-secret" not in str(exc_info.value)
    assert "kubernetes.internal" not in str(exc_info.value)


def test_large_inventory_is_deterministic_and_complete() -> None:
    secrets = tuple(_secret(run_id=f"bulk-{index:05d}", age_seconds=200000) for index in range(10_000))

    first = _service(_FakeClient(secrets=secrets)).plan(_plan_request())
    second = _service(_FakeClient(secrets=tuple(reversed(secrets)))).plan(_plan_request())

    assert first["inventory"]["managed_secrets"] == 10_000
    assert first["delete_candidates"] == second["delete_candidates"]
    assert first["delete_candidates"] == sorted(first["delete_candidates"])


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc_info:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc_info.value.code or 0), captured.out, captured.err


def test_cli_plan_and_apply_are_actionable_and_never_print_physical_names(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate = _secret(run_id="cli-candidate", age_seconds=200000)
    client = _FakeClient(secrets=(candidate,))
    build_calls: list[tuple[str, str | None]] = []

    def _build(*, auth_mode: str, kube_context: str | None):
        build_calls.append((auth_mode, kube_context))
        return client

    import dpone.readiness.airflow_self_service_secret_gc as command

    monkeypatch.setattr(command, "build_kubernetes_airflow_connection_secret_gc_adapter", _build)

    code, stdout, stderr = _run_cli(
        ["airflow", "connection-secret-gc-plan", "--namespace", _NAMESPACE],
        capsys,
    )
    assert code == 0, stderr
    assert "dpone airflow connection Secret GC: NEEDS_CLEANUP" in stdout
    assert "- item 1: delete" in stdout
    assert "rerun with --format json for protected digest references" in stdout
    assert candidate.annotations[SECRET_REF_ANNOTATION] not in stdout
    assert candidate.name not in stdout
    assert "connection-secret-gc-apply" in stdout

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "connection-secret-gc-apply",
            "--namespace",
            _NAMESPACE,
            "--actor",
            "ci://gc",
            "--allowed-actor",
            "ci://gc",
            "--confirm-delete",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["schema"] == "dpone.airflow-connection-secret-gc-apply.v1"
    assert payload["deleted_secret_refs"] == [candidate.annotations[SECRET_REF_ANNOTATION]]
    assert candidate.name not in stdout
    assert build_calls == [("auto", None), ("auto", None)]


def test_connection_gc_renderer_compatibility_aliases_are_exact() -> None:
    payload = {
        "status": "ok",
        "namespace": _NAMESPACE,
        "inventory": {"managed_secrets": 0},
        "items": [],
    }

    assert connection_secret_gc_plan_text(payload) == connection_gc_plan_text(payload)
    assert connection_secret_gc_apply_text(payload) == connection_gc_apply_text(payload)


def test_cli_rejects_unconfirmed_apply_before_building_kubernetes_client(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import dpone.readiness.airflow_self_service_secret_gc as command

    monkeypatch.setattr(
        command,
        "build_kubernetes_airflow_connection_secret_gc_adapter",
        lambda **_: pytest.fail("Kubernetes client must not be built before confirmation"),
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "connection-secret-gc-apply",
            "--namespace",
            _NAMESPACE,
            "--actor",
            "ci://gc",
            "--allowed-actor",
            "ci://gc",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 4, stderr
    error = json.loads(stdout)["errors"][0]
    assert error["code"] == "DPONE_AIRFLOW_SECRET_GC_CONFIRMATION_REQUIRED"
    assert error["fixes"][0]["command"].endswith(f"--namespace {_NAMESPACE}")


def test_cli_does_not_echo_invalid_namespace_in_fix_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "connection-secret-gc-plan",
            "--namespace",
            "INVALID_NAMESPACE",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2, stderr
    error = json.loads(stdout)["errors"][0]
    assert error["code"] == "DPONE_AIRFLOW_SECRET_GC_INPUT_INVALID"
    assert "command" not in error["fixes"][0]
    assert "INVALID_NAMESPACE" not in stdout


def test_cli_rejects_in_cluster_context_combination_before_building_kubernetes_client(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import dpone.readiness.airflow_self_service_secret_gc as command

    monkeypatch.setattr(
        command,
        "build_kubernetes_airflow_connection_secret_gc_adapter",
        lambda **_: pytest.fail("Kubernetes client must not be built for invalid local auth options"),
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "connection-secret-gc-plan",
            "--namespace",
            _NAMESPACE,
            "--kube-auth",
            "in-cluster",
            "--kube-context",
            "prod",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2, stderr
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_AIRFLOW_SECRET_GC_INPUT_INVALID"


@pytest.mark.parametrize(
    ("status", "expected_code", "expected_exit"),
    [
        (403, "DPONE_AIRFLOW_SECRET_GC_ACCESS_DENIED", 4),
        (500, "DPONE_AIRFLOW_SECRET_GC_DELETE_FAILED", 3),
    ],
)
def test_cli_apply_distinguishes_delete_rbac_from_dependency_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: int,
    expected_code: str,
    expected_exit: int,
) -> None:
    candidate = _secret(run_id=f"delete-{status}", age_seconds=200000)
    client = _FakeClient(
        secrets=(candidate,),
        delete_errors={
            candidate.name: AirflowConnectionSecretGcApiError(
                operation="delete",
                status=status,
                reason="access_denied" if status == 403 else "api_failure",
            )
        },
    )
    import dpone.readiness.airflow_self_service_secret_gc as command

    monkeypatch.setattr(
        command,
        "build_kubernetes_airflow_connection_secret_gc_adapter",
        lambda **_: client,
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "connection-secret-gc-apply",
            "--namespace",
            _NAMESPACE,
            "--actor",
            "ci://gc",
            "--allowed-actor",
            "ci://gc",
            "--confirm-delete",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == expected_exit, stderr
    payload = json.loads(stdout)
    assert payload["status"] == "failed"
    assert payload["items"][0]["error_code"] == expected_code


@pytest.mark.parametrize(
    ("error", "expected_code", "expected_exit"),
    [
        (
            AirflowConnectionSecretGcApiError(operation="configure", status=403, reason="access_denied"),
            "DPONE_AIRFLOW_SECRET_GC_ACCESS_DENIED",
            4,
        ),
        (
            AirflowConnectionSecretGcApiError(operation="configure", status=406, reason="metadata_only_unsupported"),
            "DPONE_AIRFLOW_SECRET_GC_METADATA_ONLY_UNSUPPORTED",
            4,
        ),
        (
            AirflowConnectionSecretGcApiError(operation="list", status=410, reason="inventory_expired"),
            "DPONE_AIRFLOW_SECRET_GC_INVENTORY_EXPIRED",
            3,
        ),
        (
            AirflowConnectionSecretGcApiError(operation="configure", status=None, reason="sdk_unavailable"),
            "DPONE_AIRFLOW_SECRET_GC_SDK_UNAVAILABLE",
            3,
        ),
    ],
)
def test_cli_maps_redacted_kubernetes_errors_to_stable_exit_codes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: AirflowConnectionSecretGcApiError,
    expected_code: str,
    expected_exit: int,
) -> None:
    import dpone.readiness.airflow_self_service_secret_gc as command

    monkeypatch.setattr(
        command,
        "build_kubernetes_airflow_connection_secret_gc_adapter",
        lambda **_: (_ for _ in ()).throw(error),
    )

    code, stdout, stderr = _run_cli(
        ["airflow", "connection-secret-gc-plan", "--namespace", _NAMESPACE, "--format", "json"],
        capsys,
    )

    assert code == expected_exit, stderr
    error_payload = json.loads(stdout)["errors"][0]
    assert error_payload["code"] == expected_code
    if expected_code == "DPONE_AIRFLOW_SECRET_GC_SDK_UNAVAILABLE":
        assert error_payload["fixes"] == [
            {
                "id": "install_kubernetes_support",
                "safety": "manual",
                "command": "python -m pip install 'dpone[kubernetes]'",
            }
        ]


def test_cli_maps_unexpected_exception_to_internal_error_without_echoing_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import dpone.readiness.airflow_self_service_secret_gc as command

    monkeypatch.setattr(
        command,
        "build_kubernetes_airflow_connection_secret_gc_adapter",
        lambda **_: (_ for _ in ()).throw(RuntimeError("token=raw-secret api.internal")),
    )

    code, stdout, stderr = _run_cli(
        ["airflow", "connection-secret-gc-plan", "--namespace", _NAMESPACE, "--format", "json"],
        capsys,
    )

    assert code == 5, stderr
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == "DPONE_INTERNAL_AIRFLOW_SECRET_GC_FAILED"
    assert "raw-secret" not in stdout
    assert "api.internal" not in stdout


def test_gc_schema_contracts_match_published_json_schemas() -> None:
    import jsonschema

    client = _FakeClient(secrets=(_secret(run_id="schema", age_seconds=200000),))
    plan = _service(client).plan(_plan_request())
    apply = _service(client).apply(_apply_request())

    for name, payload in (
        ("airflow-connection-secret-gc-plan", plan),
        ("airflow-connection-secret-gc-apply", apply),
    ):
        contract = get_gitops_schema_contract(name)
        assert contract is not None
        jsonschema.Draft202012Validator(contract.schema).validate(payload)
        published = json.loads(Path(f"docs/schemas/gitops/{name}.schema.json").read_text(encoding="utf-8"))
        assert published == contract.schema
