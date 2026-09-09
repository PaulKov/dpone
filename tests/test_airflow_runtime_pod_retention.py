from __future__ import annotations

import argparse
import inspect
import io
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from dpone_airflow_pack.provider_execution_contract import (
    RUNTIME_POD_CONTRACT_KEY,
    RUNTIME_POD_CONTRACT_VALUE,
    RUNTIME_POD_LABEL_SELECTOR,
    RUNTIME_POD_MANAGED_BY_KEY,
    RUNTIME_POD_MANAGED_BY_VALUE,
    WORKLOAD_ID_METADATA_KEY,
)

from dpone.adapters.airflow_runtime_pod_retention_events import (
    JsonLinesAirflowRuntimePodRetentionEvidencePublisher,
)
from dpone.adapters.kubernetes_airflow_runtime_pod_retention import (
    KubernetesAirflowRuntimePodRetentionAdapter,
)
from dpone.adapters.kubernetes_metadata import KubernetesMetadataReadBudget, KubernetesPartialMetadataClient
from dpone.commands.airflow_runtime_pod_retention_cmd import cmd_airflow_runtime_pod_retention_apply
from dpone.contracts.airflow_runtime_pod_metadata import AirflowRuntimePodOwnershipContract
from dpone.contracts.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApplyRequest,
    AirflowRuntimePodRetentionError,
    AirflowRuntimePodRetentionPlanRequest,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.ports.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApiError,
    AirflowRuntimePodRetentionEvidenceError,
    TerminalPodMetadataSnapshot,
)
from dpone.ports.kubernetes_metadata import KubernetesMetadataSnapshot
from dpone.readiness.airflow_self_service_pod_retention import (
    runtime_pod_retention_apply_command_result,
    runtime_pod_retention_error_result,
)
from dpone.services.airflow_runtime_pod_retention import AirflowRuntimePodRetentionService

_NOW = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)
_NAMESPACE = "airflow-example"
_OWNERSHIP = AirflowRuntimePodOwnershipContract(
    managed_by_key=RUNTIME_POD_MANAGED_BY_KEY,
    managed_by_value=RUNTIME_POD_MANAGED_BY_VALUE,
    runtime_contract_key=RUNTIME_POD_CONTRACT_KEY,
    runtime_contract_value=RUNTIME_POD_CONTRACT_VALUE,
    workload_id_key=WORKLOAD_ID_METADATA_KEY,
)


def _metadata(
    name: str,
    *,
    age_seconds: int = 100_000,
    uid: str | None = None,
    labels: dict[str, str] | None = None,
    deleting: bool = False,
    created_at: datetime | None = None,
) -> KubernetesMetadataSnapshot:
    return KubernetesMetadataSnapshot(
        resource="pods",
        name=name,
        uid=uid or f"uid-{name}",
        resource_version="42",
        creation_timestamp=created_at if created_at is not None else _NOW - timedelta(seconds=age_seconds),
        labels=labels
        if labels is not None
        else {
            RUNTIME_POD_MANAGED_BY_KEY: RUNTIME_POD_MANAGED_BY_VALUE,
            RUNTIME_POD_CONTRACT_KEY: RUNTIME_POD_CONTRACT_VALUE,
            WORKLOAD_ID_METADATA_KEY: "orders_load",
            "dag_id": "DAG__sales__orders__refresh",
            "task_id": "orders_load",
            "run_id": "scheduled__2026-08-03",
        },
        annotations={},
        deletion_timestamp=_NOW - timedelta(minutes=1) if deleting else None,
    )


def _terminal(name: str, *, phase: str = "Succeeded", **kwargs: object) -> TerminalPodMetadataSnapshot:
    return TerminalPodMetadataSnapshot(metadata=_metadata(name, **kwargs), phase=phase)


class _FakePodClient:
    def __init__(
        self,
        pods: tuple[TerminalPodMetadataSnapshot, ...],
        *,
        delete_errors: dict[str, AirflowRuntimePodRetentionApiError] | None = None,
        credential_mode: str = "in-cluster",
        credential_context: str | None = None,
    ) -> None:
        self.pods = pods
        self.delete_errors = delete_errors or {}
        self.list_calls = 0
        self.deletes: list[dict[str, str]] = []
        self.credential_mode = credential_mode
        self.credential_context = credential_context

    def resolved_credential_mode(self) -> str:
        return self.credential_mode

    def resolved_credential_context(self) -> str | None:
        return self.credential_context

    def list_terminal_pod_metadata(self, *, namespace: str, page_size: int) -> tuple[TerminalPodMetadataSnapshot, ...]:
        assert namespace == _NAMESPACE
        assert page_size > 0
        self.list_calls += 1
        return self.pods

    def delete_pod(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
        error = self.delete_errors.get(name)
        if error is not None:
            raise error
        self.deletes.append({"namespace": namespace, "name": name, "uid": uid, "resource_version": resource_version})


class _RecordingEvidencePublisher:
    durability = "process_ordered"

    def __init__(self, *, fail_on_sequence: int | None = None) -> None:
        self.events: list[dict[str, object]] = []
        self.fail_on_sequence = fail_on_sequence

    def publish(self, event: dict[str, object]) -> None:
        if event["sequence"] == self.fail_on_sequence:
            raise AirflowRuntimePodRetentionEvidenceError("simulated evidence failure")
        self.events.append(dict(event))


def _service(
    client: _FakePodClient,
    *,
    evidence: _RecordingEvidencePublisher | None = None,
) -> AirflowRuntimePodRetentionService:
    return AirflowRuntimePodRetentionService(
        inventory=client,
        ownership=_OWNERSHIP,
        deletion=client,
        credentials=client,
        evidence=evidence or _RecordingEvidencePublisher(),
        now=lambda: _NOW,
    )


def test_destructive_service_does_not_expose_replaceable_classifier_policy() -> None:
    signature = inspect.signature(AirflowRuntimePodRetentionService)

    assert "classifier" not in signature.parameters


def _plan_request(**overrides: object) -> AirflowRuntimePodRetentionPlanRequest:
    values = {"namespace": _NAMESPACE, "minimum_age_seconds": 86_400, "page_size": 500, **overrides}
    return AirflowRuntimePodRetentionPlanRequest(**values)


def _apply_request(**overrides: object) -> AirflowRuntimePodRetentionApplyRequest:
    actor = "serviceaccount://airflow-example/dpone-runtime-pod-retention"
    values = {
        "namespace": _NAMESPACE,
        "minimum_age_seconds": 86_400,
        "page_size": 500,
        "max_delete_count": 100,
        "actor": actor,
        "allowed_actors": (actor,),
        "confirm_delete": True,
        "kube_auth_mode": "in-cluster",
        **overrides,
    }
    return AirflowRuntimePodRetentionApplyRequest(**values)


def test_label_selector_is_exact_and_requires_workload_key() -> None:
    assert RUNTIME_POD_LABEL_SELECTOR == (
        "dpone.dev/managed-by=airflow-provider,dpone.dev/runtime-contract=init-fetch-v2,dpone.dev/workload-id"
    )


def test_core_runtime_pod_policy_and_adapter_do_not_import_provider_package() -> None:
    for path in (
        "src/dpone/services/airflow_runtime_pod_retention_policy.py",
        "src/dpone/adapters/kubernetes_airflow_runtime_pod_retention.py",
    ):
        assert "dpone_airflow_pack" not in Path(path).read_text(encoding="utf-8")


def test_namespace_whitespace_is_rejected_instead_of_normalized() -> None:
    with pytest.raises(AirflowRuntimePodRetentionError):
        _plan_request(namespace=" airflow-example")


def test_plan_deletes_only_old_correlated_terminal_pods_and_reports_age_limitation() -> None:
    pods = (
        _terminal("old-success"),
        _terminal("old-failed", phase="Failed"),
        _terminal("young", age_seconds=3_600),
        _terminal("terminating", deleting=True),
        _terminal(
            "missing-correlation",
            labels={
                RUNTIME_POD_MANAGED_BY_KEY: RUNTIME_POD_MANAGED_BY_VALUE,
                RUNTIME_POD_CONTRACT_KEY: RUNTIME_POD_CONTRACT_VALUE,
                WORKLOAD_ID_METADATA_KEY: "orders_load",
            },
        ),
    )

    report = _service(_FakePodClient(pods)).plan(_plan_request())

    assert report["schema"] == "dpone.airflow-runtime-pod-retention-plan.v1"
    assert report["status"] == "needs_attention"
    assert report["age_basis"] == "creation_timestamp_fallback"
    assert report["terminal_age_exact"] is False
    assert (
        GitOpsSchemaValidator().validate(
            report,
            expected_kind="dpone.airflow-runtime-pod-retention-plan.v1",
        )
        == ()
    )
    assert report["delete_candidates"] == ["old-failed", "old-success"]
    reasons = {item["pod_name"]: item["reason"] for item in report["items"]}
    assert reasons == {
        "missing-correlation": "missing_correlation",
        "old-failed": "stale_terminal",
        "old-success": "stale_terminal",
        "terminating": "terminating",
        "young": "minimum_age",
    }


def test_duplicate_uid_across_terminal_phase_queries_is_quarantined() -> None:
    pods = (
        _terminal("same", phase="Succeeded", uid="uid-same"),
        _terminal("same", phase="Failed", uid="uid-same"),
    )

    report = _service(_FakePodClient(pods)).plan(_plan_request())

    assert report["delete_candidates"] == []
    assert {item["reason"] for item in report["items"]} == {"inventory_phase_conflict"}
    assert report["status"] == "needs_attention"


def test_future_or_missing_creation_timestamp_is_preserved() -> None:
    pods = (
        _terminal("missing", created_at=None),
        _terminal("future", created_at=_NOW + timedelta(minutes=5)),
    )
    missing = TerminalPodMetadataSnapshot(
        metadata=KubernetesMetadataSnapshot(
            resource="pods",
            name="missing",
            uid="uid-missing",
            resource_version="1",
            creation_timestamp=None,
            labels=_metadata("template").labels,
        ),
        phase="Succeeded",
    )

    report = _service(_FakePodClient((missing, pods[1]))).plan(_plan_request())

    assert {item["reason"] for item in report["items"]} == {"timestamp_missing", "future_timestamp"}
    assert report["delete_candidates"] == []


def test_naive_application_clock_uses_runtime_pod_error_domain() -> None:
    service = AirflowRuntimePodRetentionService(
        inventory=_FakePodClient((_terminal("candidate"),)),
        ownership=_OWNERSHIP,
        now=lambda: datetime(2026, 8, 3, 12, 0),
    )

    with pytest.raises(AirflowRuntimePodRetentionError) as exc_info:
        service.plan(_plan_request())

    assert exc_info.value.code == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CLOCK_INVALID"


def test_apply_replans_fresh_deletes_bounded_batch_and_uses_preconditions() -> None:
    client = _FakePodClient(tuple(_terminal(f"pod-{index}", age_seconds=100_000 + index) for index in range(3)))

    report = _service(client).apply(_apply_request(max_delete_count=2))

    assert client.list_calls == 1
    assert len(client.deletes) == 2
    assert all(item["uid"].startswith("uid-pod-") and item["resource_version"] == "42" for item in client.deletes)
    assert report["status"] == "partial"
    assert len(report["delete_accepted_pod_names"]) == 2
    assert report["deletion_evidence"] == "api_request_accepted_not_observed"
    assert report["authorization_authority"] == "kubernetes_api_rbac"
    assert report["credential_mode"] == "in-cluster"
    assert report["credential_context"] is None
    assert {item["action"] for item in report["items"] if item["reason"] == "stale_terminal"} == {"delete_accepted"}
    assert len(report["skipped_pod_names"]) == 1
    assert (
        GitOpsSchemaValidator().validate(
            report,
            expected_kind="dpone.airflow-runtime-pod-retention-apply.v1",
        )
        == ()
    )
    assert all(str(item["pod_ref"]).startswith("sha256:") for item in report["items"])
    plan = _service(_FakePodClient(client.pods)).plan(_plan_request())
    plan_refs = {item["pod_name"]: (item["pod_ref"], item["precondition_ref"]) for item in plan["items"]}
    apply_refs = {item["pod_name"]: (item["pod_ref"], item["precondition_ref"]) for item in report["items"]}
    assert apply_refs == plan_refs
    assert [item["sequence"] for item in report["items"]] == list(range(1, len(report["items"]) + 1))


def test_apply_preserves_quarantined_inventory_and_never_returns_false_success() -> None:
    pod = _terminal(
        "missing-correlation",
        labels={
            RUNTIME_POD_MANAGED_BY_KEY: RUNTIME_POD_MANAGED_BY_VALUE,
            RUNTIME_POD_CONTRACT_KEY: RUNTIME_POD_CONTRACT_VALUE,
            WORKLOAD_ID_METADATA_KEY: "orders_load",
        },
    )

    report = _service(_FakePodClient((pod,))).apply(_apply_request())

    assert report["status"] == "partial"
    assert report["delete_accepted_pod_names"] == []
    assert report["skipped_pod_names"] == ["missing-correlation"]
    assert report["items"][0]["action"] == "preserved"
    assert report["items"][0]["reason"] == "missing_correlation"


def test_apply_preserves_404_and_409_then_stops_after_hard_failure() -> None:
    pods = tuple(_terminal(name) for name in ("absent", "changed", "failed", "not-attempted"))
    client = _FakePodClient(
        pods,
        delete_errors={
            "absent": AirflowRuntimePodRetentionApiError(operation="delete", status=404, reason="api_failure"),
            "changed": AirflowRuntimePodRetentionApiError(operation="delete", status=409, reason="api_failure"),
            "failed": AirflowRuntimePodRetentionApiError(operation="delete", status=500, reason="api_failure"),
        },
    )

    report = _service(client).apply(_apply_request())

    assert report["status"] == "failed"
    reasons = {item["pod_name"]: item["reason"] for item in report["items"]}
    assert reasons == {
        "absent": "already_absent",
        "changed": "changed_since_plan",
        "failed": "delete_failed",
        "not-attempted": "not_attempted_after_failure",
    }
    assert client.deletes == []


def test_apply_authorization_fails_before_inventory() -> None:
    client = _FakePodClient((_terminal("candidate"),))

    with pytest.raises(Exception, match="confirm-delete"):
        _service(client).apply(_apply_request(confirm_delete=False))

    assert client.list_calls == 0


def test_plan_does_not_require_delete_capability() -> None:
    client = _FakePodClient((_terminal("candidate"),))

    report = AirflowRuntimePodRetentionService(
        inventory=client,
        ownership=_OWNERSHIP,
        now=lambda: _NOW,
    ).plan(_plan_request())

    assert report["delete_candidates"] == ["candidate"]
    assert client.deletes == []


def test_apply_reports_delete_acceptance_without_claiming_absence() -> None:
    client = _FakePodClient(
        (_terminal("candidate"),),
        credential_mode="kubeconfig",
        credential_context="reviewed-dev",
    )

    report = _service(client).apply(_apply_request(kube_auth_mode="kubeconfig"))

    assert client.list_calls == 1
    assert report["status"] == "ok"
    assert report["credential_mode"] == "kubeconfig"
    assert report["credential_context"] == "reviewed-dev"
    assert report["deletion_evidence"] == "api_request_accepted_not_observed"
    assert report["delete_accepted_pod_names"] == ["candidate"]
    assert report["items"][0]["action"] == "delete_accepted"


def test_apply_records_resolved_credentials_and_rejects_explicit_mode_mismatch() -> None:
    client = _FakePodClient((_terminal("candidate"),), credential_mode="in-cluster")

    with pytest.raises(AirflowRuntimePodRetentionError) as exc_info:
        _service(client).apply(_apply_request(kube_auth_mode="kubeconfig"))

    assert exc_info.value.code == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CREDENTIAL_MODE_MISMATCH"
    assert client.list_calls == 0
    assert client.deletes == []


@pytest.mark.parametrize(
    ("auth_mode", "kube_context"),
    (("auto", None), ("kubeconfig", None), ("kubeconfig", ""), ("in-cluster", "ambient-context")),
)
def test_apply_rejects_ambient_or_ambiguous_credentials_before_inventory(
    monkeypatch: pytest.MonkeyPatch,
    auth_mode: str,
    kube_context: str | None,
) -> None:
    from dpone.readiness import airflow_self_service_pod_retention as readiness

    adapter_called = False

    def unexpected_adapter(**_kwargs: object) -> None:
        nonlocal adapter_called
        adapter_called = True

    monkeypatch.setattr(readiness, "_adapter", unexpected_adapter)
    actor = "serviceaccount://airflow-example/dpone-runtime-pod-retention"

    result = runtime_pod_retention_apply_command_result(
        namespace=_NAMESPACE,
        minimum_age_seconds=86_400,
        page_size=500,
        max_delete_count=100,
        actor=actor,
        allowed_actors=(actor,),
        confirm_delete=True,
        auth_mode=auth_mode,
        kube_context=kube_context,
        evidence=_RecordingEvidencePublisher(),
    )

    assert result.passed is False
    assert result.exit_code == 2
    assert adapter_called is False
    assert result.errors[0]["code"] == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INPUT_INVALID"


def test_apply_publishes_operation_intent_before_each_delete_and_completed_outcome() -> None:
    client = _FakePodClient((_terminal("candidate"),))
    publisher = _RecordingEvidencePublisher()

    report = _service(client, evidence=publisher).apply(_apply_request())

    assert report["operation_id"].startswith("sha256:")
    assert report["evidence_status"] == "complete"
    assert report["evidence_durability"] == "process_ordered"
    assert [event["event"] for event in publisher.events] == [
        "operation_started",
        "delete_intent",
        "delete_outcome",
        "operation_completed",
    ]
    assert publisher.events[1]["precondition_ref"] == report["items"][0]["precondition_ref"]
    assert publisher.events[2]["outcome"] == "delete_accepted"
    assert all(
        GitOpsSchemaValidator().validate(
            event,
            expected_kind="dpone.airflow-runtime-pod-retention-event.v1",
        )
        == ()
        for event in publisher.events
    )


def test_runtime_pod_evidence_schemas_reject_impossible_states() -> None:
    client = _FakePodClient((_terminal("candidate"),))
    publisher = _RecordingEvidencePublisher()
    report = _service(client, evidence=publisher).apply(_apply_request())
    validator = GitOpsSchemaValidator()

    invalid_intent = dict(publisher.events[1])
    invalid_intent.update(pod_ref=None, precondition_ref=None, pod_name=None, outcome="ok")
    assert validator.validate(
        invalid_intent,
        expected_kind="dpone.airflow-runtime-pod-retention-event.v1",
    )

    unresolved_credentials = {**report, "credential_mode": "auto"}
    assert validator.validate(
        unresolved_credentials,
        expected_kind="dpone.airflow-runtime-pod-retention-apply.v1",
    )

    false_success = {**report, "status": "ok", "evidence_status": "incomplete"}
    assert validator.validate(
        false_success,
        expected_kind="dpone.airflow-runtime-pod-retention-apply.v1",
    )

    failed_without_code = {
        **publisher.events[2],
        "outcome": "delete_failed",
        "error_code": None,
    }
    assert validator.validate(
        failed_without_code,
        expected_kind="dpone.airflow-runtime-pod-retention-event.v1",
    )

    duplicate_apply = {
        **report,
        "items": [report["items"][0], {**report["items"][0], "sequence": 2}],
    }
    assert any(
        issue.path == "items.pod_ref"
        for issue in validator.validate(
            duplicate_apply,
            expected_kind="dpone.airflow-runtime-pod-retention-apply.v1",
        )
    )


def test_runtime_pod_plan_semantics_reject_duplicate_or_forged_projections() -> None:
    validator = GitOpsSchemaValidator()
    report = _service(_FakePodClient((_terminal("candidate"),))).plan(_plan_request())
    duplicate = {
        **report,
        "items": [report["items"][0], report["items"][0]],
        "inventory": {**report["inventory"], "terminal_pods": 2, "succeeded_pods": 2},
        "delete_candidates": ["candidate"],
    }

    issues = validator.validate(
        duplicate,
        expected_kind="dpone.airflow-runtime-pod-retention-plan.v1",
    )

    assert {issue.path for issue in issues}.issuperset({"items.pod_ref", "items.precondition_ref", "items.pod_name"})

    forged_status = {**report, "status": "ok"}
    assert any(
        issue.path == "status"
        for issue in validator.validate(
            forged_status,
            expected_kind="dpone.airflow-runtime-pod-retention-plan.v1",
        )
    )


def test_abrupt_death_after_intent_leaves_recovery_evidence_before_mutation_outcome() -> None:
    class FatalCrash(BaseException):
        pass

    class CrashingClient(_FakePodClient):
        def delete_pod(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
            raise FatalCrash("simulated SIGKILL boundary")

    client = CrashingClient((_terminal("candidate"),))
    publisher = _RecordingEvidencePublisher()

    with pytest.raises(FatalCrash):
        _service(client, evidence=publisher).apply(_apply_request())

    assert [event["event"] for event in publisher.events] == ["operation_started", "delete_intent"]
    assert publisher.events[-1]["pod_name"] == "candidate"
    assert publisher.events[-1]["outcome"] == "intent_recorded"


def test_apply_stops_before_delete_when_intent_evidence_cannot_be_published() -> None:
    client = _FakePodClient((_terminal("candidate"),))
    publisher = _RecordingEvidencePublisher(fail_on_sequence=2)

    report = _service(client, evidence=publisher).apply(_apply_request())

    assert client.deletes == []
    assert report["status"] == "failed"
    assert report["evidence_status"] == "incomplete"
    assert report["items"][0]["error_code"] == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_UNAVAILABLE"


def test_apply_reports_post_delete_evidence_failure_without_losing_delete_acceptance() -> None:
    client = _FakePodClient((_terminal("candidate"),))
    publisher = _RecordingEvidencePublisher(fail_on_sequence=3)

    report = _service(client, evidence=publisher).apply(_apply_request())

    assert client.deletes[0]["name"] == "candidate"
    assert report["status"] == "failed"
    assert report["evidence_status"] == "incomplete"
    assert report["delete_accepted_pod_names"] == ["candidate"]
    assert report["failed_pod_names"] == ["candidate"]
    assert report["items"][0]["action"] == "delete_accepted"
    assert report["items"][0]["reason"] == "evidence_unavailable"
    assert (
        GitOpsSchemaValidator().validate(
            report,
            expected_kind="dpone.airflow-runtime-pod-retention-apply.v1",
        )
        == ()
    )


def test_apply_rejects_invalid_evidence_capability_before_inventory() -> None:
    client = _FakePodClient((_terminal("candidate"),))
    publisher = _RecordingEvidencePublisher()
    publisher.durability = "unknown"

    with pytest.raises(AirflowRuntimePodRetentionError) as exc_info:
        _service(client, evidence=publisher).apply(_apply_request())

    assert exc_info.value.code == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_CAPABILITY_INVALID"
    assert client.list_calls == 0
    assert client.deletes == []


def test_jsonl_evidence_adapter_writes_one_flushed_machine_readable_event() -> None:
    class FlushCountingStream(io.StringIO):
        flush_count = 0

        def flush(self) -> None:
            self.flush_count += 1
            super().flush()

    stream = FlushCountingStream()
    event = {"schema": "dpone.airflow-runtime-pod-retention-event.v1", "sequence": 1}

    JsonLinesAirflowRuntimePodRetentionEvidencePublisher(stream).publish(event)

    assert json.loads(stream.getvalue()) == event
    assert stream.getvalue().endswith("\n")
    assert stream.flush_count == 1


def test_apply_emits_partial_evidence_when_controlled_cancellation_interrupts_batch() -> None:
    class InterruptingClient(_FakePodClient):
        def delete_pod(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
            if name == "second":
                raise SystemExit("simulated termination")
            super().delete_pod(
                namespace=namespace,
                name=name,
                uid=uid,
                resource_version=resource_version,
            )

    client = InterruptingClient((_terminal("first", age_seconds=100_002), _terminal("second", age_seconds=100_001)))

    report = _service(client).apply(_apply_request())

    assert report["status"] == "failed"
    assert report["delete_accepted_pod_names"] == ["first"]
    by_name = {item["pod_name"]: item for item in report["items"]}
    assert by_name["first"]["action"] == "delete_accepted"
    assert by_name["second"]["action"] == "failed"
    assert by_name["second"]["error_code"] == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INTERRUPTED"
    assert (
        GitOpsSchemaValidator().validate(
            report,
            expected_kind="dpone.airflow-runtime-pod-retention-apply.v1",
        )
        == ()
    )


class _MetadataTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.deletes: list[dict[str, str]] = []

    def list_metadata_page(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        phase = str(kwargs["field_selector"]).split("=", 1)[1]
        item = _metadata(f"pod-{phase.lower()}")
        return {
            "kind": "PartialObjectMetadataList",
            "metadata": {"continue": ""},
            "items": [
                {
                    "kind": "PartialObjectMetadata",
                    "metadata": {
                        "name": item.name,
                        "uid": item.uid,
                        "resourceVersion": item.resource_version,
                        "creationTimestamp": item.creation_timestamp.isoformat().replace("+00:00", "Z"),
                        "labels": item.labels,
                    },
                }
            ],
        }

    def delete_pod(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
        self.deletes.append({"namespace": namespace, "name": name, "uid": uid, "resource_version": resource_version})


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("creationTimestamp", "2026-08-01T00:00:00"),
        ("deletionTimestamp", "not-a-timestamp"),
        ("creationTimestamp", {"unexpected": "mapping"}),
    ),
)
def test_kubernetes_adapter_rejects_present_untrusted_timestamp_before_policy(
    field: str,
    value: object,
) -> None:
    class InvalidTimestampTransport(_MetadataTransport):
        def list_metadata_page(self, **kwargs: object) -> dict[str, object]:
            page = super().list_metadata_page(**kwargs)
            metadata = page["items"][0]["metadata"]
            metadata[field] = value
            return page

    adapter = KubernetesAirflowRuntimePodRetentionAdapter(
        transport=InvalidTimestampTransport(),
        credential_mode="kubeconfig",
        credential_context="reviewed-dev",
        label_selector=RUNTIME_POD_LABEL_SELECTOR,
    )

    with pytest.raises(AirflowRuntimePodRetentionApiError, match="invalid_metadata_response"):
        adapter.list_terminal_pod_metadata(namespace=_NAMESPACE, page_size=500)


def test_kubernetes_adapter_uses_exact_terminal_field_selectors_and_metadata_only_inventory() -> None:
    transport = _MetadataTransport()
    adapter = KubernetesAirflowRuntimePodRetentionAdapter(
        transport=transport,
        credential_mode="kubeconfig",
        credential_context="reviewed-dev",
        label_selector=RUNTIME_POD_LABEL_SELECTOR,
    )

    pods = adapter.list_terminal_pod_metadata(namespace=_NAMESPACE, page_size=500)

    assert [pod.phase for pod in pods] == ["Failed", "Succeeded"]
    assert [call["field_selector"] for call in transport.calls] == ["status.phase=Failed", "status.phase=Succeeded"]
    assert all(call["label_selector"] == RUNTIME_POD_LABEL_SELECTOR for call in transport.calls)
    assert all("PartialObjectMetadataList" in str(call["accept"]) for call in transport.calls)


def test_terminal_phase_queries_share_one_inventory_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    import dpone.adapters.kubernetes_airflow_runtime_pod_retention as adapter_module

    transport = _MetadataTransport()
    monkeypatch.setattr(
        adapter_module,
        "KubernetesMetadataReadBudget",
        lambda: KubernetesMetadataReadBudget(remaining_items=1, remaining_bytes=1024 * 1024),
    )
    adapter = KubernetesAirflowRuntimePodRetentionAdapter(
        transport=transport,
        credential_mode="kubeconfig",
        credential_context="reviewed-dev",
        label_selector=RUNTIME_POD_LABEL_SELECTOR,
    )

    with pytest.raises(AirflowRuntimePodRetentionApiError, match="invalid_metadata_response"):
        adapter.list_terminal_pod_metadata(namespace=_NAMESPACE, page_size=500)

    assert len(transport.calls) == 1


def test_metadata_client_rejects_page_larger_than_requested_limit() -> None:
    class OversizedPage:
        def list_metadata_page(self, **_kwargs: object) -> dict[str, object]:
            item = {
                "kind": "PartialObjectMetadata",
                "metadata": {
                    "name": "pod",
                    "uid": "uid",
                    "resourceVersion": "1",
                    "creationTimestamp": "2026-08-01T00:00:00Z",
                },
            }
            return {
                "kind": "PartialObjectMetadataList",
                "metadata": {"continue": ""},
                "items": [item, item],
            }

    with pytest.raises(Exception, match="invalid_metadata_response"):
        KubernetesPartialMetadataClient(transport=OversizedPage()).list_metadata(
            resource="pods",
            namespace=_NAMESPACE,
            label_selector=RUNTIME_POD_LABEL_SELECTOR,
            field_selector="status.phase=Succeeded",
            page_size=1,
        )


def test_access_denied_delete_maps_to_security_exit_four(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.readiness import airflow_self_service_pod_retention as readiness

    client = _FakePodClient(
        (_terminal("denied"),),
        delete_errors={
            "denied": AirflowRuntimePodRetentionApiError(operation="delete", status=403, reason="access_denied")
        },
    )
    monkeypatch.setattr(readiness, "_adapter", lambda **_kwargs: client)
    actor = "serviceaccount://airflow-example/dpone-runtime-pod-retention"

    result = runtime_pod_retention_apply_command_result(
        namespace=_NAMESPACE,
        minimum_age_seconds=86_400,
        page_size=500,
        max_delete_count=100,
        actor=actor,
        allowed_actors=(actor,),
        confirm_delete=True,
        auth_mode="in-cluster",
        kube_context=None,
        evidence=_RecordingEvidencePublisher(),
    )

    assert result.passed is False
    assert result.exit_code == 4


def test_evidence_unavailable_maps_to_dependency_exit_three() -> None:
    result = runtime_pod_retention_error_result(
        AirflowRuntimePodRetentionError(
            "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_UNAVAILABLE",
            "Mutation evidence could not be published.",
        ),
        namespace=_NAMESPACE,
    )

    assert result.passed is False
    assert result.exit_code == 3


def test_apply_cli_separates_report_stdout_from_mutation_events_stderr(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.readiness import airflow_self_service_pod_retention as readiness

    client = _FakePodClient((_terminal("cli-evidence", created_at=datetime(2020, 1, 1, tzinfo=UTC)),))
    monkeypatch.setattr(readiness, "_adapter", lambda **_kwargs: client)
    actor = "serviceaccount://airflow-example/dpone-runtime-pod-retention"
    args = argparse.Namespace(
        namespace=_NAMESPACE,
        minimum_age_seconds=86_400,
        page_size=500,
        max_delete_count=100,
        actor=actor,
        allowed_actor=[actor],
        confirm_delete=True,
        kube_auth="in-cluster",
        kube_context=None,
        format="json",
    )

    exit_code = cmd_airflow_runtime_pod_retention_apply(
        args,
        ctx=object(),
        logger=logging.getLogger("test.runtime_pod_retention_cli"),
    )

    captured = capsys.readouterr()
    report = json.loads(captured.out)
    events = [json.loads(line) for line in captured.err.splitlines()]
    assert exit_code == 0
    assert report["schema"] == "dpone.airflow-runtime-pod-retention-apply.v1"
    assert report["evidence_status"] == "complete"
    assert report["operation_id"] == events[0]["operation_id"]
    assert [event["event"] for event in events] == [
        "operation_started",
        "delete_intent",
        "delete_outcome",
        "operation_completed",
    ]
    assert client.deletes == [
        {
            "namespace": _NAMESPACE,
            "name": "cli-evidence",
            "uid": "uid-cli-evidence",
            "resource_version": "42",
        }
    ]


def test_apply_error_remediation_preserves_reviewed_plan_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.readiness import airflow_self_service_pod_retention as readiness

    def fail_adapter(**_kwargs: object) -> None:
        raise ValueError("invalid test input")

    monkeypatch.setattr(readiness, "_adapter", fail_adapter)

    result = runtime_pod_retention_apply_command_result(
        namespace=_NAMESPACE,
        minimum_age_seconds=172_800,
        page_size=250,
        max_delete_count=50,
        actor="user://reviewer",
        allowed_actors=("user://reviewer",),
        confirm_delete=True,
        auth_mode="kubeconfig",
        kube_context="reviewed-prod",
        evidence=_RecordingEvidencePublisher(),
    )

    command = result.errors[0]["fixes"][0]["command"]
    assert command == (
        "dpone airflow runtime-pod-retention-plan --namespace airflow-example "
        "--minimum-age-seconds 172800 --page-size 250 --kube-auth kubeconfig "
        "--kube-context reviewed-prod"
    )


def test_invalid_in_cluster_context_remediation_preserves_auth_mode() -> None:
    from dpone.readiness import airflow_self_service_pod_retention as readiness

    result = readiness.runtime_pod_retention_plan_command_result(
        namespace=_NAMESPACE,
        minimum_age_seconds=172_800,
        page_size=250,
        auth_mode="in-cluster",
        kube_context="reviewed-prod",
    )

    command = result.errors[0]["fixes"][0]["command"]
    assert "--kube-auth in-cluster" in command
    assert "--kube-context" not in command


def test_invalid_scope_remediation_uses_safe_canonical_defaults() -> None:
    from dpone.readiness import airflow_self_service_pod_retention as readiness

    result = readiness.runtime_pod_retention_plan_command_result(
        namespace="NOT VALID",
        minimum_age_seconds=299,
        page_size=1001,
        auth_mode="auto",
        kube_context=None,
    )

    command = result.errors[0]["fixes"][0]["command"]
    assert command == (
        "dpone airflow runtime-pod-retention-plan --namespace airflow-example "
        "--minimum-age-seconds 86400 --page-size 500 --kube-auth auto"
    )
