"""Plan/apply orchestration for bounded Airflow runtime Pod retention."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import Any

from dpone.contracts.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApplyRequest,
    AirflowRuntimePodRetentionError,
    AirflowRuntimePodRetentionPlanRequest,
    validate_apply_kubernetes_authority,
)
from dpone.contracts.airflow_runtime_pod_retention_apply import (
    EVIDENCE_DURABILITIES,
    EVIDENCE_FAILURE_CODE,
    derive_runtime_pod_apply_state,
)
from dpone.ports.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApiError,
    AirflowRuntimePodRetentionCredentialSource,
    AirflowRuntimePodRetentionDeletion,
    AirflowRuntimePodRetentionEvidenceError,
    AirflowRuntimePodRetentionEvidencePublisher,
    AirflowRuntimePodRetentionInventory,
)
from dpone.services.airflow_runtime_pod_retention_evidence import AirflowRuntimePodRetentionEvidenceSequence
from dpone.services.airflow_runtime_pod_retention_policy import (
    AirflowRuntimePodRetentionCandidate,
    RuntimePodOwnership,
    classify_runtime_pods,
    iso_utc,
    require_aware_utc,
    runtime_pod_precondition_ref,
    runtime_pod_ref,
)


@dataclass(frozen=True, slots=True)
class _Classification:
    report: dict[str, Any]
    candidates: tuple[AirflowRuntimePodRetentionCandidate, ...]


class AirflowRuntimePodRetentionService:
    def __init__(
        self,
        *,
        inventory: AirflowRuntimePodRetentionInventory,
        ownership: RuntimePodOwnership,
        deletion: AirflowRuntimePodRetentionDeletion | None = None,
        credentials: AirflowRuntimePodRetentionCredentialSource | None = None,
        evidence: AirflowRuntimePodRetentionEvidencePublisher | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._inventory = inventory
        self._ownership = ownership
        self._deletion = deletion
        self._credentials = credentials
        self._evidence = evidence
        self._now = now or (lambda: datetime.now(UTC))

    def plan(self, request: AirflowRuntimePodRetentionPlanRequest) -> dict[str, Any]:
        return self._classify(request).report

    def apply(self, request: AirflowRuntimePodRetentionApplyRequest) -> dict[str, Any]:
        request.require_authorized()
        if self._deletion is None:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DELETE_CAPABILITY_MISSING",
                "Airflow runtime Pod retention apply requires a deletion capability.",
            )
        if self._evidence is None:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_CAPABILITY_MISSING",
                "Airflow runtime Pod retention apply requires an evidence publisher.",
            )
        evidence_durability = self._resolved_evidence_durability()
        credential_mode, credential_context = self._resolved_credential_authority(request.kube_auth_mode)
        classified = self._classify(
            AirflowRuntimePodRetentionPlanRequest(
                namespace=request.namespace,
                minimum_age_seconds=request.minimum_age_seconds,
                page_size=request.page_size,
            )
        )
        selected = classified.candidates[: request.max_delete_count]
        remainder = classified.candidates[request.max_delete_count :]
        observed_at = str(classified.report["observed_at"])
        try:
            evidence = AirflowRuntimePodRetentionEvidenceSequence.start(
                publish_event=self._evidence.publish,
                namespace=request.namespace,
                observed_at=observed_at,
                actor=request.actor.strip(),
                credential_mode=credential_mode,
                credential_context=credential_context,
                selected_preconditions=tuple(
                    runtime_pod_precondition_ref(
                        request.namespace,
                        candidate.pod.metadata.uid,
                        candidate.pod.metadata.resource_version,
                    )
                    for candidate in selected
                ),
            )
        except AirflowRuntimePodRetentionEvidenceError:
            raise _evidence_unavailable() from None
        items: list[dict[str, Any]] = []
        evidence_status = "complete"
        stop = False
        for candidate in selected:
            name = candidate.pod.metadata.name
            if stop:
                items.append(
                    _apply_item(
                        candidate,
                        namespace=request.namespace,
                        action="skipped",
                        reason="not_attempted_after_failure",
                    )
                )
                continue
            intent_item = _apply_item(
                candidate,
                namespace=request.namespace,
                action="delete_accepted",
                reason="stale_terminal",
            )
            try:
                evidence.intent(intent_item)
            except AirflowRuntimePodRetentionEvidenceError:
                items.append(
                    _apply_item(
                        candidate,
                        namespace=request.namespace,
                        action="failed",
                        reason="evidence_unavailable",
                        error_code=EVIDENCE_FAILURE_CODE,
                    )
                )
                evidence_status = "incomplete"
                stop = True
                continue
            try:
                self._deletion.delete_pod(
                    namespace=request.namespace,
                    name=name,
                    uid=candidate.pod.metadata.uid,
                    resource_version=candidate.pod.metadata.resource_version,
                )
            except AirflowRuntimePodRetentionApiError as exc:
                if exc.status in {404, 409}:
                    reason = "already_absent" if exc.status == 404 else "changed_since_plan"
                    item = _apply_item(candidate, namespace=request.namespace, action="skipped", reason=reason)
                else:
                    item = _apply_item(
                        candidate,
                        namespace=request.namespace,
                        action="failed",
                        reason="delete_failed",
                        error_code=_delete_error_code(exc),
                        http_status=exc.status,
                    )
                    stop = True
            except (KeyboardInterrupt, SystemExit):
                item = _apply_item(
                    candidate,
                    namespace=request.namespace,
                    action="failed",
                    reason="delete_failed",
                    error_code="DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INTERRUPTED",
                )
                stop = True
            else:
                item = intent_item
            try:
                evidence.outcome(item, outcome=_event_outcome(item))
            except AirflowRuntimePodRetentionEvidenceError:
                item["reason"] = "evidence_unavailable"
                item["error_code"] = EVIDENCE_FAILURE_CODE
                evidence_status = "incomplete"
                stop = True
            items.append(item)
        items.extend(
            _apply_item(candidate, namespace=request.namespace, action="skipped", reason="batch_limit")
            for candidate in remainder
        )
        candidate_refs = {
            runtime_pod_ref(request.namespace, candidate.pod.metadata.uid) for candidate in classified.candidates
        }
        preserved_items = [
            _preserved_item(item) for item in classified.report["items"] if item["pod_ref"] not in candidate_refs
        ]
        items.extend(preserved_items)
        for sequence, item in enumerate(items, start=1):
            item["sequence"] = sequence
        state = derive_runtime_pod_apply_state(items, evidence_status=evidence_status)
        report = {
            "schema": "dpone.airflow-runtime-pod-retention-apply.v1",
            "status": state.status,
            "operation_id": evidence.operation_id,
            "evidence_status": evidence_status,
            "evidence_durability": evidence_durability,
            "namespace": request.namespace,
            "actor": request.actor.strip(),
            "actor_source": "operator_acknowledgement",
            "authorization_authority": "kubernetes_api_rbac",
            "credential_mode": credential_mode,
            "credential_context": credential_context,
            "deletion_evidence": "api_request_accepted_not_observed",
            "observed_at": classified.report["observed_at"],
            "minimum_age_seconds": request.minimum_age_seconds,
            "max_delete_count": request.max_delete_count,
            "age_basis": "creation_timestamp_fallback",
            "terminal_age_exact": False,
            "items": items,
            "delete_accepted_pod_names": list(state.delete_accepted_pod_names),
            "skipped_pod_names": list(state.skipped_pod_names),
            "failed_pod_names": list(state.failed_pod_names),
        }
        try:
            evidence.complete(state.status)
        except AirflowRuntimePodRetentionEvidenceError:
            report["status"] = "failed"
            report["evidence_status"] = "incomplete"
        return report

    def _resolved_evidence_durability(self) -> str:
        assert self._evidence is not None
        durability = getattr(self._evidence, "durability", None)
        if durability not in EVIDENCE_DURABILITIES:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_CAPABILITY_INVALID",
                "Airflow runtime Pod retention evidence durability capability is invalid.",
            )
        return str(durability)

    def _resolved_credential_authority(self, requested_mode: str) -> tuple[str, str | None]:
        if self._credentials is None:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CREDENTIAL_SOURCE_MISSING",
                "Airflow runtime Pod retention apply requires resolved credential evidence.",
            )
        resolved_mode = self._credentials.resolved_credential_mode()
        if resolved_mode not in {"in-cluster", "kubeconfig"}:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CREDENTIAL_SOURCE_INVALID",
                "Airflow runtime Pod retention credential evidence is invalid.",
            )
        if requested_mode != "auto" and requested_mode != resolved_mode:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CREDENTIAL_MODE_MISMATCH",
                "Resolved Kubernetes credential mode does not match the requested mode.",
            )
        resolved_context = self._credentials.resolved_credential_context()
        validate_apply_kubernetes_authority(resolved_mode, resolved_context)
        return resolved_mode, resolved_context

    def _classify(self, request: AirflowRuntimePodRetentionPlanRequest) -> _Classification:
        try:
            observed_at = require_aware_utc(self._now())
        except ValueError:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CLOCK_INVALID",
                "Airflow runtime Pod retention requires an explicit timezone-aware clock.",
            ) from None
        try:
            pods = self._inventory.list_terminal_pod_metadata(
                namespace=request.namespace,
                page_size=request.page_size,
            )
        except AirflowRuntimePodRetentionApiError as exc:
            raise error_from_api(exc) from None
        classified = classify_runtime_pods(
            pods,
            namespace=request.namespace,
            observed_at=observed_at,
            minimum_age_seconds=request.minimum_age_seconds,
            ownership=self._ownership,
        )
        report = {
            "schema": "dpone.airflow-runtime-pod-retention-plan.v1",
            "status": classified.status,
            "namespace": request.namespace,
            "observed_at": iso_utc(observed_at),
            "minimum_age_seconds": request.minimum_age_seconds,
            "page_size": request.page_size,
            "age_basis": "creation_timestamp_fallback",
            "terminal_age_exact": False,
            "inventory": classified.inventory,
            "warnings": list(classified.warnings),
            "items": list(classified.items),
            "delete_candidates": [candidate.pod.metadata.name for candidate in classified.candidates],
        }
        return _Classification(report=report, candidates=classified.candidates)


def _apply_item(
    candidate: AirflowRuntimePodRetentionCandidate,
    *,
    namespace: str,
    action: str,
    reason: str,
    error_code: str | None = None,
    http_status: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pod_ref": runtime_pod_ref(namespace, candidate.pod.metadata.uid),
        "precondition_ref": runtime_pod_precondition_ref(
            namespace,
            candidate.pod.metadata.uid,
            candidate.pod.metadata.resource_version,
        ),
        "pod_name": candidate.pod.metadata.name,
        "phase": candidate.pod.phase,
        "action": action,
        "reason": reason,
    }
    if error_code is not None:
        payload["error_code"] = error_code
    if isinstance(http_status, int) and not isinstance(http_status, bool) and 100 <= http_status <= 599:
        payload["http_status"] = http_status
    return payload


def _preserved_item(plan_item: dict[str, Any]) -> dict[str, Any]:
    return {
        "pod_ref": plan_item["pod_ref"],
        "precondition_ref": plan_item["precondition_ref"],
        "pod_name": plan_item["pod_name"],
        "phase": plan_item["phase"],
        "action": "preserved",
        "reason": plan_item["reason"],
    }


def _delete_error_code(exc: AirflowRuntimePodRetentionApiError) -> str:
    if exc.status in {401, 403} or exc.reason == "access_denied":
        return "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACCESS_DENIED"
    return "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DELETE_FAILED"


def _event_outcome(item: dict[str, Any]) -> str:
    if item["action"] == "delete_accepted":
        return "delete_accepted"
    if item.get("error_code") == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INTERRUPTED":
        return "interrupted"
    return str(item["reason"])


def _evidence_unavailable() -> AirflowRuntimePodRetentionError:
    return AirflowRuntimePodRetentionError(
        "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_UNAVAILABLE",
        "Airflow runtime Pod retention mutation evidence could not be published.",
    )


def error_from_api(exc: AirflowRuntimePodRetentionApiError) -> AirflowRuntimePodRetentionError:
    if exc.status in {401, 403} or exc.reason == "access_denied":
        code = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACCESS_DENIED"
    elif exc.status == 406 or exc.reason == "metadata_only_unsupported":
        code = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_METADATA_ONLY_UNSUPPORTED"
    elif exc.status == 410 or exc.reason == "inventory_expired":
        code = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INVENTORY_EXPIRED"
    elif exc.reason == "sdk_unavailable":
        code = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_SDK_UNAVAILABLE"
    elif exc.reason == "invalid_metadata_response":
        code = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INVENTORY_INVALID"
    else:
        code = "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DEPENDENCY_UNAVAILABLE"
    return AirflowRuntimePodRetentionError(code, "Airflow runtime Pod metadata inventory is unavailable or unsafe.")


__all__ = [
    "AirflowRuntimePodRetentionApplyRequest",
    "AirflowRuntimePodRetentionError",
    "AirflowRuntimePodRetentionPlanRequest",
    "AirflowRuntimePodRetentionService",
    "error_from_api",
]
