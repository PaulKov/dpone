"""Policy service for metadata-only Airflow Connection Secret garbage collection."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dpone.ports.airflow_connection_secret_gc import (
    AirflowConnectionSecretGcApiError,
    AirflowConnectionSecretGcDeletion,
    AirflowConnectionSecretGcInventory,
    KubernetesMetadataSnapshot,
)
from dpone.services.airflow_connection_secret_gc_models import (
    AirflowConnectionSecretGcApplyRequest,
    AirflowConnectionSecretGcError,
    AirflowConnectionSecretGcPlanRequest,
)
from dpone.services.airflow_connection_secret_gc_policy import (
    AirflowConnectionSecretGcCandidate,
    classify_airflow_connection_secret_inventory,
    iso_utc,
    require_aware_utc,
)


@dataclass(frozen=True, slots=True)
class _Classification:
    report: dict[str, Any]
    candidates: tuple[AirflowConnectionSecretGcCandidate, ...]


class AirflowConnectionSecretGcService:
    """Classify protected/orphaned Secrets and conditionally delete a bounded batch."""

    def __init__(
        self,
        *,
        inventory: AirflowConnectionSecretGcInventory,
        deletion: AirflowConnectionSecretGcDeletion,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._inventory = inventory
        self._deletion = deletion
        self._now = now or (lambda: datetime.now(UTC))

    def plan(self, request: AirflowConnectionSecretGcPlanRequest) -> dict[str, Any]:
        return self._classify(request).report

    def apply(self, request: AirflowConnectionSecretGcApplyRequest) -> dict[str, Any]:
        request.require_authorized()
        classified = self._classify(
            AirflowConnectionSecretGcPlanRequest(
                namespace=request.namespace,
                minimum_age_seconds=request.minimum_age_seconds,
                page_size=request.page_size,
            )
        )
        selected = classified.candidates[: request.max_delete_count]
        remainder = classified.candidates[request.max_delete_count :]
        items = [_apply_item(candidate, action="skipped", reason="batch_limit") for candidate in remainder]
        quarantined_items = _quarantined_apply_items(classified.report)
        items.extend(quarantined_items)
        deleted: list[str] = []
        skipped: list[str] = [candidate.secret_ref for candidate in remainder]
        failed: list[str] = []
        needs_retry = bool(remainder) or bool(quarantined_items)
        stop = False
        for candidate in selected:
            if stop:
                items.append(_apply_item(candidate, action="skipped", reason="not_attempted_after_failure"))
                skipped.append(candidate.secret_ref)
                continue
            try:
                self._deletion.delete_secret(
                    namespace=request.namespace,
                    name=candidate.snapshot.name,
                    uid=candidate.snapshot.uid,
                    resource_version=candidate.snapshot.resource_version,
                )
            except AirflowConnectionSecretGcApiError as exc:
                if exc.status in {404, 409}:
                    reason = "already_absent" if exc.status == 404 else "changed_since_plan"
                    items.append(_apply_item(candidate, action="skipped", reason=reason))
                    skipped.append(candidate.secret_ref)
                    needs_retry = needs_retry or exc.status == 409
                    continue
                items.append(
                    _apply_item(
                        candidate,
                        action="failed",
                        reason="delete_failed",
                        error_code=_delete_error_code(exc),
                        status=exc.status,
                    )
                )
                failed.append(candidate.secret_ref)
                stop = True
                continue
            deleted.append(candidate.secret_ref)
            items.append(_apply_item(candidate, action="deleted", reason=candidate.reason))
        status = "failed" if failed else "partial" if needs_retry else "ok"
        return {
            "schema": "dpone.airflow-connection-secret-gc-apply.v1",
            "status": status,
            "namespace": request.namespace,
            "actor": request.actor.strip(),
            "observed_at": classified.report["observed_at"],
            "minimum_age_seconds": request.minimum_age_seconds,
            "max_delete_count": request.max_delete_count,
            "items": sorted(items, key=lambda item: str(item["secret_ref"])),
            "deleted_secret_refs": sorted(deleted),
            "skipped_secret_refs": sorted(set(skipped)),
            "failed_secret_refs": sorted(failed),
        }

    def _classify(self, request: AirflowConnectionSecretGcPlanRequest) -> _Classification:
        observed_at = require_aware_utc(self._now())
        secrets = self._list_inventory("secrets", request)
        pods = self._list_inventory("pods", request)
        classified = classify_airflow_connection_secret_inventory(
            secrets=secrets,
            pods=pods,
            observed_at=observed_at,
            minimum_age_seconds=request.minimum_age_seconds,
        )
        report = {
            "schema": "dpone.airflow-connection-secret-gc-plan.v1",
            "status": classified.status,
            "namespace": request.namespace,
            "observed_at": iso_utc(observed_at),
            "minimum_age_seconds": request.minimum_age_seconds,
            "page_size": request.page_size,
            "inventory": classified.inventory,
            "items": list(classified.items),
            "delete_candidates": [candidate.secret_ref for candidate in classified.candidates],
        }
        return _Classification(report=report, candidates=classified.candidates)

    def _list_inventory(
        self,
        resource: str,
        request: AirflowConnectionSecretGcPlanRequest,
    ) -> tuple[KubernetesMetadataSnapshot, ...]:
        try:
            if resource == "secrets":
                return self._inventory.list_secret_metadata(namespace=request.namespace, page_size=request.page_size)
            return self._inventory.list_pod_metadata(namespace=request.namespace, page_size=request.page_size)
        except AirflowConnectionSecretGcApiError as exc:
            raise airflow_connection_secret_gc_error_from_api(exc) from None


def _apply_item(
    candidate: AirflowConnectionSecretGcCandidate,
    *,
    action: str,
    reason: str,
    error_code: str | None = None,
    status: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "secret_ref": candidate.secret_ref,
        "attempt_ref": candidate.attempt_ref,
        "action": action,
        "reason": reason,
    }
    if error_code is not None:
        payload["error_code"] = error_code
    if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599:
        payload["http_status"] = status
    return payload


def _delete_error_code(exc: AirflowConnectionSecretGcApiError) -> str:
    if exc.status in {401, 403} or exc.reason == "access_denied":
        return "DPONE_AIRFLOW_SECRET_GC_ACCESS_DENIED"
    return "DPONE_AIRFLOW_SECRET_GC_DELETE_FAILED"


def _quarantined_apply_items(plan: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "secret_ref": item["secret_ref"],
            "attempt_ref": item.get("attempt_ref"),
            "action": "skipped",
            "reason": item["reason"],
        }
        for item in plan["items"]
        if item["action"] == "quarantine"
    ]


def airflow_connection_secret_gc_error_from_api(
    exc: AirflowConnectionSecretGcApiError,
) -> AirflowConnectionSecretGcError:
    """Map a redacted adapter failure to one stable application error."""

    if exc.status in {401, 403} or exc.reason == "access_denied":
        code = "DPONE_AIRFLOW_SECRET_GC_ACCESS_DENIED"
    elif exc.status == 406 or exc.reason == "metadata_only_unsupported":
        code = "DPONE_AIRFLOW_SECRET_GC_METADATA_ONLY_UNSUPPORTED"
    elif exc.status == 410 or exc.reason == "inventory_expired":
        code = "DPONE_AIRFLOW_SECRET_GC_INVENTORY_EXPIRED"
    elif exc.reason == "sdk_unavailable":
        code = "DPONE_AIRFLOW_SECRET_GC_SDK_UNAVAILABLE"
    elif exc.reason == "invalid_metadata_response":
        code = "DPONE_AIRFLOW_SECRET_GC_INVENTORY_INVALID"
    else:
        code = "DPONE_AIRFLOW_SECRET_GC_DEPENDENCY_UNAVAILABLE"
    return AirflowConnectionSecretGcError(
        code, "Airflow Connection Secret metadata inventory is unavailable or unsafe."
    )


def airflow_connection_secret_gc_error_from_exception(
    exc: BaseException,
) -> AirflowConnectionSecretGcError | None:
    """Map known adapter errors while leaving unexpected failures distinguishable."""

    if not isinstance(exc, AirflowConnectionSecretGcApiError):
        return None
    return airflow_connection_secret_gc_error_from_api(exc)


__all__ = [
    "AirflowConnectionSecretGcApplyRequest",
    "AirflowConnectionSecretGcError",
    "AirflowConnectionSecretGcPlanRequest",
    "AirflowConnectionSecretGcService",
    "airflow_connection_secret_gc_error_from_api",
    "airflow_connection_secret_gc_error_from_exception",
]
