"""Bind trusted auditor artifacts to exact producer run attempts."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicy
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.ports.ci_shadow_reconciliation import CiShadowReconciliationProvider
from dpone.services.ci.shadow_capacity_archive import (
    CapacityArchiveError,
    extract_single_json_payload,
    verify_provider_archive,
)
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget
from dpone.services.ci.shadow_reconciliation_observation import ObservationError
from dpone.services.ci.shadow_reconciliation_tree import AttemptArtifactInventory

_MAX_ARCHIVE_BYTES = 8_388_608
_RECEIPT_MEMBER = "audit-receipt.json"
_RECEIPT_NAME = re.compile(r"^pr-gate-shadow-audit-(\d+)-(\d+)-(\d+)-(\d+)$")


@dataclass(frozen=True)
class AuditReceipt:
    """One archive-authenticated receipt and its exact producer identity."""

    auditor_run_id: int
    auditor_attempt: int
    producer_repository_id: int
    producer_run_id: int
    producer_attempt: int
    canonical_payload: bytes
    exact_producer_payload: bytes


def acquire_audit_receipts(
    inventories: Sequence[AttemptArtifactInventory],
    *,
    archive_fetcher: Callable[[int], bytes],
    provider: CiShadowReconciliationProvider,
    policy: ReconciliationPolicy,
    budget: RequestBudget,
) -> tuple[AuditReceipt, ...]:
    """Download one metered receipt per auditor attempt and bind its producer.

    ``archive_fetcher`` is the redirect-aware transport composition root.  It
    must meter the initial archive request and each redirect through ``budget``
    before returning bytes; this layer authenticates the bytes and accounts for
    the independent exact producer-attempt lookup.
    """

    receipts: list[AuditReceipt] = []
    for inventory in inventories:
        metadata = _select_receipt_metadata(inventory)
        archive = archive_fetcher(_positive(metadata.get("id"), "artifact id"))
        try:
            verify_provider_archive(
                archive,
                provider_size=_positive(metadata.get("size_in_bytes"), "artifact size"),
                provider_digest=_digest(metadata.get("digest")),
                max_archive_bytes=_MAX_ARCHIVE_BYTES,
            )
            payload = extract_single_json_payload(
                archive,
                expected_name=_RECEIPT_MEMBER,
                max_payload_bytes=_MAX_ARCHIVE_BYTES,
            ).payload
        except CapacityArchiveError as exc:
            raise ObservationError("audit receipt archive is not authenticated") from exc
        receipt = _receipt_identity(payload, inventory)
        exact_payload, exact = _exact_producer(provider, receipt, policy, budget)
        if (
            exact.get("id") != receipt.producer_run_id
            or exact.get("run_attempt") != receipt.producer_attempt
            or _positive(
                _mapping(exact.get("repository"), "exact producer repository").get("id"), "exact producer repository"
            )
            != receipt.producer_repository_id
        ):
            raise ObservationError("exact producer run changed identity")
        receipts.append(
            AuditReceipt(
                auditor_run_id=receipt.auditor_run_id,
                auditor_attempt=receipt.auditor_attempt,
                producer_repository_id=receipt.producer_repository_id,
                producer_run_id=receipt.producer_run_id,
                producer_attempt=receipt.producer_attempt,
                canonical_payload=receipt.canonical_payload,
                exact_producer_payload=exact_payload,
            )
        )
    if len({(receipt.auditor_run_id, receipt.auditor_attempt) for receipt in receipts}) != len(receipts):
        raise ObservationError("auditor receipt identity is duplicated")
    return tuple(sorted(receipts, key=lambda item: (item.auditor_run_id, item.auditor_attempt)))


def _select_receipt_metadata(inventory: AttemptArtifactInventory) -> Mapping[str, object]:
    matches: list[Mapping[str, object]] = []
    for artifact in inventory.artifacts:
        name = artifact.get("name")
        match = _RECEIPT_NAME.fullmatch(name) if isinstance(name, str) else None
        if match is None:
            continue
        producer_run, producer_attempt, auditor_run, auditor_attempt = (int(value) for value in match.groups())
        if (
            auditor_run == inventory.run_id
            and auditor_attempt == inventory.attempt
            and producer_run > 0
            and producer_attempt > 0
        ):
            matches.append(artifact)
    if len(matches) != 1:
        raise ObservationError("auditor attempt does not have exactly one matching receipt artifact")
    if matches[0].get("expired") is not False:
        raise ObservationError("auditor receipt artifact is expired or ambiguous")
    return matches[0]


def _receipt_identity(payload: bytes, inventory: AttemptArtifactInventory) -> AuditReceipt:
    try:
        receipt = strict_json_object(payload)
    except StrictJsonError as exc:
        raise ObservationError("audit receipt JSON is invalid") from exc
    event = _mapping(receipt.get("observed_event"), "receipt observed event")
    auditor = _mapping(receipt.get("auditor_identity"), "receipt auditor identity")
    if receipt.get("schema_version") != "dpone.pr-gate-shadow-audit.v1":
        raise ObservationError("audit receipt schema is unsupported")
    if (
        _positive(auditor.get("run_id"), "receipt auditor run") != inventory.run_id
        or _positive(auditor.get("run_attempt"), "receipt auditor attempt") != inventory.attempt
    ):
        raise ObservationError("audit receipt does not bind its auditor attempt")
    return AuditReceipt(
        auditor_run_id=inventory.run_id,
        auditor_attempt=inventory.attempt,
        producer_repository_id=_positive(event.get("repository_id"), "receipt producer repository"),
        producer_run_id=_positive(event.get("producer_run_id"), "receipt producer run"),
        producer_attempt=_positive(event.get("producer_run_attempt"), "receipt producer attempt"),
        canonical_payload=payload,
        exact_producer_payload=b"",
    )


def _exact_producer(
    provider: CiShadowReconciliationProvider,
    receipt: AuditReceipt,
    policy: ReconciliationPolicy,
    budget: RequestBudget,
) -> tuple[bytes, Mapping[str, object]]:
    payload = budget.dispatch(
        "exact_producer_run_requests",
        lambda timeout: provider.get_workflow_run_attempt(
            run_id=receipt.producer_run_id,
            attempt=receipt.producer_attempt,
            timeout_seconds=timeout,
            max_response_bytes=budget.remaining_response_bytes,
        ),
    )
    try:
        value = strict_json_object(payload)
    except StrictJsonError as exc:
        raise ObservationError("exact producer run JSON is invalid") from exc
    if value.get("workflow_id") != policy.producer_workflow_id:
        raise ObservationError("exact producer run does not match the fixed producer workflow")
    return payload, value


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ObservationError(f"{name} is malformed")
    return value


def _positive(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ObservationError(f"{name} is malformed")
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ObservationError("artifact digest is malformed")
    return value


__all__ = ["AuditReceipt", "acquire_audit_receipts"]
