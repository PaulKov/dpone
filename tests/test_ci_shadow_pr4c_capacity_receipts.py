from __future__ import annotations

import hashlib
import io
import json
import zipfile

import pytest

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget
from dpone.services.ci.shadow_reconciliation_receipts import acquire_audit_receipts
from dpone.services.ci.shadow_reconciliation_tree import AttemptArtifactInventory


class _Provider:
    def get_workflow_run_attempt(self, **kwargs: object) -> bytes:
        return _json(
            {
                "id": kwargs["run_id"],
                "run_attempt": kwargs["attempt"],
                "repository": {"id": 1},
                "workflow_id": ReconciliationPolicyV1.fixed().producer_workflow_id,
            }
        )


def _json(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _archive(payload: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("audit-receipt.json", payload)
    return output.getvalue()


def _budget() -> RequestBudget:
    return RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=lambda: 0.0)


def test_receipt_acquisition_authenticates_archive_and_binds_exact_producer_attempt() -> None:
    payload = _json(
        {
            "schema_version": "dpone.pr-gate-shadow-audit.v1",
            "observed_event": {"repository_id": 1, "producer_run_id": 10, "producer_run_attempt": 2},
            "auditor_identity": {"run_id": 30, "run_attempt": 1},
        }
    )
    archive = _archive(payload)
    inventory = AttemptArtifactInventory(
        30,
        1,
        (
            {
                "id": 40,
                "name": "pr-gate-shadow-audit-10-2-30-1",
                "expired": False,
                "size_in_bytes": len(archive),
                "digest": "sha256:" + hashlib.sha256(archive).hexdigest(),
            },
        ),
    )
    budget = _budget()

    receipts = acquire_audit_receipts(
        (inventory,),
        archive_fetcher=lambda artifact_id: archive,
        provider=_Provider(),
        policy=ReconciliationPolicyV1.fixed(),
        budget=budget,
    )

    assert [(receipt.producer_run_id, receipt.producer_attempt) for receipt in receipts] == [(10, 2)]
    assert budget.counters()["exact_producer_run_requests"] == 1


def test_receipt_acquisition_rejects_missing_or_ambiguous_attempt_receipts() -> None:
    inventory = AttemptArtifactInventory(30, 1, ())

    with pytest.raises(ValueError, match="exactly one"):
        acquire_audit_receipts(
            (inventory,),
            archive_fetcher=lambda _: b"",
            provider=_Provider(),
            policy=ReconciliationPolicyV1.fixed(),
            budget=_budget(),
        )
