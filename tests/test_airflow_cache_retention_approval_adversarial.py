from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

import pytest

from dpone.contracts.deployment_cache_retention_state import canonical_digest, retention_operation_id

ROOT = Path(__file__).parents[1]
RUNBOOK = ROOT / "docs" / "airflow-cache-retention-approval.md"
REVIEW_ID = "00000000-0000-4000-8000-000000000001"


def _bash_block_after(content: str, marker: str) -> str:
    tail = content.split(marker, 1)[1]
    return tail.split("```bash", 1)[1].split("```", 1)[0].strip()


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _publication(*, expected_schema: str, target_sha256: str) -> dict[str, Any]:
    return {
        "schema": "dpone.airflow-cache-status-publication.v1",
        "passed": True,
        "status": "published",
        "source": ".retention.tmp",
        "target": "last-retention.json",
        "failure_marker": "last-retention-publication-failure.json",
        "expected_schema": expected_schema,
        "attempted_at": datetime.now(UTC).isoformat(),
        "source_sha256": target_sha256,
        "target_sha256": target_sha256,
    }


def _plan() -> dict[str, Any]:
    current = "sha256:" + "a" * 64
    protected = "sha256:" + "b" * 64
    candidate = "sha256:" + "c" * 64
    body: dict[str, Any] = {
        "schema": "dpone.deployment-cache-retention-plan.v1",
        "environment": "dev",
        "current_deployment_id": current,
        "protected_deployment_ids": [protected],
        "items": [
            {"deployment_id": current, "action": "protect", "reason": "current", "path": "/cache/current"},
            {
                "deployment_id": protected,
                "action": "protect",
                "reason": "retention_evidence",
                "path": "/cache/protected",
            },
            {
                "deployment_id": candidate,
                "action": "delete",
                "reason": "unreferenced",
                "path": "/cache/candidate",
            },
        ],
        "delete_candidates": [candidate],
        "recovery_revision": "sha256:" + "d" * 64,
    }
    return {**body, "plan_sha256": canonical_digest(body)}


def _apply(plan: dict[str, Any]) -> dict[str, Any]:
    items = plan["items"]
    assert isinstance(items, list)
    apply_items = [{**item, "action": "deleted" if item["action"] == "delete" else "skipped"} for item in items]
    plan_sha256 = str(plan["plan_sha256"])
    return {
        "schema": "dpone.deployment-cache-retention-apply.v3",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": plan["current_deployment_id"],
        "items": apply_items,
        "deleted_deployment_ids": list(plan["delete_candidates"]),
        "skipped_deployment_ids": [
            str(item["deployment_id"])
            for item in apply_items
            if item["action"] == "skipped" and item["deployment_id"] is not None
        ],
        "reviewed_plan_sha256": plan_sha256,
        "activation_history_revision": "sha256:" + "e" * 64,
        "operation_id": retention_operation_id(
            environment="dev",
            reviewed_plan_sha256=plan_sha256,
            review_id=REVIEW_ID,
        ),
        "review_id": REVIEW_ID,
        "receipt_revision": "sha256:" + "f" * 64,
        "transaction_status": "committed",
    }


def _write_snapshot(
    path: Path,
    *,
    plan: dict[str, Any] | None = None,
    apply: dict[str, Any] | None = None,
    publication: dict[str, Any],
) -> None:
    operational_status: dict[str, Any] = {}
    if plan is not None:
        operational_status.update(
            retention_plan=plan,
            retention_plan_publication=publication,
            retention_plan_publication_failure=None,
        )
    if apply is not None:
        operational_status.update(
            retention_apply=apply,
            retention_apply_publication=publication,
            retention_apply_publication_failure=None,
        )
    path.write_text(
        json.dumps({"warnings": [], "operational_status": operational_status}),
        encoding="utf-8",
    )


def _write_review_evidence(path: Path, plan: dict[str, Any]) -> None:
    path.mkdir(mode=0o700)
    plan_bytes = _canonical_bytes(plan)
    publication = _publication(
        expected_schema="dpone.deployment-cache-retention-plan.v1",
        target_sha256=_sha256(plan_bytes),
    )
    (path / "plan.json").write_bytes(plan_bytes)
    (path / "publication.json").write_bytes(_canonical_bytes(publication))
    checksums = "".join(
        f"{hashlib.sha256((path / name).read_bytes()).hexdigest()}  {name}\n"
        for name in ("plan.json", "publication.json")
    )
    (path / "SHA256SUMS").write_text(checksums, encoding="utf-8")


def _run(block: str, *, cwd: Path, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", block],
        cwd=cwd,
        env={**os.environ, **environment},
        check=False,
        text=True,
        capture_output=True,
    )


def test_review_step_seals_the_canonical_plan_for_post_apply_verification(tmp_path: Path) -> None:
    plan = _plan()
    plan_bytes = _canonical_bytes(plan)
    publication = _publication(
        expected_schema="dpone.deployment-cache-retention-plan.v1",
        target_sha256=_sha256(plan_bytes),
    )
    snapshot = tmp_path / "plan-snapshot.json"
    review_evidence = tmp_path / "review-evidence"
    _write_snapshot(snapshot, plan=plan, publication=publication)
    block = _bash_block_after(RUNBOOK.read_text(encoding="utf-8"), "Extract and seal the exact canonical plan")

    result = _run(
        block,
        cwd=tmp_path,
        environment={
            "CACHE_STATUS_SNAPSHOT": str(snapshot),
            "DPONE_ENVIRONMENT": "dev",
            "DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR": str(review_evidence),
        },
    )

    assert result.returncode == 0, result.stderr
    assert (review_evidence / "plan.json").read_bytes() == plan_bytes
    assert (review_evidence / "publication.json").is_file()
    assert (review_evidence / "SHA256SUMS").is_file()
    assert f"DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256={plan['plan_sha256']}" in result.stdout


@pytest.mark.parametrize(
    "forgery",
    ("protected_deletion", "omitted_candidate", "publication_digest", "plan_body"),
)
def test_post_apply_verifier_rejects_evidence_that_is_not_bound_to_reviewed_plan(
    tmp_path: Path,
    forgery: str,
) -> None:
    plan = _plan()
    review_evidence = tmp_path / "review-evidence"
    if forgery == "plan_body":
        plan["protected_deployment_ids"] = []
    _write_review_evidence(review_evidence, plan)
    apply = _apply(plan)
    if forgery == "protected_deletion":
        protected = str(plan["protected_deployment_ids"][0])
        for item in apply["items"]:
            if item["deployment_id"] == protected:
                item["action"] = "deleted"
                item["reason"] = "unreferenced"
        apply["deleted_deployment_ids"].append(protected)
        apply["skipped_deployment_ids"].remove(protected)
    elif forgery == "omitted_candidate":
        candidate = str(plan["delete_candidates"][0])
        apply["items"] = [item for item in apply["items"] if item["deployment_id"] != candidate]
        apply["deleted_deployment_ids"] = []
        for field in ("operation_id", "review_id", "receipt_revision", "transaction_status"):
            apply.pop(field)
    elif forgery == "plan_body":
        apply = _apply(_plan())
    apply_bytes = _canonical_bytes(apply)
    target_sha256 = _sha256(apply_bytes)
    if forgery == "publication_digest":
        target_sha256 = "sha256:" + "0" * 64
    publication = _publication(
        expected_schema="dpone.deployment-cache-retention-apply.v3",
        target_sha256=target_sha256,
    )
    snapshot = tmp_path / "apply-snapshot.json"
    _write_snapshot(snapshot, apply=apply, publication=publication)
    block = _bash_block_after(RUNBOOK.read_text(encoding="utf-8"), "After the one-time approval is deployed")

    result = _run(
        block,
        cwd=tmp_path,
        environment={
            "CACHE_STATUS_SNAPSHOT": str(snapshot),
            "DPONE_ENVIRONMENT": "dev",
            "DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256": str(_plan()["plan_sha256"]),
            "DPONE_CACHE_RETENTION_REVIEW_ID": REVIEW_ID,
            "DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR": str(review_evidence),
        },
    )

    assert result.returncode != 0, f"{forgery} unexpectedly passed\nstdout={result.stdout}\nstderr={result.stderr}"


def test_post_apply_verifier_accepts_exact_plan_apply_and_publication_bytes(tmp_path: Path) -> None:
    plan = _plan()
    review_evidence = tmp_path / "review-evidence"
    _write_review_evidence(review_evidence, plan)
    apply = _apply(plan)
    publication = _publication(
        expected_schema="dpone.deployment-cache-retention-apply.v3",
        target_sha256=_sha256(_canonical_bytes(apply)),
    )
    snapshot = tmp_path / "apply-snapshot.json"
    _write_snapshot(snapshot, apply=apply, publication=publication)
    block = _bash_block_after(RUNBOOK.read_text(encoding="utf-8"), "After the one-time approval is deployed")

    result = _run(
        block,
        cwd=tmp_path,
        environment={
            "CACHE_STATUS_SNAPSHOT": str(snapshot),
            "DPONE_ENVIRONMENT": "dev",
            "DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256": str(plan["plan_sha256"]),
            "DPONE_CACHE_RETENTION_REVIEW_ID": REVIEW_ID,
            "DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR": str(review_evidence),
        },
    )

    assert result.returncode == 0, result.stderr
    assert "verified plan-bound destructive v3 apply evidence" in result.stdout


def test_post_apply_verifier_accepts_plan_bound_noop_without_receipt(tmp_path: Path) -> None:
    plan = _plan()
    plan["items"] = [item for item in plan["items"] if item["action"] != "delete"]
    plan["delete_candidates"] = []
    plan["plan_sha256"] = canonical_digest({key: value for key, value in plan.items() if key != "plan_sha256"})
    review_evidence = tmp_path / "review-evidence"
    _write_review_evidence(review_evidence, plan)
    apply = _apply(plan)
    for field in (
        "activation_history_revision",
        "operation_id",
        "review_id",
        "receipt_revision",
        "transaction_status",
    ):
        apply.pop(field)
    publication = _publication(
        expected_schema="dpone.deployment-cache-retention-apply.v3",
        target_sha256=_sha256(_canonical_bytes(apply)),
    )
    snapshot = tmp_path / "apply-snapshot.json"
    _write_snapshot(snapshot, apply=apply, publication=publication)
    block = _bash_block_after(RUNBOOK.read_text(encoding="utf-8"), "After the one-time approval is deployed")

    result = _run(
        block,
        cwd=tmp_path,
        environment={
            "CACHE_STATUS_SNAPSHOT": str(snapshot),
            "DPONE_ENVIRONMENT": "dev",
            "DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256": str(plan["plan_sha256"]),
            "DPONE_CACHE_RETENTION_REVIEW_ID": REVIEW_ID,
            "DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR": str(review_evidence),
        },
    )

    assert result.returncode == 0, result.stderr
    assert "verified plan-bound schema-valid v3 no-op" in result.stdout
