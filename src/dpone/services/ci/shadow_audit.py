"""Fail-closed identity audit for PR Gate shadow claims.

The service deliberately has no filesystem, subprocess, checkout, cache, or
network dependency.  A trusted composition root injects a read-only provider.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from time import monotonic, sleep

from dpone.contracts.ci_shadow_audit import (
    AUDITOR_BUNDLE_UNAPPROVED,
    AUDITOR_CLAIMS_UNVERIFIED,
    AUDITOR_JOBS_UNVERIFIED,
    AUDITOR_MERGE_REF_UNVERIFIED,
    AUDITOR_RUN_UNVERIFIED,
    AuditResult,
    MergeTuple,
)
from dpone.ports.ci_shadow_audit import CiShadowAuditProvider, CiShadowAuditProviderError
from dpone.services.ci.shadow import ShadowContractError, audit_provider_decision
from dpone.services.ci.shadow_audit_identity import positive_int as _positive_int
from dpone.services.ci.shadow_audit_identity import sha as _sha
from dpone.services.ci.shadow_audit_identity import stable_merge_snapshot as _stable_snapshot


def audit_event(
    event: Mapping[str, object],
    *,
    provider: CiShadowAuditProvider,
    expected_bundle_digest: str,
    trusted_bundle: Sequence[Mapping[str, str]],
    clock: Callable[[], float] = monotonic,
    wait: Callable[[float], None] = sleep,
) -> AuditResult | None:
    """Audit one completed producer event or return receipt-free pending telemetry.

    The event fixture holds trusted workflow-run identity plus untrusted claims.
    Provider observations are freshly acquired for two consecutive equal
    snapshots both initially and again at the receipt linearization point.
    """

    _require_event_shape(event)
    workflow_run = _mapping(event["workflow_run"], "workflow_run")
    if workflow_run["conclusion"] == "action_required":
        return None
    if workflow_run["status"] != "completed" or workflow_run["conclusion"] not in {"success", "failure"}:
        return _unverified_receipt(event, blockers=["PROVENANCE_NON_ALLOWLISTED"])

    try:
        _require_exact_run_identity(workflow_run, provider)
    except (CiShadowAuditProviderError, ShadowContractError, KeyError, TypeError, ValueError):
        return _unverified_receipt(event, blockers=[AUDITOR_RUN_UNVERIFIED])
    try:
        claims = _mapping(event["claims"], "claims")
        _require_claims_match_run(claims, workflow_run)
        if claims.get("implementation_bundle_digest") != expected_bundle_digest:
            raise ShadowContractError("claims bundle digest does not match trusted auditor bundle")
    except (CiShadowAuditProviderError, ShadowContractError, KeyError, TypeError, ValueError):
        return _unverified_receipt(event, blockers=[AUDITOR_CLAIMS_UNVERIFIED])
    try:
        initial = _stable_snapshot(claims, workflow_run, provider, clock, wait)
        _validate_claims_tuple(claims, initial)
    except (CiShadowAuditProviderError, ShadowContractError, KeyError, TypeError, ValueError):
        return _unverified_receipt(event, blockers=[AUDITOR_MERGE_REF_UNVERIFIED])
    try:
        _require_trusted_bundle_sources(initial, provider, trusted_bundle)
    except (CiShadowAuditProviderError, ShadowContractError, KeyError, TypeError, ValueError):
        return _unverified_receipt(
            event,
            blockers=[AUDITOR_BUNDLE_UNAPPROVED],
            stage="RUN_AUTHENTICATED",
            merge_tuple=initial,
        )
    try:
        decision = _provider_decision(claims, workflow_run, provider)
    except (CiShadowAuditProviderError, ShadowContractError, KeyError, TypeError, ValueError):
        return _unverified_receipt(
            event,
            blockers=[AUDITOR_JOBS_UNVERIFIED],
            stage="RUN_AUTHENTICATED",
            merge_tuple=initial,
        )
    try:
        linearized = _stable_snapshot(claims, workflow_run, provider, clock, wait)
        if linearized != initial:
            raise ShadowContractError("merge tuple changed before receipt persistence")
    except (CiShadowAuditProviderError, ShadowContractError, KeyError, TypeError, ValueError):
        return _unverified_receipt(
            event,
            blockers=[AUDITOR_MERGE_REF_UNVERIFIED],
            stage="JOBS_BOUND",
            merge_tuple=initial,
        )

    if decision == "UNVERIFIED":
        return _unverified_receipt(
            event,
            blockers=[AUDITOR_JOBS_UNVERIFIED],
            stage="JOBS_BOUND",
            merge_tuple=initial,
        )

    return _receipt(event, decision=decision, stage="CLAIMS_BOUND", blockers=[], merge_tuple=initial)


def _require_event_shape(event: Mapping[str, object]) -> None:
    if set(event) != {"workflow_run", "claims", "auditor"}:
        raise ShadowContractError("audit event has unknown or missing fields")


def _require_claims_match_run(claims: Mapping[str, object], run: Mapping[str, object]) -> None:
    for name in ("repository_id", "run_id", "run_attempt", "head_sha"):
        if claims.get(name) != run.get(name):
            raise ShadowContractError(f"claims {name} does not match producer run")
    _sha(claims.get("head_sha"), "claims.head_sha")


def _require_exact_run_identity(event_run: Mapping[str, object], provider: CiShadowAuditProvider) -> None:
    """Bind the delivery to the provider's exact producer-run record."""

    run_id = _positive_int(event_run.get("run_id"), "workflow_run.run_id")
    provider_run = provider.get_workflow_run(run_id)
    for name in (
        "repository_id",
        "head_repository_id",
        "run_id",
        "run_attempt",
        "head_branch",
        "head_sha",
        "status",
        "conclusion",
    ):
        if provider_run.get(name) != event_run.get(name):
            raise ShadowContractError(f"provider run {name} does not match incoming event")
    _positive_int(provider_run.get("head_repository_id"), "provider run head_repository_id")
    head_branch = provider_run.get("head_branch")
    if not isinstance(head_branch, str) or not head_branch:
        raise ShadowContractError("provider run head_branch must be a non-empty string")
    if provider_run.get("event") != "pull_request":
        raise ShadowContractError("producer run is not a pull_request workflow")
    if provider_run.get("path") != ".github/workflows/pr-gate-shadow.yml":
        raise ShadowContractError("producer workflow path is not exact")


def _validate_claims_tuple(claims: Mapping[str, object], merge_tuple: MergeTuple) -> None:
    if (
        claims.get("repository_id"),
        claims.get("pr_number"),
        claims.get("base_sha"),
        claims.get("head_sha"),
        claims.get("merge_sha"),
    ) != (
        merge_tuple.repository_id,
        merge_tuple.pr_number,
        merge_tuple.base_sha,
        merge_tuple.head_sha,
        merge_tuple.merge_sha,
    ):
        raise ShadowContractError("claims tuple does not match provider tuple")


def _provider_decision(
    claims: Mapping[str, object], workflow_run: Mapping[str, object], provider: CiShadowAuditProvider
) -> str:
    """Fold exact provider job conclusions instead of trusting claimed status."""

    jobs = provider.list_attempt_jobs(
        _positive_int(workflow_run.get("run_id"), "workflow_run.run_id"),
        _positive_int(workflow_run.get("run_attempt"), "workflow_run.run_attempt"),
    )
    return audit_provider_decision(claims.get("jobs"), [dict(job) for job in jobs], claims.get("status"))


def _require_trusted_bundle_sources(
    merge_tuple: MergeTuple,
    provider: CiShadowAuditProvider,
    trusted_bundle: Sequence[Mapping[str, str]],
) -> None:
    """Bind immutable B/H/M workflow bytes and H control-plane bytes to trust.

    The trusted manifest is loaded only from the default-branch auditor
    checkout.  The producer's self-reported digest is never a substitute for
    these Git-object comparisons.
    """

    if not trusted_bundle:
        raise ShadowContractError("trusted bundle is empty")
    base_tree = provider.get_git_tree(merge_tuple.base_sha)
    head_tree = provider.get_git_tree(merge_tuple.head_sha)
    merge_tree = provider.get_git_tree(merge_tuple.merge_sha)
    for entry in trusted_bundle:
        path = entry.get("path")
        role = entry.get("role")
        expected_digest = entry.get("sha256")
        if not isinstance(path, str) or not isinstance(role, str) or not isinstance(expected_digest, str):
            raise ShadowContractError("trusted bundle entry is malformed")
        head_entry = _regular_blob(head_tree, path)
        content = provider.get_git_blob(_sha(head_entry.get("sha"), "trusted bundle blob"))
        if hashlib.sha256(content).hexdigest() != expected_digest:
            raise ShadowContractError("subject trusted bundle byte digest differs")
        if role == "EXECUTION_WORKFLOW":
            base_entry = _regular_blob(base_tree, path)
            merge_entry = _regular_blob(merge_tree, path)
            if head_entry.get("sha") != base_entry.get("sha") or head_entry.get("sha") != merge_entry.get("sha"):
                raise ShadowContractError("producer workflow blob differs across B/H/M")


def _regular_blob(tree: Mapping[str, Mapping[str, object]], path: str) -> Mapping[str, object]:
    entry = tree.get(path)
    if not isinstance(entry, Mapping) or entry.get("type") != "blob" or entry.get("mode") != "100644":
        raise ShadowContractError("trusted bundle path is not a regular Git blob")
    return entry


def _unverified_receipt(
    event: Mapping[str, object],
    *,
    blockers: list[str],
    stage: str = "EVENT_RECEIVED",
    merge_tuple: MergeTuple | None = None,
) -> AuditResult:
    return _receipt(event, decision="UNVERIFIED", stage=stage, blockers=blockers, merge_tuple=merge_tuple)


def _receipt(
    event: Mapping[str, object],
    *,
    decision: str,
    stage: str,
    blockers: list[str],
    merge_tuple: MergeTuple | None,
) -> AuditResult:
    run = _mapping(event["workflow_run"], "workflow_run")
    auditor = _mapping(event["auditor"], "auditor")
    receipt: dict[str, object] = {
        "schema_version": "dpone.pr-gate-shadow-audit.v1",
        "decision": decision,
        "audit_stage": stage,
        "observed_event": {
            "repository_id": run.get("repository_id"),
            "producer_run_id": run.get("run_id"),
            "producer_run_attempt": run.get("run_attempt"),
            "head_sha": run.get("head_sha"),
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
        },
        "producer_identity": None
        if merge_tuple is None
        else {
            "repository_id": merge_tuple.repository_id,
            "pr_number": merge_tuple.pr_number,
            "base_sha": merge_tuple.base_sha,
            "head_sha": merge_tuple.head_sha,
            "merge_sha": merge_tuple.merge_sha,
            "eligible_set_digest": merge_tuple.eligible_set_digest,
        },
        "auditor_identity": dict(auditor),
        "blockers": sorted(blockers),
    }
    return AuditResult(decision=decision, receipt=receipt)  # type: ignore[arg-type]


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ShadowContractError(f"{name} must be an object")
    return value
