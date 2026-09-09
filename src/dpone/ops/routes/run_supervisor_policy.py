"""Generic route run supervisor policy."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.run_supervisor_models import RouteRunDecision, RouteRunEvidence, RouteRunStatus

_UNSAFE_BLOCKER_TOKENS = (
    "state",
    "schema",
    "approval",
    "repair",
    "release",
    "route_mismatch",
    "duplicate",
    "commit",
    "fencing",
    "token",
)


class RouteRunSupervisorPolicy:
    """Classify a route run from normalized evidence only."""

    def evaluate(
        self,
        *,
        route: RouteKey,
        profile_exists: bool,
        evidence: Sequence[RouteRunEvidence],
    ) -> RouteRunDecision:
        blockers = _blockers(route=route, profile_exists=profile_exists, evidence=evidence)
        warnings = _warnings(evidence)
        status = _status(blockers=blockers, evidence=evidence, profile_exists=profile_exists)
        return RouteRunDecision(
            status=status,
            passed=status == "ready",
            retry_safe=status == "retryable",
            blockers=blockers,
            warnings=warnings,
            next_actions=_next_actions(status=status, blockers=blockers),
        )


def _blockers(
    *,
    route: RouteKey,
    profile_exists: bool,
    evidence: Sequence[RouteRunEvidence],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if not profile_exists:
        blockers.append(f"route.unsupported:{route.colon_id}")
    for item in evidence:
        blockers.extend(item.blockers)
        if item.requires_manual_approval:
            blockers.append(f"{item.name}.manual_approval_required")
    return tuple(dict.fromkeys(item for item in blockers if item))


def _warnings(evidence: Sequence[RouteRunEvidence]) -> tuple[str, ...]:
    warnings: list[str] = []
    optional_missing = [item.name for item in evidence if item.missing and not item.required]
    if optional_missing:
        warnings.append(f"optional_evidence_missing:{','.join(optional_missing)}")
    return tuple(warnings)


def _status(
    *,
    blockers: tuple[str, ...],
    evidence: Sequence[RouteRunEvidence],
    profile_exists: bool,
) -> RouteRunStatus:
    if not blockers and profile_exists:
        return "ready"
    if _has_hard_blocker(blockers, evidence):
        return "blocked"
    if any(item.requires_manual_approval for item in evidence):
        return "manual_approval_required"
    if _unsafe_to_retry(blockers, evidence):
        return "unsafe_to_retry"
    if _retryable(evidence):
        return "retryable"
    return "blocked"


def _has_hard_blocker(blockers: tuple[str, ...], evidence: Sequence[RouteRunEvidence]) -> bool:
    if any(blocker.startswith("route.unsupported") for blocker in blockers):
        return True
    if any(blocker.endswith(".missing") for blocker in blockers):
        return True
    if any(blocker.endswith(".invalid_json") or blocker.endswith(".invalid_shape") for blocker in blockers):
        return True
    return any(item.route_case_id and not item.route_matched for item in evidence)


def _unsafe_to_retry(blockers: tuple[str, ...], evidence: Sequence[RouteRunEvidence]) -> bool:
    if any(item.safe_to_retry is False for item in evidence if not item.passed):
        return True
    return any(any(token in blocker for token in _UNSAFE_BLOCKER_TOKENS) for blocker in blockers)


def _retryable(evidence: Sequence[RouteRunEvidence]) -> bool:
    failed = [item for item in evidence if not item.passed and not item.missing]
    return bool(failed) and all(item.safe_to_retry is not False for item in failed)


def _next_actions(*, status: RouteRunStatus, blockers: tuple[str, ...]) -> tuple[str, ...]:
    if status == "ready":
        return tuple()
    actions: list[str] = []
    if status == "retryable":
        actions.append("Resolve the runtime blocker, then rerun the route manifest or replay command.")
    elif status == "unsafe_to_retry":
        actions.append("Stop automatic retry; inspect state, schema, repair, and release evidence before continuing.")
    elif status == "manual_approval_required":
        actions.append("Capture approval evidence, then rerun route-run-supervisor.")
    else:
        actions.append("Resolve required route run evidence blockers before continuing.")
    for blocker in blockers:
        if blocker.startswith("route.unsupported"):
            actions.append("Add the route to the integration matrix before supervising route runs.")
        elif blocker.endswith(".missing"):
            actions.append(f"Attach `{blocker.removesuffix('.missing')}` evidence with --artifact.")
        elif blocker.endswith(".route_mismatch"):
            actions.append("Regenerate the mismatched artifact for the same source, sink, and strategy.")
        else:
            actions.append(f"Open the upstream artifact and resolve `{blocker}`.")
    return tuple(dict.fromkeys(actions))


__all__ = ["RouteRunSupervisorPolicy"]
