"""Evaluate one live GitHub snapshot against frozen policy contexts."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


models = _load_sibling("dpone_agent_release_commit_models", "release_commit_models.py")
FAILED_STATES = models.FAILED_STATES
PENDING_STATES = models.PENDING_STATES
SPECIAL_STATES = models.SPECIAL_STATES
Blocker = models.Blocker
GateReport = models.GateReport
LiveSnapshot = models.LiveSnapshot
Observation = models.Observation
RequiredContext = models.RequiredContext
ContextResult = tuple[dict[str, Any], tuple[Any, ...]]


def evaluate_snapshot(
    snapshot: Any,
    repo: str,
    commit_sha: str,
    ruleset_id: int,
    attempts: int,
    policy_contexts: tuple[str, ...],
    *,
    policy_sha256: str = "",
    policy_projection: dict[str, Any] | None = None,
) -> Any:
    blockers: list[Any] = []
    if snapshot.enforcement != "active":
        blockers.append(Blocker("RULESET_NOT_ACTIVE", "The live GitHub ruleset enforcement is not active."))
    if not snapshot.required_contexts:
        message = "The live GitHub ruleset has no required status contexts."
        blockers.append(Blocker("REQUIRED_CONTEXTS_UNAVAILABLE", message))
    live_contexts = frozenset(item.name for item in snapshot.required_contexts)
    policy_names = frozenset(policy_contexts)
    if live_contexts != policy_names:
        missing = ", ".join(sorted(policy_names - live_contexts)) or "none"
        unexpected = ", ".join(sorted(live_contexts - policy_names)) or "none"
        message = (
            "Live required-context names differ from the checked-in policy "
            f"(missing: {missing}; unexpected: {unexpected})."
        )
        blockers.append(Blocker("RULESET_POLICY_DRIFT", message))
    if policy_projection is not None:
        blockers.extend(_projection_blockers(policy_projection, getattr(snapshot, "ruleset_projection", None)))
    if any(item.commit_sha != commit_sha.lower() for item in snapshot.observations):
        message = "Live GitHub evidence is not bound to the requested release commit."
        blockers.append(Blocker("LIVE_EVIDENCE_COMMIT_MISMATCH", message))
    contexts: list[dict[str, Any]] = []
    for requirement in snapshot.required_contexts:
        result, context_blockers = _evaluate_context(requirement, snapshot.observations, commit_sha)
        contexts.append(result)
        blockers.extend(context_blockers)
    return GateReport(
        "PASS" if not blockers else "FAIL",
        repo,
        commit_sha,
        ruleset_id,
        attempts,
        tuple(contexts),
        tuple(blockers),
        policy_sha256,
    )


def _projection_blockers(policy_projection: dict[str, Any], live_projection: Any) -> list[Any]:
    if not isinstance(live_projection, dict):
        return [
            Blocker(
                "RULESET_PROJECTION_UNAVAILABLE",
                "Live ruleset projection is unavailable for SS-46 governance compare.",
            )
        ]
    projection = _load_sibling("dpone_agent_governance_projection", "governance_projection.py")
    return [
        Blocker(item.code, item.message)
        for item in projection.compare_ruleset_projection(policy_projection, live_projection)
    ]


def _evaluate_context(required: Any, evidence: tuple[Any, ...], commit_sha: str) -> ContextResult:
    app_id = required.integration_id
    same_name = tuple(item for item in evidence if item.context == required.name)
    # Legacy commit statuses remain diagnostic input. Only check-runs bound to
    # the required GitHub App may cross the serialized authority boundary.
    eligible = tuple(item for item in same_name if item.source == "check_run" and item.integration_id == app_id)
    authoritative = tuple(sorted(eligible, key=repr))
    if app_id is None or app_id <= 0:
        message = (
            f"Required context '{required.name}' is not bound to a positive "
            "GitHub App integration id in the live ruleset."
        )
        blockers: tuple[Any, ...] = (Blocker("REQUIRED_CONTEXT_PRODUCER_UNBOUND", message),)
    elif same_name and not eligible:
        message = (
            f"Required context '{required.name}' has no check-run from GitHub App {app_id} on commit {commit_sha}."
        )
        blockers = (Blocker("REQUIRED_CONTEXT_PRODUCER_MISMATCH", message),)
    else:
        blockers = _context_blockers(required.name, authoritative, commit_sha)
    return (
        {
            "blocker_codes": [item.code for item in blockers],
            "context": required.name,
            "current_observations": [dict(item._asdict()) for item in authoritative],
            "integration_id": app_id,
            "observed_count": len(authoritative),
            "status": "PASS" if not blockers else "FAIL",
        },
        blockers,
    )


def _context_blockers(context: str, observations: tuple[Any, ...], commit_sha: str) -> tuple[Any, ...]:
    if not observations:
        return (
            Blocker("REQUIRED_CONTEXT_MISSING", f"Required context '{context}' is missing on commit {commit_sha}."),
        )
    return tuple(
        blocker
        for observation in observations
        if (blocker := _observation_blocker(context, observation, commit_sha)) is not None
    )


def _observation_blocker(context: str, observation: Any, commit_sha: str) -> Any | None:
    if observation.state == "success":
        return None
    if observation.state in PENDING_STATES:
        code = "REQUIRED_CONTEXT_PENDING"
    elif observation.state in FAILED_STATES:
        code = "REQUIRED_CONTEXT_FAILED"
    elif observation.state in SPECIAL_STATES:
        code = f"REQUIRED_CONTEXT_{observation.state.upper()}"
    else:
        code = "REQUIRED_CONTEXT_UNSUCCESSFUL"
    message = (
        f"Required context '{context}' observed {observation.state} from {observation.source} on commit {commit_sha}."
    )
    return Blocker(code, message)
