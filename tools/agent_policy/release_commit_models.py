"""Shared types and state sets for the exact-commit release gate."""

from __future__ import annotations

from typing import Any, NamedTuple

DEFAULT_POLICY_PATH = ".agents/policy/github-branch-protection.yml"
PENDING_STATES = frozenset({"expected", "in_progress", "pending", "queued", "requested", "waiting"})
FAILED_STATES = frozenset({"action_required", "error", "failure", "startup_failure", "timed_out"})
SPECIAL_STATES = frozenset({"cancelled", "neutral", "skipped", "stale"})
TERMINAL_BLOCKERS = frozenset(
    {
        "REQUIRED_CONTEXT_PRODUCER_UNBOUND",
        "RULESET_POLICY_DRIFT",
        "RULESET_TARGET_DRIFT",
        "RULESET_CONDITIONS_DRIFT",
        "BYPASS_ACTOR_DRIFT",
        "RULESET_VERSION_DRIFT",
        "REQUIRED_CONTEXT_PRODUCER_MISMATCH",
        "SEALED_AUTHORITY_DRIFT",
        "SEALED_AUTHORITY_UNAVAILABLE",
        "SEALED_PRIVILEGED_AUTHORITY_DRIFT",
    }
)


class RequiredContext(NamedTuple):
    name: str
    integration_id: int | None


class Observation(NamedTuple):
    context: str
    source: str
    evidence_id: int | None
    state: str
    integration_id: int | None = None
    commit_sha: str | None = None


class LiveSnapshot(NamedTuple):
    enforcement: str
    required_contexts: tuple[RequiredContext, ...]
    observations: tuple[Observation, ...]
    ruleset_projection: dict[str, Any] | None = None


class Blocker(NamedTuple):
    code: str
    message: str


class GateReport(NamedTuple):
    status: str
    repo: str
    commit_sha: str
    ruleset_id: int
    attempts: int
    contexts: tuple[dict[str, Any], ...]
    blockers: tuple[Blocker, ...]
    policy_sha256: str = ""
    policy_path: str = DEFAULT_POLICY_PATH

    def to_payload(self) -> dict[str, Any]:
        payload = dict(self._asdict())
        payload["blockers"] = [dict(item._asdict()) for item in self.blockers]
        payload["contexts"] = list(self.contexts)
        payload["decision"] = "GO" if self.status == "PASS" else "NO-GO"
        payload["repository"] = payload.pop("repo")
        payload["schema_version"] = 1
        return payload
