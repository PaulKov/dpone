"""Resume policy planning for orchestrated dpone runs."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.orchestration.state import OrchestrationJobState


@dataclass(frozen=True, slots=True)
class ResumeDecision:
    allowed: bool
    policy: str
    blockers: tuple[str, ...] = tuple()


class ResumePolicyPlanner:
    """Evaluates previous job state before a new orchestrated attempt."""

    _SUPPORTED_POLICIES = {"fail", "resume", "restart"}

    def decide(self, *, previous: OrchestrationJobState | None, policy: str) -> ResumeDecision:
        normalized_policy = policy if policy in self._SUPPORTED_POLICIES else "fail"
        if previous is None:
            return ResumeDecision(allowed=True, policy=normalized_policy)
        if previous.status == "committed" and normalized_policy != "restart":
            return ResumeDecision(
                allowed=False,
                policy=normalized_policy,
                blockers=("job_state.already_committed",),
            )
        if previous.status == "running":
            return ResumeDecision(
                allowed=False,
                policy=normalized_policy,
                blockers=("job_state.already_running",),
            )
        if previous.resumable and normalized_policy == "fail":
            return ResumeDecision(
                allowed=False,
                policy=normalized_policy,
                blockers=("job_state.resume_required",),
            )
        return ResumeDecision(allowed=True, policy=normalized_policy)
