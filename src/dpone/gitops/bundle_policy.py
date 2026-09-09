from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class _BundlePolicy(Protocol):
    @property
    def verify_lock(self) -> bool: ...

    @property
    def fail_on_empty_impact(self) -> bool: ...

    @property
    def fail_on_warnings(self) -> bool: ...

    @property
    def require_lock(self) -> bool: ...


class _AffectedReport(Protocol):
    @property
    def impacted_manifests(self) -> tuple[object, ...]: ...


@dataclass(frozen=True, slots=True)
class GitOpsBundlePolicyBlocker:
    code: str
    message: str
    path: str
    source: str


class GitOpsBundlePolicyEvaluator:
    """Applies scheduler handoff policy gates to bundle inputs."""

    def evaluate(
        self,
        *,
        affected: _AffectedReport,
        policy: _BundlePolicy,
        warnings: tuple[object, ...],
    ) -> tuple[GitOpsBundlePolicyBlocker, ...]:
        blockers: list[GitOpsBundlePolicyBlocker] = []
        if policy.require_lock and not policy.verify_lock:
            blockers.append(
                GitOpsBundlePolicyBlocker(
                    code="lock_verification_required",
                    message="--require-lock requires --verify-lock so bundle verification checks plan digests",
                    path=".",
                    source="--require-lock",
                )
            )
        if policy.fail_on_empty_impact and not affected.impacted_manifests:
            blockers.append(
                GitOpsBundlePolicyBlocker(
                    code="empty_impact",
                    message="No manifests were impacted by the changed paths",
                    path=".",
                    source="--fail-on-empty-impact",
                )
            )
        if policy.fail_on_warnings and warnings:
            blockers.append(
                GitOpsBundlePolicyBlocker(
                    code="warnings_present",
                    message="Warnings were present and --fail-on-warnings was enabled",
                    path=".",
                    source="--fail-on-warnings",
                )
            )
        return tuple(blockers)


__all__ = ["GitOpsBundlePolicyBlocker", "GitOpsBundlePolicyEvaluator"]
