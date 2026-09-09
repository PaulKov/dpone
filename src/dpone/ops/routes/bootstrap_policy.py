"""Pure policy helpers for route onboarding reports."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ops.routes.bootstrap_models import ArtifactSummary, OnboardingCheck, OnboardingStatus


from collections.abc import Sequence


def checks_score(checks: Sequence[OnboardingCheck]) -> float:
    """Return a simple percentage score for normalized checks."""

    required = tuple(check for check in checks if check.required)
    scored = required or tuple(checks)
    if not scored:
        return 100.0
    passed = sum(1 for check in scored if check.passed)
    return round((passed / len(scored)) * 100.0, 2)


def artifacts_score(artifacts: Sequence[ArtifactSummary]) -> float:
    """Return a simple percentage score for route doctor artifacts."""

    required = tuple(artifact for artifact in artifacts if artifact.required)
    scored = required or tuple(artifacts)
    if not scored:
        return 100.0
    passed = sum(1 for artifact in scored if artifact.passed)
    return round((passed / len(scored)) * 100.0, 2)


def status_from_blockers(blockers: Sequence[str], warnings: Sequence[str]) -> OnboardingStatus:
    """Classify one onboarding report."""

    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "ready"


def checks_blockers(checks: Sequence[OnboardingCheck]) -> tuple[str, ...]:
    """Return fail-closed blockers for required failed checks."""

    return tuple(dict.fromkeys(f"{check.name}.missing" for check in checks if check.required and not check.passed))


def checks_warnings(checks: Sequence[OnboardingCheck]) -> tuple[str, ...]:
    """Return warnings for optional failed checks."""

    return tuple(
        dict.fromkeys(f"{check.name}.optional_missing" for check in checks if not check.required and not check.passed)
    )


def artifact_blockers(artifacts: Sequence[ArtifactSummary]) -> tuple[str, ...]:
    """Return blockers from required missing or failed upstream artifacts."""

    blockers: list[str] = []
    for artifact in artifacts:
        if artifact.required and artifact.missing:
            blockers.append(f"{artifact.name}.missing")
        elif artifact.required and not artifact.passed:
            blockers.append(f"{artifact.name}.not_passed")
        blockers.extend(artifact.blockers)
    return tuple(dict.fromkeys(blockers))


def artifact_warnings(artifacts: Sequence[ArtifactSummary]) -> tuple[str, ...]:
    """Return warnings from optional artifacts."""

    warnings: list[str] = []
    for artifact in artifacts:
        if not artifact.required and artifact.missing:
            warnings.append(f"{artifact.name}.optional_missing")
        warnings.extend(artifact.warnings)
    return tuple(dict.fromkeys(warnings))


def next_actions_for_blockers(blockers: Sequence[str], *, ready_action: str) -> tuple[str, ...]:
    """Translate blockers into operator actions."""

    if not blockers:
        return (ready_action,)
    actions: list[str] = []
    for blocker in blockers:
        if blocker.endswith(".missing"):
            actions.append(f"Attach or generate `{blocker.removesuffix('.missing')}` evidence and rerun the command.")
        elif blocker.endswith(".not_passed"):
            actions.append(f"Open `{blocker.removesuffix('.not_passed')}` evidence and resolve failed checks.")
        else:
            actions.append(f"Resolve `{blocker}` before continuing.")
    return tuple(dict.fromkeys(actions))
