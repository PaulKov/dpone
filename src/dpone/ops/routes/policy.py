"""Generic route readiness scoring and blocker policy."""

from __future__ import annotations

from dpone.ops.routes.models import RouteEvidenceItem, RouteProfile, RouteReadinessDecision


class RouteReadinessPolicy:
    """Evaluate route readiness from a profile and normalized evidence."""

    def evaluate(
        self,
        *,
        profile: RouteProfile,
        evidence: tuple[RouteEvidenceItem, ...],
    ) -> RouteReadinessDecision:
        required_names = tuple(dict.fromkeys(profile.required_evidence))
        by_name = {item.name: item for item in evidence}
        required_items = tuple(by_name.get(name) for name in required_names)
        passed_required = sum(1 for item in required_items if item is not None and item.passed)
        score = round((passed_required / len(required_names)) * 100.0, 2) if required_names else 100.0
        blockers = _blockers(required_names, by_name, profile)
        warnings = tuple(
            f"{item.name}.not_passed" for item in evidence if not item.required and not item.passed and not item.missing
        )
        return RouteReadinessDecision(
            passed=not blockers,
            level=_level(profile=profile, score=score, blockers=blockers),
            score=score,
            blockers=blockers,
            warnings=warnings,
            next_actions=_next_actions(blockers),
        )


def _blockers(
    required_names: tuple[str, ...],
    by_name: dict[str, RouteEvidenceItem],
    profile: RouteProfile,
) -> tuple[str, ...]:
    result: list[str] = []
    if profile.certification_status == "not_supported":
        result.append(f"route.not_supported:{profile.key.colon_id}")
    for name in required_names:
        item = by_name.get(name)
        if item is None:
            result.append(f"{name}.missing")
            continue
        if not item.passed:
            result.extend(item.blockers or (f"{name}.not_passed",))
    return tuple(dict.fromkeys(result))


def _level(*, profile: RouteProfile, score: float, blockers: tuple[str, ...]) -> str:
    if profile.certification_status == "not_supported":
        return "unknown"
    if not blockers:
        return "certified"
    if score >= 80.0:
        return "degraded"
    return "blocked"


def _next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        name, _, suffix = blocker.partition(".")
        if suffix == "missing":
            actions.append(f"Provide or regenerate required route evidence `{name}`.")
        elif blocker.startswith("route.not_supported"):
            actions.append("Add the route to the integration matrix before requesting readiness.")
        else:
            actions.append(f"Open the source artifact for `{name}` and follow its runbook.")
    return tuple(dict.fromkeys(actions))
