"""Canonical SS-46 ruleset projection and fail-closed live comparison."""

from __future__ import annotations

from typing import Any, NamedTuple


class ProjectionBlocker(NamedTuple):
    code: str
    message: str


def canonical_ruleset_projection(ruleset: Any) -> dict[str, Any]:
    """Return the non-base SS-46 projection used for A/B/C equality checks."""

    return {
        "bypass_actors": sorted(
            (
                {
                    "actor_id": actor.actor_id,
                    "actor_type": actor.actor_type,
                    "bypass_mode": actor.bypass_mode,
                }
                for actor in ruleset.bypass_actors
            ),
            key=lambda item: (item["actor_id"], item["actor_type"], item["bypass_mode"]),
        ),
        "conditions": {
            "exclude": list(ruleset.exclude),
            "include": list(ruleset.include),
        },
        "enforcement": ruleset.enforcement,
        "id": ruleset.id,
        "name": ruleset.name,
        "required_status_checks": {
            "checks": [
                {
                    "context": check.context,
                    "integration_id": check.integration_id,
                    "producer": {
                        "kind": check.producer_kind,
                        "workflow_id": check.workflow_id,
                        "workflow_path": check.workflow_path,
                    },
                }
                for check in ruleset.checks
            ],
            "strict": ruleset.strict_checks,
        },
        "target": ruleset.target,
        "version_id": ruleset.version_id,
    }


def compare_ruleset_projection(expected: dict[str, Any], live: dict[str, Any]) -> tuple[ProjectionBlocker, ...]:
    """Compare policy projection to a live ruleset payload; never invent IDs."""

    blockers: list[ProjectionBlocker] = []
    if live.get("id") != expected["id"]:
        blockers.append(ProjectionBlocker("RULESET_ID_DRIFT", "Live ruleset id differs from frozen policy."))
    if live.get("version_id") != expected["version_id"]:
        blockers.append(
            ProjectionBlocker("RULESET_VERSION_DRIFT", "Live ruleset version_id differs from frozen policy.")
        )
    if live.get("target") != expected["target"]:
        blockers.append(ProjectionBlocker("RULESET_TARGET_DRIFT", "Live ruleset target differs from frozen policy."))
    if live.get("enforcement") != expected["enforcement"]:
        blockers.append(
            ProjectionBlocker("RULESET_ENFORCEMENT_DRIFT", "Live ruleset enforcement differs from frozen policy.")
        )
    if _normalized_conditions(live.get("conditions")) != expected["conditions"]:
        blockers.append(
            ProjectionBlocker("RULESET_CONDITIONS_DRIFT", "Live ruleset conditions differ from frozen policy.")
        )
    if _normalized_actors(live.get("bypass_actors")) != expected["bypass_actors"]:
        blockers.append(ProjectionBlocker("BYPASS_ACTOR_DRIFT", "Live bypass actors differ from frozen policy."))
    blockers.extend(_check_blockers(expected["required_status_checks"], live.get("required_status_checks")))
    return tuple(blockers)


def _check_blockers(expected: dict[str, Any], live_raw: Any) -> list[ProjectionBlocker]:
    if not isinstance(live_raw, dict):
        return [
            ProjectionBlocker(
                "REQUIRED_CONTEXTS_UNAVAILABLE",
                "Live ruleset required_status_checks are unavailable.",
            )
        ]
    blockers: list[ProjectionBlocker] = []
    expected_checks = {item["context"]: item for item in expected["checks"]}
    live_checks_raw = live_raw.get("checks")
    if not isinstance(live_checks_raw, list):
        return [
            ProjectionBlocker(
                "REQUIRED_CONTEXTS_UNAVAILABLE",
                "Live ruleset required checks are unavailable.",
            )
        ]
    live_checks: dict[str, dict[str, Any]] = {}
    for item in live_checks_raw:
        if not isinstance(item, dict) or not isinstance(item.get("context"), str):
            blockers.append(ProjectionBlocker("REQUIRED_CONTEXT_INVALID", "Live required check entry is invalid."))
            continue
        live_checks[item["context"]] = item
    if set(live_checks) != set(expected_checks):
        blockers.append(
            ProjectionBlocker(
                "RULESET_POLICY_DRIFT",
                "Live required-context names differ from frozen policy.",
            )
        )
    for context, expected_check in sorted(expected_checks.items()):
        live_check = live_checks.get(context)
        if live_check is None:
            continue
        if live_check.get("integration_id") != expected_check["integration_id"]:
            blockers.append(
                ProjectionBlocker(
                    "REQUIRED_CONTEXT_PRODUCER_MISMATCH",
                    f"Required context '{context}' integration_id differs from frozen policy.",
                )
            )
            continue
        live_producer = live_check.get("producer")
        if not isinstance(live_producer, dict):
            blockers.append(
                ProjectionBlocker(
                    "REQUIRED_CONTEXT_PRODUCER_UNBOUND",
                    f"Required context '{context}' has no producer identity.",
                )
            )
            continue
        expected_producer = expected_check["producer"]
        if (
            live_producer.get("kind") != expected_producer["kind"]
            or live_producer.get("workflow_path") != expected_producer["workflow_path"]
            or live_producer.get("workflow_id") != expected_producer["workflow_id"]
        ):
            blockers.append(
                ProjectionBlocker(
                    "REQUIRED_CONTEXT_PRODUCER_MISMATCH",
                    f"Required context '{context}' producer identity differs from frozen policy.",
                )
            )
    return blockers


def _normalized_conditions(raw: Any) -> dict[str, list[str]] | None:
    if not isinstance(raw, dict):
        return None
    include = raw.get("include")
    exclude = raw.get("exclude")
    if not isinstance(include, list) or not isinstance(exclude, list):
        return None
    if not all(isinstance(item, str) for item in include + exclude):
        return None
    return {"exclude": list(exclude), "include": list(include)}


def _normalized_actors(raw: Any) -> list[dict[str, Any]] | None:
    if not isinstance(raw, list):
        return None
    actors: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            return None
        actor_id = item.get("actor_id")
        actor_type = item.get("actor_type")
        bypass_mode = item.get("bypass_mode")
        if not isinstance(actor_id, int) or isinstance(actor_id, bool) or actor_id <= 0:
            return None
        if not isinstance(actor_type, str) or not isinstance(bypass_mode, str):
            return None
        actors.append({"actor_id": actor_id, "actor_type": actor_type, "bypass_mode": bypass_mode})
    return sorted(actors, key=lambda item: (item["actor_id"], item["actor_type"], item["bypass_mode"]))
