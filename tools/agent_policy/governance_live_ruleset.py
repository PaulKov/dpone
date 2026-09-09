"""Normalize live GitHub ruleset API payloads into SS-46 projections."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def live_ruleset_projection(
    payload: dict[str, Any],
    *,
    ruleset_id: int | None = None,
) -> dict[str, Any]:
    """Build a canonical projection from one GitHub ruleset API object.

    Producer workflow path/id are not present on the ruleset API; checks carry
    only ``context`` and ``integration_id``. Missing producers fail closed when
    compared to a v2 policy that requires them.
    """

    if not isinstance(payload, dict):
        raise ValueError("live ruleset payload must be a mapping")
    resolved_id = _optional_positive_int(payload.get("id"))
    if resolved_id is None:
        resolved_id = ruleset_id
    if resolved_id is None:
        raise ValueError("GitHub ruleset id must be a positive integer")
    version_id = _version_id(payload)
    conditions = _conditions(payload.get("conditions"))
    actors = _bypass_actors(payload.get("bypass_actors"))
    checks = _required_checks(payload.get("rules"))
    return {
        # GitHub deliberately omits bypass actors for callers without write
        # permission.  Preserve that distinction; an omitted privileged field
        # is not evidence of an empty bypass list.
        "bypass_actors": actors,
        "conditions": conditions if conditions is not None else {"exclude": [], "include": []},
        "enforcement": str(payload.get("enforcement") or ""),
        "id": resolved_id,
        "name": str(payload.get("name") or ""),
        "required_status_checks": {
            "checks": checks,
            "strict": _strict_flag(payload.get("rules")),
        },
        "rules": _canonical_rules(payload.get("rules")),
        "target": str(payload.get("target") or ""),
        "updated_at": canonical_ruleset_updated_at(payload.get("updated_at")),
        "version_id": version_id,
    }


def observable_ruleset_projection(projection: dict[str, Any]) -> dict[str, Any]:
    """Return the exact ruleset surface observable by a read-only token.

    ``version_id`` is available only from the administration-only history API,
    and ``bypass_actors`` may be redacted for read-only callers.  The GitHub-
    managed ``updated_at`` value binds those privileged policy fields to the
    separately reviewed frozen baseline without granting release jobs an admin
    credential.
    """

    if not isinstance(projection, dict):
        raise ValueError("live ruleset projection must be a mapping")
    updated_at = canonical_ruleset_updated_at(projection.get("updated_at"))
    if updated_at is None:
        raise ValueError("GitHub ruleset updated_at is unavailable")
    required = (
        "conditions",
        "enforcement",
        "id",
        "name",
        "required_status_checks",
        "rules",
        "target",
    )
    if any(key not in projection for key in required):
        raise ValueError("live ruleset observable projection is incomplete")
    return {
        "conditions": _canonical_value(projection["conditions"]),
        "enforcement": projection["enforcement"],
        "id": projection["id"],
        "name": projection["name"],
        "required_status_checks": _canonical_value(projection["required_status_checks"]),
        "rules": _canonical_value(projection["rules"]),
        "target": projection["target"],
        "updated_at": updated_at,
    }


def canonical_ruleset_updated_at(value: Any) -> str | None:
    """Normalize one provider-managed ruleset revision timestamp to UTC."""

    if not isinstance(value, str) or not value or value != value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _canonical_rules(raw: Any) -> list[dict[str, Any]]:
    """Return the complete security-relevant rule surface in stable order."""

    if not isinstance(raw, list):
        raise ValueError("live ruleset rules must be a list")
    rules: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("live ruleset rule must be a mapping")
        rule_type = item.get("type")
        if not isinstance(rule_type, str) or not rule_type or rule_type in seen:
            raise ValueError("live ruleset rule types must be non-empty and unique")
        seen.add(rule_type)
        parameters = item.get("parameters", {})
        if not isinstance(parameters, dict):
            raise ValueError("live ruleset rule parameters must be a mapping")
        normalized = _canonical_value(parameters)
        if rule_type == "required_status_checks":
            checks = normalized.get("required_status_checks")
            if not isinstance(checks, list):
                raise ValueError("live ruleset required_status_checks must be a list")
            normalized["required_status_checks"] = sorted(checks, key=lambda check: str(check.get("context")))
        rules.append({"parameters": normalized, "type": rule_type})
    return sorted(rules, key=lambda rule: str(rule["type"]))


def _canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("live ruleset parameter keys must be strings")
        return {key: _canonical_value(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise ValueError("live ruleset parameters contain an unsupported value")


def _version_id(payload: dict[str, Any]) -> int | None:
    direct = _optional_positive_int(payload.get("version_id"))
    if direct is not None:
        return direct
    current = payload.get("current_version")
    if isinstance(current, dict):
        return _optional_positive_int(current.get("id"))
    return None


def _conditions(raw: Any) -> dict[str, list[str]] | None:
    if not isinstance(raw, dict):
        return None
    ref_name = raw.get("ref_name") if isinstance(raw.get("ref_name"), dict) else raw
    if not isinstance(ref_name, dict):
        return None
    include = ref_name.get("include")
    exclude = ref_name.get("exclude")
    if not isinstance(include, list) or not isinstance(exclude, list):
        return None
    if not all(isinstance(item, str) for item in include + exclude):
        return None
    return {"exclude": list(exclude), "include": list(include)}


def _bypass_actors(raw: Any) -> list[dict[str, Any]] | None:
    if not isinstance(raw, list):
        return None
    actors: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            return None
        actor_id = _optional_positive_int(item.get("actor_id"))
        actor_type = item.get("actor_type")
        bypass_mode = item.get("bypass_mode")
        if actor_id is None or not isinstance(actor_type, str) or not isinstance(bypass_mode, str):
            return None
        actors.append({"actor_id": actor_id, "actor_type": actor_type, "bypass_mode": bypass_mode})
    return sorted(actors, key=lambda item: (item["actor_id"], item["actor_type"], item["bypass_mode"]))


def _required_checks(rules: Any) -> list[dict[str, Any]]:
    if not isinstance(rules, list):
        raise ValueError("live ruleset rules must be a list")
    checks: list[dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
            continue
        parameters = rule.get("parameters")
        raw_checks = parameters.get("required_status_checks") if isinstance(parameters, dict) else None
        if not isinstance(raw_checks, list):
            raise ValueError("live ruleset required_status_checks must be a list")
        for check in raw_checks:
            if not isinstance(check, dict) or not isinstance(check.get("context"), str):
                raise ValueError("live required status check must have a context")
            context = check["context"].strip()
            if not context:
                raise ValueError("live required status check context must not be empty")
            entry: dict[str, Any] = {
                "context": context,
                "integration_id": _optional_positive_int(check.get("integration_id")),
            }
            checks.append(entry)
    return sorted(checks, key=lambda item: item["context"])


def _strict_flag(rules: Any) -> bool | None:
    if not isinstance(rules, list):
        return None
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
            continue
        parameters = rule.get("parameters")
        if isinstance(parameters, dict):
            return bool(parameters.get("strict_required_status_checks_policy"))
    return None


def _positive_int(value: Any, label: str) -> int:
    parsed = _optional_positive_int(value)
    if parsed is None:
        raise ValueError(f"GitHub {label} must be a positive integer")
    return parsed


def _optional_positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


__all__ = [
    "canonical_ruleset_updated_at",
    "live_ruleset_projection",
    "observable_ruleset_projection",
]
