"""Build legacy release-ruleset projections from checked-out YAML policy."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from typing import Any

MAX_POLICY_BYTES = 1024 * 1024


def _load_live_ruleset() -> Any:
    path = Path(__file__).with_name("governance_live_ruleset.py")
    spec = importlib.util.spec_from_file_location("dpone_release_yaml_live_ruleset", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load governance_live_ruleset.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _bounded_policy(path: Path) -> tuple[dict[str, Any], bytes]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("branch-protection policy must be one regular file")
    size = path.stat().st_size
    if size <= 0 or size > MAX_POLICY_BYTES:
        raise ValueError("branch-protection policy size is outside its closed limit")
    with path.open("rb") as stream:
        raw = stream.read(MAX_POLICY_BYTES + 1)
    if len(raw) != size or len(raw) > MAX_POLICY_BYTES:
        raise ValueError("branch-protection policy changed while read or exceeded its limit")
    try:
        import yaml
    except ImportError as exc:
        raise ValueError("branch-protection policy parser is unavailable") from exc
    try:
        payload = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeError, yaml.YAMLError) as exc:
        raise ValueError("branch-protection policy must be valid UTF-8 YAML") from exc
    if not isinstance(payload, dict):
        raise ValueError("branch-protection policy must be one YAML mapping")
    return payload, raw


def policy_projection(
    path: Path,
    *,
    ruleset_id: int,
    expected_checks: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    """Build the exact authorized live projection from legacy YAML bytes."""

    payload, raw = _bounded_policy(path)
    ruleset = payload.get("ruleset")
    if payload.get("schema_version") != 1 or not isinstance(ruleset, dict):
        raise ValueError("branch-protection policy schema is unsupported")
    if ruleset.get("id") != ruleset_id:
        raise ValueError("branch-protection policy ruleset id differs from release evidence")
    version_id = ruleset.get("version_id")
    if isinstance(version_id, bool) or not isinstance(version_id, int) or version_id <= 0:
        raise ValueError("branch-protection policy ruleset version id is invalid")
    name = ruleset.get("name")
    updated_at = _load_live_ruleset().canonical_ruleset_updated_at(ruleset.get("updated_at"))
    target = ruleset.get("target")
    enforcement = ruleset.get("enforcement")
    conditions = _string_lists(ruleset.get("conditions"), label="ruleset conditions")
    pull_request = _mapping(ruleset.get("pull_request"), label="pull-request policy")
    status_checks = _mapping(ruleset.get("required_status_checks"), label="status-check policy")
    protected_actions = _mapping(ruleset.get("protected_actions"), label="protected-action policy")
    bypass = _mapping(ruleset.get("bypass"), label="bypass policy")
    if not isinstance(name, str) or not name or target != "branch" or enforcement != "active" or updated_at is None:
        raise ValueError("branch-protection policy live ruleset identity is invalid")
    policy_contexts = status_checks.get("checks")
    integration_id = status_checks.get("integration_id")
    if (
        not isinstance(policy_contexts, list)
        or not all(isinstance(item, str) and item for item in policy_contexts)
        or isinstance(integration_id, bool)
        or not isinstance(integration_id, int)
        or integration_id <= 0
    ):
        raise ValueError("branch-protection policy required-check identities are invalid")
    authorized_checks = sorted(
        ({"context": context, "integration_id": integration_id} for context in policy_contexts),
        key=lambda item: item["context"],
    )
    if authorized_checks != expected_checks:
        raise ValueError("required-check receipt producer identities differ from frozen policy")
    pull_parameters = _pull_parameters(pull_request)
    if pull_request.get("required") is not True:
        raise ValueError("frozen policy must require pull requests")
    if protected_actions.get("block_deletion") is not True or protected_actions.get("block_force_push") is not True:
        raise ValueError("frozen policy must block deletion and force pushes")
    required_parameters = {
        "do_not_enforce_on_create": _boolean(status_checks, "do_not_enforce_on_create"),
        "required_status_checks": authorized_checks,
        "strict_required_status_checks_policy": _boolean(status_checks, "strict"),
    }
    rules: list[dict[str, Any]] = [
        {"parameters": {}, "type": "deletion"},
        {"parameters": {}, "type": "non_fast_forward"},
        {"parameters": pull_parameters, "type": "pull_request"},
        {"parameters": required_parameters, "type": "required_status_checks"},
    ]
    projection = {
        "bypass_actors": _actors(bypass.get("actors")),
        "conditions": conditions,
        "enforcement": enforcement,
        "id": ruleset_id,
        "name": name,
        "required_status_checks": {
            "checks": authorized_checks,
            "strict": required_parameters["strict_required_status_checks_policy"],
        },
        "rules": sorted(rules, key=lambda rule: str(rule["type"])),
        "target": target,
        "updated_at": updated_at,
        "version_id": version_id,
    }
    return projection, hashlib.sha256(raw).hexdigest()


def _pull_parameters(pull_request: dict[str, Any]) -> dict[str, Any]:
    return {
        "allowed_merge_methods": _string_list(pull_request.get("allowed_merge_methods"), "merge methods"),
        "dismiss_stale_reviews_on_push": _boolean(pull_request, "dismiss_stale_reviews_on_push"),
        "require_code_owner_review": _boolean(pull_request, "require_code_owner_review"),
        "require_extra_approval_for_unattributed_changes": _boolean(
            pull_request,
            "require_extra_approval_for_unattributed_changes",
        ),
        "require_last_push_approval": _boolean(pull_request, "require_last_push_approval"),
        "required_approving_review_count": _non_negative_int(
            pull_request,
            "required_approving_review_count",
        ),
        "required_review_thread_resolution": _boolean(
            pull_request,
            "required_review_thread_resolution",
        ),
        "required_reviewers": _empty_list(pull_request.get("required_reviewers"), "required reviewers"),
    }


def _mapping(raw: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be a mapping")
    return raw


def _string_lists(raw: Any, *, label: str) -> dict[str, list[str]]:
    data = _mapping(raw, label=label)
    return {
        "exclude": _string_list(data.get("exclude"), f"{label} exclude", allow_empty=True),
        "include": _string_list(data.get("include"), f"{label} include"),
    }


def _string_list(raw: Any, label: str, *, allow_empty: bool = False) -> list[str]:
    if not isinstance(raw, list) or (not raw and not allow_empty):
        raise ValueError(f"{label} must be a string list")
    if not all(isinstance(item, str) and item and item == item.strip() for item in raw):
        raise ValueError(f"{label} entries must be non-empty strings")
    if len(set(raw)) != len(raw):
        raise ValueError(f"{label} entries must be unique")
    return sorted(raw)


def _actors(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError("bypass actors must be a list")
    actors: list[dict[str, Any]] = []
    for item in raw:
        actor = _mapping(item, label="bypass actor")
        actor_id = actor.get("actor_id")
        actor_type = actor.get("actor_type")
        bypass_mode = actor.get("bypass_mode")
        if (
            isinstance(actor_id, bool)
            or not isinstance(actor_id, int)
            or actor_id <= 0
            or not isinstance(actor_type, str)
            or not actor_type
            or not isinstance(bypass_mode, str)
            or not bypass_mode
        ):
            raise ValueError("bypass actor identity is invalid")
        actors.append({"actor_id": actor_id, "actor_type": actor_type, "bypass_mode": bypass_mode})
    return sorted(actors, key=lambda item: (item["actor_id"], item["actor_type"], item["bypass_mode"]))


def _boolean(data: dict[str, Any], key: str) -> bool:
    value = data.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be boolean")
    return value


def _non_negative_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return value


def _empty_list(raw: Any, label: str) -> list[Any]:
    if raw != []:
        raise ValueError(f"{label} must be empty")
    return []


__all__ = ["policy_projection"]
