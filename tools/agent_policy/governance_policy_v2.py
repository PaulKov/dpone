"""Strict parser and typed views for dpone.github-governance-policy.v2."""

from __future__ import annotations

import re
from typing import Any, NamedTuple

SCHEMA = "dpone.github-governance-policy.v2"
PLACEHOLDER = re.compile(r"(?i)<[^>]*unverified[^>]*>|\bunverified\b")
ROOT_KEYS = frozenset({"schema", "schema_version", "github_api_version", "branch_governance", "release_trust"})


class StatusCheck(NamedTuple):
    context: str
    integration_id: int
    producer_kind: str
    workflow_path: str
    workflow_id: int


class BypassActor(NamedTuple):
    actor_id: int
    actor_type: str
    bypass_mode: str


class RulesetView(NamedTuple):
    id: int
    version_id: int
    name: str
    target: str
    source_type: str
    source: str
    enforcement: str
    include: tuple[str, ...]
    exclude: tuple[str, ...]
    bypass_actors: tuple[BypassActor, ...]
    checks: tuple[StatusCheck, ...]
    strict_checks: bool


class BranchGovernancePolicyView(NamedTuple):
    owner: str
    last_reviewed: str
    mode: str
    purpose: str
    ruleset: RulesetView
    owner_attestation_required: bool
    evidence_required: tuple[str, ...]

    @property
    def context_names(self) -> tuple[str, ...]:
        return tuple(item.context for item in self.ruleset.checks)


class RepositoryView(NamedTuple):
    name_with_owner: str
    repository_id: int
    canonical_remote_url: str
    protected_base_ref: str
    candidate_workflow_path: str


class ReleaseTrustPolicyView(NamedTuple):
    repository: RepositoryView
    tag_ruleset: RulesetView
    immutable_releases_required: bool
    controller_repository: str
    controller_workflow_path: str
    evidence_store_id: str


class GovernancePolicy(NamedTuple):
    schema_version: int
    github_api_version: str
    branch_view: BranchGovernancePolicyView
    release_view: ReleaseTrustPolicyView


def parse_governance_policy(payload: dict[str, Any]) -> GovernancePolicy:
    """Parse one governance-policy v2 mapping into immutable typed views."""

    if not isinstance(payload, dict):
        raise ValueError("governance policy must be a mapping")
    if payload.get("schema_version") != 2:
        raise ValueError("schema_version must be 2")
    if payload.get("schema") != SCHEMA:
        raise ValueError(f"schema must be {SCHEMA}")
    unknown = sorted(set(payload) - ROOT_KEYS)
    if unknown:
        raise ValueError(f"unknown root keys: {', '.join(unknown)}")
    api_version = _require_str(payload, "github_api_version")
    _reject_placeholders(payload)
    branch = _parse_branch(payload.get("branch_governance"))
    release = _parse_release(payload.get("release_trust"), branch.ruleset)
    return GovernancePolicy(2, api_version, branch, release)


def _parse_branch(raw: Any) -> BranchGovernancePolicyView:
    data = _mapping(raw, "branch_governance")
    mode = _require_str(data, "mode")
    if mode not in {"solo_maintainer", "multi_reviewer"}:
        raise ValueError("branch_governance.mode is invalid")
    attestation = _mapping(data.get("owner_attestation"), "owner_attestation")
    evidence = attestation.get("evidence_required")
    if not isinstance(evidence, list) or not evidence or not all(isinstance(item, str) and item for item in evidence):
        raise ValueError("owner_attestation.evidence_required must be a non-empty string list")
    return BranchGovernancePolicyView(
        owner=_require_str(data, "owner"),
        last_reviewed=_require_str(data, "last_reviewed"),
        mode=mode,
        purpose=_require_str(data, "purpose"),
        ruleset=_parse_ruleset(data.get("ruleset"), allowed_targets={"branch"}),
        owner_attestation_required=bool(attestation.get("required_when_no_independent_reviewer")),
        evidence_required=tuple(str(item) for item in evidence),
    )


def _parse_release(raw: Any, branch_ruleset: RulesetView) -> ReleaseTrustPolicyView:
    del branch_ruleset  # release view reuses branch objects at cutover; fixture supplies tag ruleset.
    data = _mapping(raw, "release_trust")
    repository = _mapping(data.get("repository"), "repository")
    controller = _mapping(data.get("trusted_controller"), "trusted_controller")
    immutable = _mapping(data.get("immutable_releases"), "immutable_releases")
    evidence = _mapping(data.get("evidence"), "evidence")
    return ReleaseTrustPolicyView(
        repository=RepositoryView(
            name_with_owner=_require_str(repository, "name_with_owner"),
            repository_id=_positive_int(repository, "repository_id"),
            canonical_remote_url=_require_str(repository, "canonical_remote_url"),
            protected_base_ref=_require_str(repository, "protected_base_ref"),
            candidate_workflow_path=_require_str(repository, "candidate_workflow_path"),
        ),
        tag_ruleset=_parse_ruleset(data.get("tag_ruleset"), allowed_targets={"tag"}, allow_empty_checks=True),
        immutable_releases_required=bool(immutable.get("required")),
        controller_repository=_require_str(controller, "repository"),
        controller_workflow_path=_require_str(controller, "workflow_path"),
        evidence_store_id=_require_str(evidence, "store_id"),
    )


def _parse_ruleset(raw: Any, *, allowed_targets: set[str], allow_empty_checks: bool = False) -> RulesetView:
    data = _mapping(raw, "ruleset")
    target = _require_str(data, "target")
    if target not in allowed_targets:
        raise ValueError(f"ruleset.target must be one of {sorted(allowed_targets)}")
    conditions = _mapping(data.get("conditions"), "conditions")
    include = _string_tuple(conditions.get("include"), "conditions.include")
    exclude = _string_tuple(conditions.get("exclude"), "conditions.exclude", allow_empty=True)
    actors_raw = data.get("bypass_actors")
    if not isinstance(actors_raw, list):
        raise ValueError("bypass_actors must be a list")
    actors = tuple(_parse_actor(item) for item in actors_raw)
    checks_block = data.get("required_status_checks")
    if allow_empty_checks and checks_block is None:
        checks: tuple[StatusCheck, ...] = ()
        strict = True
    else:
        checks_data = _mapping(checks_block, "required_status_checks")
        strict = bool(checks_data.get("strict"))
        checks = _parse_checks(checks_data.get("checks"))
    return RulesetView(
        id=_positive_int(data, "id"),
        version_id=_positive_int(data, "version_id"),
        name=_require_str(data, "name"),
        target=target,
        source_type=_require_str(data, "source_type"),
        source=_require_str(data, "source"),
        enforcement=_require_str(data, "enforcement"),
        include=include,
        exclude=exclude,
        bypass_actors=actors,
        checks=checks,
        strict_checks=strict,
    )


def _parse_checks(raw: Any) -> tuple[StatusCheck, ...]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("required_status_checks.checks must be a non-empty list")
    checks: list[StatusCheck] = []
    seen: set[str] = set()
    for item in raw:
        data = _mapping(item, "check")
        context = _require_str(data, "context")
        if context in seen:
            raise ValueError(f"duplicate required context: {context}")
        seen.add(context)
        producer = _mapping(data.get("producer"), "producer")
        checks.append(
            StatusCheck(
                context=context,
                integration_id=_positive_int(data, "integration_id"),
                producer_kind=_require_str(producer, "kind"),
                workflow_path=_require_str(producer, "workflow_path"),
                workflow_id=_positive_int(producer, "workflow_id"),
            )
        )
    return tuple(checks)


def _parse_actor(raw: Any) -> BypassActor:
    data = _mapping(raw, "bypass_actor")
    return BypassActor(
        actor_id=_positive_int(data, "actor_id"),
        actor_type=_require_str(data, "actor_type"),
        bypass_mode=_require_str(data, "bypass_mode"),
    )


def _reject_placeholders(value: Any, *, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _reject_placeholders(item, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _reject_placeholders(item, path=f"{path}[{index}]")
        return
    if isinstance(value, str) and PLACEHOLDER.search(value):
        raise ValueError(f"placeholder value is forbidden at {path}")


def _mapping(raw: Any, label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be a mapping")
    return raw


def _require_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _positive_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def _string_tuple(raw: Any, label: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(raw, list) or (not raw and not allow_empty):
        raise ValueError(f"{label} must be a string list")
    if not all(isinstance(item, str) and item.strip() and item == item.strip() for item in raw):
        raise ValueError(f"{label} entries must be non-empty strings")
    return tuple(raw)
