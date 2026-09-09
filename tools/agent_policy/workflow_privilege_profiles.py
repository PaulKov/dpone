"""Match PR-reachable authority only against the three closed profiles."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from tools.agent_policy import workflow_privilege_contracts as _contracts
from tools.agent_policy.workflow_privilege_policy_selection import report_profile_binding
from tools.agent_policy.workflow_privilege_profile_support import nested_mapping as _nested_mapping

_CI = ".github/workflows/ci.yml"
_PR_VARIANTS = frozenset({"ACTIVITY:opened", "ACTIVITY:reopened", "ACTIVITY:synchronize"})
_JsonObject = Mapping[str, Any]
_JsonSequence = Sequence[_JsonObject]
_Workflows = Mapping[str, _JsonObject]
_GENERATOR_MARKERS = tuple(
    "tools/agent_policy/governance_gate.py test_artifacts/agent-policy/agent_governance_gate.json".split()
)
_REPORT_PROFILE_BINDINGS = {
    "CODEQL_PR_UPLOAD": (
        ".github/workflows/codeql.yml",
        "analyze",
        "PR_HEAD",
        "WORKFLOW",
        {"contents": "read", "security-events": "write"},
        "36a5ff903eab36601bd3cfe3636a53497a9d72cd7ca71ce39ef9c19a8aef3c88",
        _PR_VARIANTS,
        (),
    ),
    "ADR0037_GOVERNANCE_SOURCE_ATTESTOR": (
        _CI,
        "governance-attestation",
        "PR_HEAD",
        "JOB",
        {"actions": "read", "attestations": "write", "contents": "read", "id-token": "write"},
        "c5832e92a84ddd4eaa44cbf009d45eb7de2c4a7c3bcfc4258da1702b5aa0b3af",
        _PR_VARIANTS,
        (
            ("NEEDS", _CI, "quality", _CI, "governance-source"),
            ("NEEDS", _CI, "governance-source", _CI, "governance-attestation"),
        ),
    ),
    "ADR0037_MERGED_CLOSURE_CHECK_PUBLISHER": (
        ".github/workflows/agent-pr-receipt.yml",
        "merge-closure",
        "POST_MERGE_INTEGRATED_CODE",
        "JOB",
        {"actions": "read", "checks": "write", "contents": "read"},
        "92c783c76af4f23ccbce06892fd297f73c830120a030b739a8b05055dec406f3",
        frozenset({"CLOSED_MERGED"}),
        (),
    ),
}


def _mapping(value: object) -> dict[str, Any]:
    converter = getattr(value, "to_mapping", None)
    converted = value if isinstance(value, Mapping) else converter() if callable(converter) else None
    if not isinstance(converted, Mapping):
        raise TypeError("profile input must be a mapping or expose to_mapping")
    return dict(converted)


def _drift(name: str, policy: _JsonObject, detail: str, *, route_id: str | None = None) -> _contracts.Finding:
    profile = policy["profiles"][name]
    return _contracts.finding(
        "PRIVILEGE_CODEQL_PROFILE_DRIFT" if name == "codeql" else "PRIVILEGE_ADR0037_PROFILE_DRIFT",
        subject=f"{profile['workflow']}::{profile['job']}",
        detail=detail,
        route_id=route_id,
        policy=policy,
    )


def _envelope(workflow: _JsonObject, job: _JsonObject) -> dict[str, Any]:
    return {
        "workflow_permissions": workflow.get("permissions") or {},
        "workflow_env": workflow.get("env") or {},
        "workflow_defaults": workflow.get("defaults") or {},
        "job": dict(job),
    }


def _profile_value(name: str, workflow: _JsonObject, job: _JsonObject, profile: _JsonObject) -> dict[str, Any]:
    if name != "codeql":
        return _envelope(workflow, job)
    return {
        "workflow": profile["workflow"],
        "events": dict(_nested_mapping(workflow, "on")),
        "permissions": workflow.get("permissions", {}),
        "jobs": dict(_nested_mapping(workflow, "jobs")),
    }


def _producer_counts(job: object) -> tuple[int, int]:
    steps = job.get("steps", ()) if isinstance(job, Mapping) else ()
    if not isinstance(steps, Sequence) or isinstance(steps, (str, bytes)):
        return (0, 0)
    uploads = sum(
        1
        for step in steps
        if isinstance(step, Mapping)
        and (
            step.get("id") == "governance-upload"
            or isinstance(values := step.get("with"), Mapping)
            and (
                values.get("name") == "agent-governance-gate"
                or values.get("path") == "test_artifacts/agent-policy/agent_governance_gate.json"
            )
        )
    )
    generates = any(
        marker in str(step.get("run", ""))
        for step in steps
        if isinstance(step, Mapping)
        for marker in _GENERATOR_MARKERS
    )
    return (int(bool(uploads) or generates), uploads)


def _governance_producer_value(job: object) -> dict[str, Any] | None:
    if not isinstance(job, Mapping) or set(job) != {"needs", "runs-on", "permissions", "outputs", "steps"}:
        return None
    uploads = [
        step for step in job.get("steps", ()) if isinstance(step, Mapping) and step.get("id") == "governance-upload"
    ]
    if len(uploads) != 1 or set(uploads[0]) != {"name", "id", "uses", "with"}:
        return None
    upload = uploads[0]
    return {
        "job": "governance-source",
        **{key: job.get(key) for key in ("needs", "runs-on", "permissions")},
        "artifact": {
            "step_id": upload.get("id"),
            **{key: upload.get(key) for key in ("uses", "with")},
            "outputs": job.get("outputs"),
        },
    }


def _profile_fingerprint(name: str, workflows: _Workflows, policy: _JsonObject) -> tuple[str | None, bool]:
    profile = policy["profiles"][name]
    workflow = workflows.get(profile["workflow"])
    if not isinstance(workflow, Mapping):
        return None, False
    jobs = _nested_mapping(workflow, "jobs")
    job = jobs.get(profile["job"])
    if (
        not isinstance(job, Mapping)
        or _contracts.canonical_sha256(_nested_mapping(workflow, "on")) != profile["trigger_sha256"]
    ):
        return None, False
    if name == "codeql":
        digest = _contracts.canonical_sha256(_profile_value(name, workflow, job, profile))
        allowed = {"path", "name", "on", "permissions", "jobs", "env", "defaults"}
        closed = set(workflow) <= allowed and not (workflow.get("env") or workflow.get("defaults"))
        return digest, digest == profile["semantic_sha256"] and closed
    if name == "governance_source":
        producer = jobs.get("governance-source")
        producer_value = _governance_producer_value(producer)
        if producer_value is None:
            return None, False
        digest = _contracts.canonical_sha256(_envelope(workflow, job))
        valid = (
            _contracts.canonical_sha256(producer_value) == profile["producer_sha256"]
            and digest == profile["envelope_sha256"]
        )
        return digest, valid
    digest = _contracts.canonical_sha256(_envelope(workflow, job))
    return digest, digest == profile["envelope_sha256"]


def _closed_profile_matches(
    route: _JsonObject, authority: _JsonObject, profile: _JsonObject, policy: _JsonObject
) -> bool:
    expected_classification = (
        "POST_MERGE_INTEGRATED_CODE" if profile["id"] == "ADR0037_MERGED_CLOSURE_CHECK_PUBLISHER" else "PR_HEAD"
    )
    declared = dict.fromkeys(policy["permission_access"], "none") | profile["declared_non_none"]
    effective = dict.fromkeys(policy["permission_access"], "none") | profile["effective_non_none"]
    return (
        route.get("workflow") == profile["workflow"]
        and route.get("job_id") == profile["job"]
        and route.get("event_variant") in profile["event_variants"]
        and [edge.get("kind") for edge in route.get("edge_chain", ())] == profile["edge_kinds"]
        and route.get("edge_chain") == profile["required_edge_chain"]
        and all(
            authority.get(field) == route.get(field) for field in ("route_id", "workflow", "job_id", "classification")
        )
        and authority.get("classification") == expected_classification
        and authority.get("permission_source") == profile["permission_source"]
        and authority.get("declared_permissions") == declared
        and authority.get("effective_permissions") == effective
        and authority.get("runner") == profile["runner"]
        and authority.get("environment") == profile["environment"]
        and authority.get("secrets") == profile["secrets"]
    )


def _candidate_name(route: _JsonObject, policy: _JsonObject) -> str | None:
    for name, profile in policy["profiles"].items():
        if route.get("workflow") == profile["workflow"] and route.get("job_id") == profile["job"]:
            return str(name)
    return None


def match_profile(
    route: object,
    authority: object,
    workflows: _Workflows,
    policy: _JsonObject,
) -> _contracts.MatchResult:
    """Match one route and its complete authority against a closed profile."""

    route_value, authority_value = _mapping(route), _mapping(authority)
    name = _candidate_name(route_value, policy)
    if name is None:
        return _contracts.MatchResult()
    profile = policy["profiles"][name]
    fingerprint, valid_fingerprint = _profile_fingerprint(name, workflows, policy)
    if not (valid_fingerprint and _closed_profile_matches(route_value, authority_value, profile, policy)):
        finding = _drift(
            name,
            policy,
            "closed profile identity or envelope drifted",
            route_id=str(route_value["route_id"]),
        )
        return _contracts.MatchResult(findings=(finding,))
    match = _contracts.ProfileMatch(
        route_id=str(route_value["route_id"]),
        id=str(profile["id"]),
        workflow=str(profile["workflow"]),
        job_id=str(profile["job"]),
        fingerprint=str(fingerprint),
        classification=str(authority_value["classification"]),
    )
    return _contracts.MatchResult(match=match)


def _producer_inventory(workflows: _Workflows) -> tuple[int, int]:
    counts = tuple(
        _producer_counts(job) for workflow in workflows.values() for job in _nested_mapping(workflow, "jobs").values()
    )
    return sum(item[0] for item in counts), sum(item[1] for item in counts)


def _structural_occurrences(name: str, workflows: _Workflows, profile: _JsonObject) -> int:
    expected_digest = profile.get("semantic_sha256") if name == "codeql" else profile.get("envelope_sha256")
    return sum(
        _contracts.canonical_sha256(_profile_value(name, workflow, job, profile)) == expected_digest
        for workflow in workflows.values()
        for job in _nested_mapping(workflow, "jobs").values()
        if isinstance(job, Mapping)
    )


def _subject_inventory_findings(workflows: _Workflows, policy: _JsonObject) -> list[_contracts.Finding]:
    return [
        _drift(str(name), policy, "mandatory profile subject, trigger, or fingerprint drifted")
        for name, profile in policy["profiles"].items()
        if _structural_occurrences(str(name), workflows, profile) != profile["required_occurrences"]
        or name == "governance_source"
        and _producer_inventory(workflows) != (profile["required_occurrences"],) * 2
        or not _profile_fingerprint(name, workflows, policy)[1]
    ]


def validate_mandatory_profiles(
    workflows: _Workflows,
    routes: Sequence[object],
    authorities: Sequence[object],
    policy: Mapping[str, Any],
) -> _contracts.ProfileResult:
    """Validate exact subject and route-coordinate multisets for every profile."""

    route_values = [_mapping(route) for route in routes]
    authority_values = {_mapping(item).get("route_id"): _mapping(item) for item in authorities}
    findings = _subject_inventory_findings(workflows, policy)
    matches: list[_contracts.ProfileMatch] = []
    for route in route_values:
        name = _candidate_name(route, policy)
        if name is None:
            continue
        authority = authority_values.get(route.get("route_id"))
        if authority is None:
            findings.append(
                _drift(name, policy, "profile route has no authority record", route_id=str(route["route_id"]))
            )
            continue
        result = match_profile(route, authority, workflows, policy)
        findings.extend(result.findings)
        if result.match is not None:
            matches.append(result.match)

    variants = {route["route_id"]: str(route["event_variant"]) for route in route_values}
    observed = Counter((match.id, variants[match.route_id]) for match in matches)
    for name, profile in policy["profiles"].items():
        actual = Counter({key: count for key, count in observed.items() if key[0] == profile["id"]})
        if actual != Counter((profile["id"], variant) for variant in profile["event_variants"]):
            findings.append(_drift(str(name), policy, "mandatory profile route coordinates drifted"))
    unique_findings = {tuple(item.to_mapping().items()): item for item in findings}
    return _contracts.ProfileResult(
        tuple(sorted(unique_findings.values(), key=_contracts.finding_key)),
        tuple(sorted(matches, key=lambda item: (item.id, item.route_id))),
    )


def validate_report_profile_bindings(
    roots: _JsonSequence,
    routes: Mapping[str, _JsonObject],
    authority_values: _JsonSequence,
    match_values: _JsonSequence,
    *,
    status: str,
    policy_version: int | None,
) -> None:
    """Reject serialized authority/profile evidence that weakens matcher output."""

    authorities = {item["route_id"]: item for item in authority_values}
    matches = {item["route_id"]: item for item in match_values}
    if (
        len(authorities) != len(authority_values)
        or set(authorities) != set(routes)
        or authority_values
        != sorted(authority_values, key=lambda item: (item["route_id"], item["workflow"].encode(), item["job_id"]))
        or len(matches) != len(match_values)
        or match_values != sorted(match_values, key=lambda item: (item["id"], item["route_id"]))
    ):
        raise ValueError("serialized authority or profile evidence is not canonical and bijective")
    for route_id, route in routes.items():
        authority = authorities[route_id]
        if any(authority.get(field) != route.get(field) for field in ("workflow", "job_id", "classification")):
            raise ValueError("route and authority coordinates disagree")
        if status == "PASS" and authority.get("profile_id") is None and _serialized_authority_is_privileged(authority):
            raise ValueError("PASS contains unprofiled privileged authority")
    expected = {
        route_id: authority["profile_id"]
        for route_id, authority in authorities.items()
        if authority.get("profile_id") is not None
    }
    if {route_id: match.get("id") for route_id, match in matches.items()} != expected:
        raise ValueError("profile evidence is not bijective")
    for route_id, match in matches.items():
        route, authority = routes[route_id], authorities[route_id]
        binding = report_profile_binding(
            policy_version, match.get("id"), _REPORT_PROFILE_BINDINGS.get(str(match.get("id")))
        )
        if binding is None:
            raise ValueError("serialized profile binding drifted")
        workflow, job, classification, source, non_none, fingerprint, variants, expected_edges = binding
        permissions = {key: "none" for key in authority["declared_permissions"]}
        permissions.update(non_none)
        edge_keys = ("kind", "source_workflow", "source_job", "target_workflow", "target_job")
        edges = tuple(tuple(edge[key] for key in edge_keys) for edge in route["edge_chain"])
        if not (
            roots[route["root_index"]] == {"workflow": workflow, "event": "pull_request"}
            and (route["workflow"], route["job_id"], route["classification"]) == (workflow, job, classification)
            and route["event_variant"] in variants
            and edges == expected_edges
            and (match["workflow"], match["job_id"], match["classification"], match["fingerprint"])
            == (workflow, job, classification, fingerprint)
            and authority["permission_source"] == source
            and authority["declared_permissions"] == authority["effective_permissions"] == permissions
            and authority["runner"] == {"classification": "GITHUB_HOSTED", "labels": ["ubuntu-latest"]}
            and authority["environment"] is None
            and authority["secrets"] == "NONE"
        ):
            raise ValueError("serialized profile binding drifted")


def mandatory_profiles_complete(
    matches: tuple[_contracts.ProfileMatch, ...], routes: list[_contracts.Route], policy: _JsonObject
) -> bool:
    variants = {route.route_id: route.event_variant for route in routes}
    return Counter((match.id, variants.get(match.route_id)) for match in matches) == Counter(
        (profile["id"], variant) for profile in policy["profiles"].values() for variant in profile["event_variants"]
    )


def _serialized_authority_is_privileged(authority: _JsonObject) -> bool:
    access = {*authority.get("declared_permissions", {}).values(), *authority.get("effective_permissions", {}).values()}
    return authority.get("classification") != "PROVEN_NOT_PR_REACHABLE" and (
        "write" in access
        or authority.get("secrets") != "NONE"
        or authority.get("environment") is not None
        or authority.get("runner", {}).get("classification") != "GITHUB_HOSTED"
    )


__all__ = (
    "mandatory_profiles_complete match_profile validate_mandatory_profiles validate_report_profile_bindings".split()
)
