"""Resolve declared and effective authority for one graph route."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from tools.agent_policy.workflow_privilege_contracts import (
    Authority,
    Finding,
    Route,
    RouteResolutionLimitError,
    TruthValue,
    finding,
    valid_public_text,
)
from tools.agent_policy.workflow_privilege_expressions import ExpressionLimitError, contains_secret_reference


def _permission_vocabulary(policy: Mapping[str, Any]) -> Mapping[str, tuple[str, ...] | list[str]]:
    value = policy.get("permission_access", {})
    return value if isinstance(value, Mapping) else {}


def _none_permissions(policy: Mapping[str, Any]) -> dict[str, str]:
    return {name: "none" for name in _permission_vocabulary(policy)}


def _declaration(
    value: object,
    policy: Mapping[str, Any],
    *,
    subject: str,
    route_id: str,
) -> tuple[dict[str, str], list[Finding], bool]:
    permissions = _none_permissions(policy)
    findings: list[Finding] = []
    unknown_seen = unknown_write = False
    if value in ("write-all", "read-all"):
        requested = str(value).removesuffix("-all")
        for name, access in _permission_vocabulary(policy).items():
            permissions[name] = (
                requested if requested in access else "read" if requested == "write" and "read" in access else "none"
            )
        findings.append(
            finding(
                f"PRIVILEGE_{requested.upper()}_ALL",
                subject=subject,
                route_id=route_id,
                detail=(
                    "write-all is forbidden on a PR-reachable route"
                    if requested == "write"
                    else "read-all violates the closed least-privilege boundary"
                ),
                policy=policy,
            )
        )
        return permissions, findings, requested == "write"
    if value is None:
        return permissions, findings, True
    if not isinstance(value, Mapping):
        findings.append(_unknown_permission(subject, route_id, policy))
        return permissions, findings, True
    for name, access in value.items():
        if name not in permissions or access not in _permission_vocabulary(policy).get(name, ()):
            unknown_seen = True
            unknown_write = unknown_write or access == "write"
            continue
        permissions[str(name)] = str(access)
    if unknown_seen:
        findings.append(_unknown_permission(subject, route_id, policy))
    return permissions, findings, unknown_write


def _unknown_permission(subject: str, route_id: str, policy: Mapping[str, Any]) -> Finding:
    return finding(
        "PRIVILEGE_UNKNOWN_PERMISSION",
        subject=subject,
        route_id=route_id,
        detail="permission name or access is outside the closed vocabulary",
        policy=policy,
    )


def _job_mapping(workflows: Mapping[str, Mapping[str, Any]], workflow: str, job_id: str) -> Mapping[str, Any]:
    jobs = workflows[workflow].get("jobs", {})
    value = jobs.get(job_id, {}) if isinstance(jobs, Mapping) else {}
    return value if isinstance(value, Mapping) else {}


def _declared_for_job(
    workflows: Mapping[str, Mapping[str, Any]],
    workflow: str,
    job_id: str,
) -> tuple[object, str]:
    job = _job_mapping(workflows, workflow, job_id)
    if job.get("permissions") is not None:
        return job.get("permissions"), "JOB"
    return workflows[workflow].get("permissions"), "WORKFLOW"


def _intersect(left: Mapping[str, str], right: Mapping[str, str]) -> dict[str, str]:
    rank = {"none": 0, "read": 1, "write": 2}
    return {name: min((left[name], right[name]), key=rank.__getitem__) for name in left}


def _runner(value: object, policy: Mapping[str, Any]) -> dict[str, Any]:
    labels = [value] if isinstance(value, str) else list(value) if isinstance(value, (list, tuple)) else []
    normalized = sorted(set(labels)) if all(isinstance(label, str) for label in labels) else []
    if not normalized or any("${{" in label for label in normalized):
        classification = "UNKNOWN"
    elif any(label.casefold() == "self-hosted" for label in normalized):
        classification = "SELF_HOSTED"
    elif tuple(normalized) not in _approved_hosted_runners(policy):
        classification = "UNKNOWN"
    else:
        classification = "GITHUB_HOSTED"
    return {"classification": classification, "labels": normalized}


def _approved_hosted_runners(policy: Mapping[str, Any]) -> set[tuple[str, ...]]:
    profiles = policy.get("profiles", {})
    values = profiles.values() if isinstance(profiles, Mapping) else ()
    approved: set[tuple[str, ...]] = {("windows-latest",)}
    for profile in values:
        runner = profile.get("runner", {}) if isinstance(profile, Mapping) else {}
        labels = runner.get("labels") if isinstance(runner, Mapping) else None
        if runner.get("classification") == "GITHUB_HOSTED" and isinstance(labels, list):
            approved.add(tuple(sorted(label for label in labels if isinstance(label, str))))
    return approved


def _declared_secret_kind(job: Mapping[str, Any]) -> str:
    declared = job.get("secrets", "NONE")
    if isinstance(declared, Mapping):
        return "EXPLICIT"
    normalized = str(declared)
    if normalized not in {"NONE", "EXPLICIT", "INHERIT", "UNKNOWN"}:
        normalized = "UNKNOWN"
    return "EXPLICIT" if contains_secret_reference(job) else normalized


def _secret_kind(
    route: Mapping[str, Any],
    workflows: Mapping[str, Mapping[str, Any]],
) -> str:
    coordinates = [(str(route["workflow"]), str(route["job_id"]))]
    coordinates.extend(
        (str(edge["source_workflow"]), str(edge["source_job"]))
        for edge in route.get("edge_chain", ())
        if edge.get("kind") == "LOCAL_WORKFLOW_CALL" and edge.get("source_job") is not None
    )
    kinds: list[str] = []
    for workflow, job_id in coordinates:
        job = _job_mapping(workflows, workflow, job_id)
        kinds.append(_declared_secret_kind(job))
        if contains_secret_reference(workflows[workflow].get("env")):
            kinds.append("EXPLICIT")
    if "INHERIT" in kinds:
        return "INHERIT"
    if "EXPLICIT" in kinds:
        return "EXPLICIT"
    return "UNKNOWN" if "UNKNOWN" in kinds else "NONE"


def resolve_authority(
    route: Mapping[str, Any],
    workflows: Mapping[str, Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> Authority:
    """Resolve one route without consulting mutable repository defaults."""

    route_id = str(route["route_id"])
    workflow = str(route["workflow"])
    job_id = str(route["job_id"])
    subject = f"{workflow}::{job_id}"
    declared_value, source = _declared_for_job(workflows, workflow, job_id)
    declared, findings, unknown_write = _declaration(
        declared_value,
        policy,
        subject=subject,
        route_id=route_id,
    )
    effective = dict(declared)
    oidc_write = effective.get("id-token") == "write"
    local_edges = [edge for edge in route.get("edge_chain", ()) if edge.get("kind") == "LOCAL_WORKFLOW_CALL"]
    if local_edges:
        source = "CALL_INTERSECTION"
        for edge in local_edges:
            caller_value, _ = _declared_for_job(
                workflows,
                str(edge["source_workflow"]),
                str(edge["source_job"]),
            )
            ceiling, ceiling_findings, ceiling_unknown_write = _declaration(
                caller_value,
                policy,
                subject=f"{edge['source_workflow']}::{edge['source_job']}",
                route_id=route_id,
            )
            findings.extend(ceiling_findings)
            unknown_write = unknown_write or ceiling_unknown_write
            oidc_write = oidc_write or ceiling.get("id-token") == "write"
            effective = _intersect(effective, ceiling)

    job = _job_mapping(workflows, workflow, job_id)
    runner = _runner(job.get("runs-on", job.get("runs_on")), policy)
    if isinstance(environment := job.get("environment"), Mapping):
        name = environment.get("name")
        environment = name if isinstance(name, str) and name else "UNKNOWN"
    elif environment is not None and not valid_public_text(environment, 256):
        environment = "UNKNOWN"
    secrets = _secret_kind(route, workflows)
    if environment is not None or secrets != "NONE":
        findings.append(
            finding(
                "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT",
                subject=subject,
                route_id=route_id,
                detail="PR-reachable job has secret or environment authority",
                policy=policy,
            )
        )
    if runner["classification"] != "GITHUB_HOSTED":
        findings.append(
            finding(
                "PRIVILEGE_PR_SELF_HOSTED",
                subject=subject,
                route_id=route_id,
                detail="PR-reachable runner is self-hosted or unknown",
                policy=policy,
            )
        )
    privileged = (
        unknown_write
        or oidc_write
        or "write" in effective.values()
        or environment is not None
        or secrets != "NONE"
        or runner["classification"] != "GITHUB_HOSTED"
        or declared_value is None
    )
    ordered_findings = tuple(sorted(findings, key=lambda item: (item.code, item.subject, item.detail)))
    return Authority(
        _source_witness=(ordered_findings, privileged),
        findings=ordered_findings,
        route_id=route_id,
        workflow=workflow,
        workflow_name=str(workflows[workflow].get("name", "")),
        job_id=job_id,
        classification=str(route["classification"]),
        declared_permissions=declared,
        effective_permissions=effective,
        permission_source=source,
        runner=runner,
        environment=environment,
        secrets=secrets,
        privileged=privileged,
    )


class ResolutionAudit:
    """Own trusted reachability, authority, and finding decisions around a resolver."""

    def __init__(
        self,
        candidates: tuple[Route, ...],
        workflows: Mapping[str, Mapping[str, Any]],
        policy: Mapping[str, Any],
        *,
        condition_for_route: Callable[[Route], TruthValue],
    ) -> None:
        self._candidates, self._candidate_set = candidates, frozenset(candidates)
        self._workflows, self._policy = workflows, policy
        self._condition_for_route = condition_for_route
        self._approved_profiles = frozenset((str(v["workflow"]), str(v["job"])) for v in policy["profiles"].values())
        self._conditions: dict[Route, TruthValue] = {}
        self._authorities: dict[Route, Authority] = {}

    def _accept(self, route: Route, recorded: Mapping[Route, object]) -> None:
        if route not in self._candidate_set or route in recorded:
            raise ValueError("route resolution call set drifted")

    def condition(self, route: Route) -> TruthValue:
        self._accept(route, self._conditions)
        value = self._condition_for_route(route)
        if not isinstance(value, TruthValue):
            raise ValueError("route resolution returned an invalid truth value")
        self._conditions[route] = value
        return value

    def authority(self, route: Route) -> Authority:
        self._accept(route, self._authorities)
        value = resolve_authority(route.to_mapping(), self._workflows, self._policy)
        source, privileged = value._source_witness or (None, None)
        if source != value.findings or privileged is not value.privileged:
            raise ValueError("authority decision differs from its source witness")
        self._authorities[route] = value
        return value

    def _pairs(self) -> tuple[tuple[Route, Authority], ...]:
        if (
            len(self._candidate_set) != len(self._candidates)
            or set(self._conditions) != self._candidate_set
            or set(self._authorities) != self._candidate_set
        ):
            raise ValueError("route resolution omitted or duplicated source candidates")
        pairs: list[tuple[Route, Authority]] = []
        for candidate in self._candidates:
            route = (
                replace(candidate, classification="PROVEN_NOT_PR_REACHABLE")
                if self._conditions[candidate] is TruthValue.FALSE
                else candidate
            )
            authority = replace(self._authorities[candidate], route_id=route.route_id)
            if self._conditions[candidate] is TruthValue.FALSE:
                authority = replace(authority, classification=route.classification, findings=())
            pairs.append((route, authority))
        return tuple(sorted(pairs, key=lambda item: item[0].route_id))

    def validate(self, routes: tuple[Route, ...], authorities: tuple[Authority, ...]) -> None:
        expected = self._pairs()
        if routes != tuple(item[0] for item in expected) or authorities != tuple(item[1] for item in expected):
            raise ValueError("route resolution output drifted from trusted source decisions")

    def emit_findings(self, sink: Callable[[Finding], None]) -> None:
        for candidate in self._candidates:
            if candidate not in self._conditions or candidate not in self._authorities:
                continue
            condition, authority = self._conditions[candidate], self._authorities[candidate]
            if condition is TruthValue.FALSE or self._external(candidate):
                continue
            for item in authority.findings:
                sink(item)
            if condition is TruthValue.UNKNOWN and authority.privileged:
                sink(_route_finding("PRIVILEGE_UNKNOWN_EXPRESSION", candidate, self._policy))
            if (
                condition is TruthValue.TRUE
                and authority.privileged
                and (candidate.workflow, candidate.job_id) not in self._approved_profiles
            ):
                sink(_route_finding("PRIVILEGE_UNAPPROVED_PR_WRITE", candidate, self._policy))

    def _external(self, route: Route) -> bool:
        uses = _job_mapping(self._workflows, route.workflow, route.job_id).get("uses")
        return isinstance(uses, str) and not uses.startswith("./.github/workflows/")


def _route_finding(code: str, route: Route, policy: Mapping[str, Any]) -> Finding:
    detail = {
        "PRIVILEGE_UNKNOWN_EXPRESSION": "privileged route condition cannot be proven",
        "PRIVILEGE_UNAPPROVED_PR_WRITE": "PR-reachable privileged authority is not a closed profile",
    }[code]
    return finding(
        code, subject=f"{route.workflow}::{route.job_id}", route_id=route.route_id, detail=detail, policy=policy
    )


def resolve_routes(
    candidates: tuple[Route, ...],
    *,
    condition_for_route: Callable[[Route], TruthValue],
    authority_for_route: Callable[[Route], Authority],
) -> tuple[tuple[Route, ...], tuple[Authority, ...]]:
    """Resolve ordered route authority using injected reachability decisions."""

    routes: list[Route] = []
    authorities: list[Authority] = []
    for candidate in candidates:
        try:
            condition = condition_for_route(candidate)
        except ExpressionLimitError as exc:
            raise RouteResolutionLimitError(exc.dimension, ()) from exc
        provisional = Route(
            candidate.root_index,
            candidate.event_variant,
            candidate.edge_chain,
            candidate.workflow,
            candidate.job_id,
            candidate.classification,
        )
        authority = authority_for_route(provisional)
        if condition is TruthValue.FALSE:
            route = replace(provisional, classification="PROVEN_NOT_PR_REACHABLE")
            authority = replace(authority, route_id=route.route_id, classification=route.classification, findings=())
        else:
            route = provisional
            authority = replace(authority, route_id=route.route_id)
        routes.append(route)
        authorities.append(authority)
    ordered = sorted(zip(routes, authorities, strict=True), key=lambda item: item[0].route_id)
    return tuple(item[0] for item in ordered), tuple(item[1] for item in ordered)


__all__ = ["ResolutionAudit", "resolve_authority", "resolve_routes"]
