"""Closed validation and deterministic rendering for PR3B reports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from heapq import heappop, heappush, heapreplace
from itertools import chain
from typing import Any

from jsonschema import Draft202012Validator
from tools.agent_policy import workflow_privilege_contracts as _contracts
from tools.agent_policy.workflow_privilege_profiles import validate_report_profile_bindings

_EDGE_KEYS = ("kind", "source_workflow", "source_job", "target_workflow", "target_job")
_JsonObject = Mapping[str, Any]
_JsonSequence = Sequence[_JsonObject]
_EMPTY_REPORT_INPUTS: tuple[Any, ...] = ({}, (), (), (), (), ())
_COUNT_FIELDS = "workflow_count job_count edge_count root_count route_count".split()
_EVIDENCE_FIELDS = "inventory roots route_authority privileged_profiles findings".split()
_COUNT_DIMENSIONS = dict(zip(_COUNT_FIELDS, "workflow_files jobs graph_edges roots routes".split(), strict=True))
_SUMMARY = "status={} complete={} workflows={} jobs={} edges={} roots={} routes={}"
_RUNBOOK = "runbook=docs/cicd/runbooks.md#semantic-pr-privilege-boundary"
_RECHECK = "recheck=uv run python tools/agent_policy/workflow_security_privileged.py --root . --format text"


class InternalReportError(ValueError): ...


class ReportSizeError(InternalReportError): ...


class _Descending(tuple[Any, ...]):
    def __new__(cls, item: _contracts.Finding) -> _Descending:
        return tuple.__new__(cls, (*_contracts.finding_key(item), item.status, item.recovery_command_id))

    def __lt__(self, other: _Descending) -> bool:
        return tuple.__gt__(self, other)


class FindingAccumulator:
    def __init__(self, values: Sequence[_contracts.Finding] = ()) -> None:
        self._heap: list[_Descending] = []
        self._items: dict[_Descending, _contracts.Finding] = {}
        self._overflow = False
        for item in values:
            self.add(item)

    def _retain(self, item: _contracts.Finding, capacity: int) -> None:
        key = _Descending(item)
        if key in self._items:
            return
        if len(self._heap) < capacity:
            heappush(self._heap, key)
        elif capacity and tuple(key) < tuple(self._heap[0]):
            del self._items[heapreplace(self._heap, key)]
        else:
            return
        self._items[key] = item

    def add(self, item: _contracts.Finding) -> None:
        maximum = _contracts.V1_LIMITS.findings
        if item == _contracts.RESOURCE_LIMIT_FINDING:
            if not self._overflow and len(self._heap) == maximum:
                del self._items[heappop(self._heap)]
            self._overflow = True
            return
        key = _Descending(item)
        if key in self._items:
            return
        if not self._overflow and len(self._heap) == maximum:
            self._overflow = True
            del self._items[heappop(self._heap)]
        self._retain(item, maximum - int(self._overflow))

    def finish(self) -> tuple[tuple[_contracts.Finding, ...], bool]:
        retained = (*self._items.values(), *((_contracts.RESOURCE_LIMIT_FINDING,) if self._overflow else ()))
        return tuple(sorted(retained, key=lambda item: tuple(_Descending(item)))), self._overflow


def _invalid(condition: object) -> None:
    if condition:
        raise InternalReportError("report semantic invariant drifted")


_STRING_LIMITS = {
    **dict.fromkeys("workflow source_workflow target_workflow subject".split(), 1024),
    **dict.fromkeys("workflow_name job_id source_job target_job environment".split(), 256),
    **dict.fromkeys("event_variant code recovery_command_id labels".split(), 128),
    **dict.fromkeys("route_id fingerprint sha256 manifest_sha256".split(), 64),
    "detail": 2048,
}


def _string_envelope(value: object, limit: int | None = None, *, detail: bool = False) -> bool:
    if isinstance(value, str):
        return _contracts.valid_public_text(value, limit, allow_layout=detail)
    if isinstance(value, Mapping):
        return all(
            _string_envelope(key) and _string_envelope(item, _STRING_LIMITS.get(key), detail=key == "detail")
            for key, item in value.items()
        )
    if isinstance(value, list):
        return all(_string_envelope(item, limit, detail=detail) for item in value)
    return True


def _canonical_bytes(value: object, *, max_bytes: int | None = None) -> bytes:
    try:
        return _contracts.canonical_json_bytes(value, max_bytes=max_bytes)
    except _contracts.CanonicalSizeError as exc:
        raise ReportSizeError("canonical report exceeds its closed byte limit") from exc
    except (TypeError, ValueError) as exc:
        raise InternalReportError("value is not canonical JSON") from exc


def canonical_json_bytes(report: _JsonObject) -> bytes:
    limit = report.get("limits", {}).get("report_bytes") if isinstance(report, Mapping) else None
    _invalid(type(limit) is not int)
    return _canonical_bytes(report, max_bytes=limit - 1) + b"\n"


def canonical_finding_json(finding: _JsonObject) -> str:
    return _canonical_bytes(finding).decode("ascii")


def canonical_findings(values: Sequence[_contracts.Finding]) -> tuple[_contracts.Finding, ...]:
    return FindingAccumulator(values).finish()[0]


def build_report(
    snapshot: _contracts.Snapshot,
    reference: _contracts.SnapshotReference,
    workflows: Mapping[str, _JsonObject],
    edges: Sequence[Any],
    roots: Sequence[Any],
    routes: Sequence[Any],
    authorities: Sequence[_contracts.Authority],
    matches: Sequence[Any],
    findings: Sequence[_contracts.Finding],
    *,
    inventory_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one canonical-ready report without duplicating policy in services."""

    ordered_findings = canonical_findings(findings)
    declared_dimensions = (inventory_overrides or {}).get("overflow_dimensions", ())
    selected_overflow = FindingAccumulator(findings).finish()[1]
    finding_overflow = "findings" in declared_dimensions or (
        selected_overflow and _contracts.RESOURCE_LIMIT_FINDING not in findings
    )
    status = "PASS" if not ordered_findings else "FAIL" if ordered_findings[0].status == "FAIL" else "UNVERIFIED"
    inventory: dict[str, Any] = {
        "complete": reference.complete,
        "manifest_sha256": reference.manifest_sha256,
        "workflow_count": snapshot.workflow_count,
        "job_count": sum(len(value.get("jobs", {})) for value in workflows.values()),
        "edge_count": len(edges),
        "root_count": len(roots),
        "route_count": len(routes),
        "overflow_dimensions": list(snapshot.overflow_dimensions),
    }
    inventory.update(inventory_overrides or {})
    dimensions = inventory["overflow_dimensions"]
    if finding_overflow:
        inventory.update(complete=False, manifest_sha256=None)
    inventory["overflow_dimensions"] = sorted({*dimensions, *(("findings",) if finding_overflow else ())})
    return {
        "schema_version": 1,
        "status": status,
        "ok": status == "PASS",
        "root": ".",
        "policy": {
            "path": snapshot.policy.path
            if snapshot.policy is not None
            else ".agents/policy/workflow-security-privileged.yml",
            "sha256": reference.policy_sha256,
            "schema_version": reference.policy_schema_version,
        },
        "inventory": inventory,
        "roots": [item.to_mapping() for item in roots],
        "routes": [item.to_mapping() for item in routes],
        "route_authority": [authority.to_report_mapping() for authority in authorities],
        "privileged_profiles": [item.to_mapping() for item in matches],
        "findings": [item.to_mapping() for item in ordered_findings],
        "limits": asdict(_contracts.V1_LIMITS),
    }


def build_minimal_report(
    snapshot: _contracts.Snapshot,
    reference: _contracts.SnapshotReference,
    findings: Sequence[_contracts.Finding],
    inventory_overrides: _JsonObject | None = None,
) -> dict[str, Any]:

    failure = next((item for item in canonical_findings(findings) if item.status == "FAIL"), None)
    retained = tuple(item for item in (failure, _contracts.RESOURCE_LIMIT_FINDING) if item is not None)
    return build_report(snapshot, reference, *_EMPTY_REPORT_INPUTS, retained, inventory_overrides=inventory_overrides)


def render_text(report: _JsonObject) -> str:
    inventory, findings = report["inventory"], report["findings"]
    limit = report.get("limits", {}).get("text_stdout_bytes")
    _invalid(type(limit) is not int)
    summary = _SUMMARY.format(
        report["status"], str(inventory["complete"]).lower(), *(inventory[key] for key in _COUNT_FIELDS)
    )
    lines = chain(
        (summary, f"finding_count={len(findings)}"),
        (f"finding[{index:04d}]={canonical_finding_json(item)}" for index, item in enumerate(findings, 1)),
        (_RUNBOOK, _RECHECK),
    )
    rendered = bytearray()
    for line in lines:
        encoded = f"{line}\n".encode()
        if len(rendered) + len(encoded) > limit:
            raise ReportSizeError("text report exceeds its closed byte limit")
        rendered.extend(encoded)
    return rendered.decode()


def validate_report(report: _JsonObject, schema: _JsonObject) -> None:
    try:
        Draft202012Validator.check_schema(schema)
        _invalid(tuple(Draft202012Validator(schema).iter_errors(report)))
        _invalid(not _string_envelope(report))
        _validate_semantics(report)
        canonical_json_bytes(report)
    except InternalReportError:
        raise
    except Exception as exc:
        raise InternalReportError("report violates a semantic invariant") from exc


def finalize_report(
    report: dict[str, Any],
    *,
    snapshot_reference: _contracts.SnapshotReference,
    canonical_route_ids: Sequence[str],
    evidence: _contracts.ReportEvidence,
    schema: _JsonObject,
) -> dict[str, Any]:
    policy, inventory = report.get("policy"), report.get("inventory")
    _invalid(
        not isinstance(policy, Mapping)
        or not isinstance(inventory, Mapping)
        or policy.get("sha256") != snapshot_reference.policy_sha256
        or policy.get("schema_version") != snapshot_reference.policy_schema_version
        or inventory.get("manifest_sha256") != snapshot_reference.manifest_sha256
        or inventory.get("complete") is not snapshot_reference.complete
    )
    expected_ids, report_ids = list(canonical_route_ids), [route.get("route_id") for route in report.get("routes", ())]
    identities = tuple(_contracts.canonical_sha256(report.get(section)) for section in _EVIDENCE_FIELDS)
    _invalid(
        expected_ids != sorted(set(expected_ids)) or report_ids != expected_ids or identities != evidence.identities
    )
    _invalid(evidence.requires_nonpass and report.get("status") == "PASS")
    validate_report(report, schema)
    return report


def _validate_semantics(report: _JsonObject) -> None:
    inventory, policy, limits = report["inventory"], report["policy"], report["limits"]
    routes, roots, overflow = report["routes"], report["roots"], inventory["overflow_dimensions"]
    profile_evidence = report["route_authority"], report["privileged_profiles"]
    findings, root_coordinates = report["findings"], [(item["workflow"], item["event"]) for item in roots]
    output_fallback = bool({"report_bytes", "text_stdout_bytes"} & set(overflow))
    resource, statuses = _contracts.RESOURCE_LIMIT_FINDING.to_mapping(), tuple(item["status"] for item in findings)
    minimal = findings[-1:] == [resource] and statuses in (("UNVERIFIED",), ("FAIL", "UNVERIFIED"))
    _invalid(
        type(report["schema_version"]) is not int
        or report["ok"] is not (report["status"] == "PASS")
        or policy["schema_version"] is not None
        and (type(policy["schema_version"]) is not int or policy["sha256"] is None)
        or inventory["complete"]
        and policy["sha256"] is None
        or inventory["complete"] is not (inventory["manifest_sha256"] is not None)
        or inventory["complete"]
        and overflow
        or inventory["complete"]
        and (inventory["root_count"] != len(roots) or inventory["route_count"] != len(routes))
        or overflow != sorted(set(overflow))
        or any(
            type(inventory[count]) is not int
            or (inventory[count] == limits[dimension] + 1) is not (dimension in overflow)
            for count, dimension in _COUNT_DIMENSIONS.items()
        )
        or any(type(value) is not int for value in limits.values())
        or bool(overflow) is not any(item["code"] == "PRIVILEGE_RESOURCE_LIMIT" for item in findings)
        or roots != sorted(roots, key=lambda item: (item["workflow"].encode(), item["event"]))
        or len(set(root_coordinates)) != len(roots)
        or output_fallback
        and (any((*roots, *routes, *profile_evidence)) or not minimal)
    )
    route_map = _validate_routes(routes, roots, limits)
    validate_report_profile_bindings(
        roots, route_map, *profile_evidence, status=report["status"], policy_version=policy["schema_version"]
    )
    _validate_inventory_lower_bounds(inventory, roots, routes)
    _validate_findings(findings, report["status"])
    _invalid(
        report["status"] == "PASS"
        and (
            not inventory["complete"]
            or None in (policy["sha256"], policy["schema_version"])
            or not inventory["workflow_count"]
            or findings
            or overflow
            or any(root["event"] == "pull_request_target" for root in roots)
            or inventory["route_count"] < inventory["root_count"]
            or {route["root_index"] for route in routes} != set(range(len(roots)))
        )
    )


def _validate_inventory_lower_bounds(inventory: _JsonObject, roots: _JsonSequence, routes: _JsonSequence) -> None:
    chain = tuple(edge for route in routes for edge in route["edge_chain"])
    workflows = {item["workflow"] for item in (*roots, *routes)} | {
        edge[field] for edge in chain for field in ("source_workflow", "target_workflow")
    }
    jobs = {(route["workflow"], route["job_id"]) for route in routes} | {
        (edge[f"{side}_workflow"], edge[f"{side}_job"])
        for edge in chain
        for side in ("source", "target")
        if edge[f"{side}_job"] is not None
    }
    edges = {tuple(edge[key] for key in _EDGE_KEYS) for edge in chain}
    bounds = zip(_COUNT_FIELDS, (workflows, jobs, edges, roots, routes), strict=True)
    _invalid(any(inventory[field] < len(coordinates) for field, coordinates in bounds))


def _validate_routes(routes: _JsonSequence, roots: _JsonSequence, limits: Mapping[str, int]) -> dict[str, _JsonObject]:
    route_map: dict[str, _JsonObject] = {}
    for route in routes:
        without_id = {key: value for key, value in route.items() if key != "route_id"}
        root_index = route["root_index"]
        _invalid(
            route["route_id"] != _contracts.canonical_route_id(without_id)
            or not 0 <= root_index < len(roots)
            or len(route["edge_chain"]) > limits["route_edges"]
        )
        _validate_chain(route, roots[root_index], limits)
        route_map[route["route_id"]] = route
    _invalid(len(route_map) != len(routes) or list(route_map) != sorted(route_map))
    return route_map


def _validate_chain(route: _JsonObject, root: _JsonObject, limits: Mapping[str, int]) -> None:
    current_workflow = root["workflow"]
    current_job: str | None = None
    seen_workflows = {current_workflow}
    seen_edges: set[tuple[object, ...]] = set()
    seen_jobs: set[str] = set()
    for edge in route["edge_chain"]:
        coordinate = tuple(edge[key] for key in _EDGE_KEYS)
        _invalid(coordinate in seen_edges or edge["source_workflow"] != current_workflow)
        kind = edge["kind"]
        if current_job is not None and kind in {"NEEDS", "LOCAL_WORKFLOW_CALL"} and edge["source_job"] != current_job:
            raise InternalReportError("route job chain is disconnected")
        _invalid(
            kind not in {"NEEDS", "LOCAL_WORKFLOW_CALL", "WORKFLOW_RUN"}
            or (edge["source_job"] is not None) is not (kind != "WORKFLOW_RUN")
            or (edge["target_job"] is not None) is not (kind == "NEEDS")
            or kind == "NEEDS"
            and edge["source_workflow"] != edge["target_workflow"]
            or kind != "NEEDS"
            and edge["target_workflow"] in seen_workflows
        )
        if kind == "NEEDS":
            _invalid(edge["target_job"] in {*seen_jobs, edge["source_job"]})
            seen_jobs.update((edge["source_job"], edge["target_job"]))
        else:
            seen_jobs.clear()
        seen_edges.add(coordinate)
        seen_workflows.add(edge["target_workflow"])
        current_workflow = edge["target_workflow"]
        current_job = edge["target_job"]
    local_count = sum(edge["kind"] == "LOCAL_WORKFLOW_CALL" for edge in route["edge_chain"])
    run_count = sum(edge["kind"] == "WORKFLOW_RUN" for edge in route["edge_chain"])
    _invalid(current_workflow != route["workflow"] or current_job not in (None, route["job_id"]))
    _invalid(
        local_count > limits["local_call_edges"] or len(seen_workflows) > limits["reusable_workflows_including_root"]
    )
    _invalid(run_count != int(">WORKFLOW_RUN:" in route["event_variant"]) or run_count > limits["workflow_run_edges"])


def _validate_findings(findings: Sequence[_JsonObject], status: str) -> None:
    _invalid(
        len(findings) != len({_canonical_bytes(item) for item in findings})
        or any(
            _contracts.REPORT_OUTCOMES.get(item["code"]) != (item["status"], item["recovery_command_id"])
            or len(item["detail"].encode("utf-8")) > 2048
            for item in findings
        )
    )
    expected = "FAIL" if any(item["status"] == "FAIL" for item in findings) else "UNVERIFIED" if findings else "PASS"
    _invalid(list(findings) != sorted(findings, key=_contracts.finding_key) or status != expected)
