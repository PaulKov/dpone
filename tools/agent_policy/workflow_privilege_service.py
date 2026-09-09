"""Application service composing the closed PR3B privilege proof."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

from tools.agent_policy import workflow_privilege_contracts as contracts
from tools.agent_policy import workflow_privilege_parser as parser
from tools.agent_policy import workflow_privilege_profiles as profiles_contract
from tools.agent_policy import workflow_privilege_report as report_contract
from tools.agent_policy.workflow_privilege_composition import REPORT_SCHEMA
from tools.agent_policy.workflow_privilege_expressions import RouteConditionProof
from tools.agent_policy.workflow_privilege_graph import build_graph, expand_routes
from tools.agent_policy.workflow_privilege_permissions import ResolutionAudit, resolve_routes
from tools.agent_policy.workflow_privilege_policy_selection import policy_binding_failure_for_lease
from tools.agent_policy.workflow_privilege_snapshot import SnapshotLease, snapshot_inventory

_POLICY_PATH = ".agents/policy/workflow-security-privileged.yml"
_CANONICAL_BUILD_GRAPH, _CANONICAL_EXPAND_ROUTES = build_graph, expand_routes
_CANONICAL_AUDIT_EMIT, _SOURCE_FINDING_KEY = ResolutionAudit.emit_findings, contracts.finding_key
_CANONICAL_VALIDATE_PROFILES, V1_LIMITS = profiles_contract.validate_mandatory_profiles, contracts.V1_LIMITS


def _source_finding_key(item: contracts.Finding) -> tuple[object, ...]:
    return (*_SOURCE_FINDING_KEY(item), item.status, item.recovery_command_id)


class _FindingSeal:
    def __init__(self, values: tuple[contracts.Finding, ...] = ()) -> None:
        self._observed = report_contract.FindingAccumulator()
        self._items: dict[tuple[object, ...], contracts.Finding] = {}
        self._overflow = False
        for item in values:
            self.add(item)

    def add(self, item: contracts.Finding) -> None:
        key = _source_finding_key(item)
        if item != contracts.RESOURCE_LIMIT_FINDING and key not in self._items:
            self._items[key] = item
        self._overflow |= item == contracts.RESOURCE_LIMIT_FINDING or len(self._items) > contracts.V1_LIMITS.findings
        while len(self._items) > contracts.V1_LIMITS.findings - int(self._overflow):
            self._items.pop(max(self._items))
        self._observed.add(item)

    def finish(self) -> tuple[tuple[contracts.Finding, ...], bool]:
        resources = (contracts.RESOURCE_LIMIT_FINDING,) if self._overflow else ()
        selected = tuple(sorted((*self._items.values(), *resources), key=_source_finding_key))
        if self._observed.finish() != (selected, self._overflow):
            raise report_contract.InternalReportError("finding selection differs from its accumulated source")
        return selected, self._overflow


def _emit_audit_findings(audit: ResolutionAudit, destination: _FindingSeal) -> None:
    expected, observed = _FindingSeal(), _FindingSeal()
    _CANONICAL_AUDIT_EMIT(audit, expected.add)
    audit.emit_findings(observed.add)
    selected = expected.finish()
    if observed.finish() != selected:
        raise report_contract.InternalReportError("authority findings differ from trusted route decisions")
    for item in selected[0]:
        destination.add(item)


class WorkflowPrivilegeService:
    """Compose injected parsing/evaluation boundaries for one repository scan."""

    def __init__(
        self,
        *,
        policy_schema: Mapping[str, Any],
        policy_schemas: Mapping[int, Mapping[str, Any]] | None = None,
        report_schema: Mapping[str, Any],
        acquire_snapshot: Callable[[Path], SnapshotLease],
    ) -> None:
        self._policy_schema, self._report_schema = policy_schema, report_schema
        self._policy_schemas = {1: policy_schema, **(policy_schemas or {})}
        self._acquire_snapshot = acquire_snapshot

    def scan(self, root: Path) -> dict[str, Any]:
        with closing(self._acquire_snapshot(root)) as lease:
            acquired = lease.snapshot
            if not acquired.complete or acquired.policy is None:
                finalized = lease.finalize(policy_schema_version=None)
                return self._finalize_minimal(finalized.snapshot, finalized.reference)
            policy_version: int | None = None
            source_path = acquired.policy.path
            try:
                selected_schema = self._policy_schemas.get(selected_version := getattr(lease, "version", 1))
                if selected_schema is None:
                    raise parser.PolicyValidationError("unknown privilege policy version")
                parsed_policy = parser.parse_policy(acquired.policy, schema=selected_schema)
                policy_version = parsed_policy.schema_version
                if binding_path := policy_binding_failure_for_lease(selected_version, parsed_policy.value, lease):
                    return self._finalize_invalid_input(lease, None, binding_path)
                workflows: dict[str, dict[str, Any]] = {}
                for source in acquired.workflows:
                    source_path = source.path
                    workflows[source.path] = parser.parse_workflow(source, limits=parsed_policy.limits).to_mapping()
            except parser.YamlInputError as exc:
                if exc.limit_dimension is not None:
                    return self._finalize_resource(lease, policy_version, (exc.limit_dimension,))
                return self._finalize_invalid_input(lease, policy_version, source_path)
            except (parser.PolicyValidationError, parser.WorkflowValidationError):
                return self._finalize_invalid_input(lease, policy_version, source_path)

            policy = dict(parsed_policy.value)
            source_sha256 = contracts.canonical_sha256({"policy": policy, "workflows": workflows})
            graph = build_graph(workflows, policy)
            if source_sha256 != contracts.canonical_sha256({"policy": policy, "workflows": workflows}) or graph != (
                _CANONICAL_BUILD_GRAPH(workflows, policy)
            ):
                raise report_contract.InternalReportError("graph result differs from its normalized source")
            inventory = {"job_count": graph.job_count, "edge_count": graph.edge_count, "root_count": graph.root_count}
            if graph.overflow_dimensions:
                return self._finalize_resource(
                    lease,
                    parsed_policy.schema_version,
                    graph.overflow_dimensions,
                    graph.findings,
                    **inventory,
                )
            expansion = expand_routes(graph, workflows, policy)
            if source_sha256 != contracts.canonical_sha256({"policy": policy, "workflows": workflows}) or expansion != (
                _CANONICAL_EXPAND_ROUTES(graph, workflows, policy)
            ):
                raise report_contract.InternalReportError("route expansion differs from its graph source")
            inventory["route_count"] = expansion.route_count
            if expansion.overflow_dimensions:
                return self._finalize_resource(
                    lease,
                    parsed_policy.schema_version,
                    expansion.overflow_dimensions,
                    (*graph.findings, *expansion.findings),
                    **inventory,
                )
            finding_accumulator = _FindingSeal((*graph.findings, *expansion.findings))
            audit = ResolutionAudit(
                expansion.routes,
                workflows,
                policy,
                condition_for_route=RouteConditionProof(graph.roots, workflows),
            )
            try:
                routes, authorities = resolve_routes(
                    expansion.routes,
                    condition_for_route=audit.condition,
                    authority_for_route=audit.authority,
                )
            except contracts.RouteResolutionLimitError as exc:
                _emit_audit_findings(audit, finding_accumulator)
                findings, finding_overflow = finding_accumulator.finish()
                dimensions = tuple(sorted({exc.dimension, *(("findings",) if finding_overflow else ())}))
                return self._finalize_resource(lease, parsed_policy.schema_version, dimensions, findings, **inventory)
            audit.validate(routes, authorities)
            _emit_audit_findings(audit, finding_accumulator)
            resolved_route_ids = tuple(route.route_id for route in routes)
            active = [route for route in routes if route.classification != "PROVEN_NOT_PR_REACHABLE"]
            active_authorities = [item for item in authorities if item.classification != "PROVEN_NOT_PR_REACHABLE"]
            expected = _CANONICAL_VALIDATE_PROFILES(workflows, active, active_authorities, policy)
            profiles = profiles_contract.validate_mandatory_profiles(workflows, active, active_authorities, policy)
            if (
                source_sha256 != contracts.canonical_sha256({"policy": policy, "workflows": workflows})
                or profiles != expected
            ):
                raise report_contract.InternalReportError("profile evidence differs from its normalized source")
            profiles = expected
            if not profiles.findings and not profiles_contract.mandatory_profiles_complete(
                profiles.matches, active, policy
            ):
                raise report_contract.InternalReportError("mandatory profile evidence was omitted")
            matches, profile_by_route = profiles.matches, {match.route_id: match.id for match in profiles.matches}
            authorities = tuple(replace(item, profile_id=profile_by_route.get(item.route_id)) for item in authorities)

            for item in profiles.findings:
                finding_accumulator.add(item)
            findings, finding_overflow = finding_accumulator.finish()
            finalized = lease.finalize(policy_schema_version=parsed_policy.schema_version)
            if not finalized.snapshot.complete:
                dimensions = {*finalized.snapshot.overflow_dimensions, *(("findings",) if finding_overflow else ())}
                overrides = {**inventory, "overflow_dimensions": sorted(dimensions)}
                return self._finalize_minimal(finalized.snapshot, finalized.reference, findings, overrides)
            reference = finalized.reference
            if finding_overflow:
                reference = replace(reference, manifest_sha256=None, complete=False)
                inventory.update(complete=False, manifest_sha256=None, overflow_dimensions=["findings"])
            trusted_inventory = snapshot_inventory(finalized.snapshot, reference, inventory)
            evidence = contracts.report_evidence(trusted_inventory, graph.roots, authorities, matches, findings)
            report = report_contract.build_report(
                finalized.snapshot,
                reference,
                workflows,
                graph.edges,
                graph.roots,
                routes,
                authorities,
                matches,
                findings,
                inventory_overrides=inventory,
            )
            return self._finalize_bounded(
                finalized.snapshot, reference, report, resolved_route_ids, findings, trusted_inventory, evidence
            )

    def _finalize_bounded(
        self,
        snapshot: contracts.Snapshot,
        reference: contracts.SnapshotReference,
        report: dict[str, Any],
        route_ids: tuple[str, ...],
        findings: tuple[contracts.Finding, ...],
        trusted_inventory: Mapping[str, Any],
        evidence: contracts.ReportEvidence,
    ) -> dict[str, Any]:
        dimensions: set[str] = set()
        validated: dict[str, Any] | None = None
        try:
            validated = report_contract.finalize_report(
                report,
                snapshot_reference=reference,
                canonical_route_ids=route_ids,
                evidence=evidence,
                schema=self._report_schema,
            )
        except report_contract.ReportSizeError:
            dimensions.add("report_bytes")
        try:
            report_contract.render_text(report)
        except report_contract.ReportSizeError:
            dimensions.add("text_stdout_bytes")
        if not dimensions:
            if validated is None:
                raise report_contract.InternalReportError("bounded finalization did not produce a report")
            return validated
        downgraded = replace(reference, manifest_sha256=None, complete=False)
        counts = {
            f"{name}_count": trusted_inventory[f"{name}_count"] for name in "workflow job edge root route".split()
        }
        counts["overflow_dimensions"] = sorted({*trusted_inventory["overflow_dimensions"], *dimensions})
        fallback_evidence = contracts.report_evidence(
            snapshot_inventory(snapshot, downgraded, counts), (), (), (), _minimal_findings(findings)
        )
        fallback = report_contract.build_minimal_report(snapshot, downgraded, findings, counts)
        validated = report_contract.finalize_report(
            fallback,
            snapshot_reference=downgraded,
            canonical_route_ids=(),
            evidence=fallback_evidence,
            schema=self._report_schema,
        )
        report_contract.render_text(validated)
        return validated

    def _finalize_invalid_input(
        self,
        lease: SnapshotLease,
        policy_schema_version: int | None,
        subject: str,
    ) -> dict[str, Any]:
        finalized = lease.finalize(policy_schema_version=policy_schema_version)
        code = "PRIVILEGE_INVALID_WORKFLOW" if policy_schema_version else "PRIVILEGE_INVALID_POLICY"
        detail = "workflow failed strict normalization" if policy_schema_version else "policy failed strict validation"
        return self._finalize_minimal(
            finalized.snapshot,
            finalized.reference,
            (contracts.finding(code, subject, detail),),
        )

    def _finalize_resource(
        self,
        lease: SnapshotLease,
        policy_schema_version: int | None,
        dimensions: tuple[str, ...],
        findings: tuple[contracts.Finding, ...] = (),
        **inventory_counts: int,
    ) -> dict[str, Any]:
        if policy_schema_version is None:
            findings = (
                *findings,
                contracts.finding("PRIVILEGE_INVALID_POLICY", _POLICY_PATH, "policy exceeded limits"),
            )
        present = {item.subject for item in findings if item.code == "PRIVILEGE_RESOURCE_LIMIT"}
        resources = tuple(
            contracts.Finding(
                "PRIVILEGE_RESOURCE_LIMIT",
                "UNVERIFIED",
                dimension,
                f"{dimension} exceeded closed maximum {getattr(contracts.V1_LIMITS, dimension)}",
                "REDUCE_OR_PARTITION_WORKFLOWS",
            )
            for dimension in dimensions
            if dimension not in present
        )
        finalized = lease.finalize(policy_schema_version=policy_schema_version)
        if not finalized.snapshot.complete:
            overflow = tuple(sorted({*finalized.snapshot.overflow_dimensions, *dimensions}))
            return self._finalize_minimal(
                finalized.snapshot,
                finalized.reference,
                (*findings, *resources),
                {"overflow_dimensions": list(overflow), **inventory_counts},
            )
        reference = replace(finalized.reference, manifest_sha256=None, complete=False)
        overflow = tuple(sorted({*finalized.snapshot.overflow_dimensions, *dimensions}))
        overrides = {"complete": False, "overflow_dimensions": list(overflow), **inventory_counts}
        return self._finalize_empty(finalized.snapshot, reference, (*findings, *resources), overrides)

    def _finalize_minimal(
        self,
        snapshot: contracts.Snapshot,
        reference: contracts.SnapshotReference,
        additional: tuple[contracts.Finding, ...] = (),
        inventory_overrides: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._finalize_empty(snapshot, reference, (*snapshot.findings, *additional), inventory_overrides)

    def _finalize_empty(
        self,
        snapshot: contracts.Snapshot,
        reference: contracts.SnapshotReference,
        findings: tuple[contracts.Finding, ...],
        inventory_overrides: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        canonical = _FindingSeal(findings).finish()[0]
        evidence = contracts.report_evidence(
            snapshot_inventory(snapshot, reference, inventory_overrides), (), (), (), canonical
        )
        report = report_contract.build_report(
            snapshot,
            reference,
            {},
            (),
            (),
            (),
            (),
            (),
            findings,
            inventory_overrides=inventory_overrides,
        )
        return report_contract.finalize_report(
            report,
            snapshot_reference=reference,
            canonical_route_ids=(),
            evidence=evidence,
            schema=self._report_schema,
        )


def scan_repository(root: Path) -> dict[str, Any]:
    """Default composition root using immutable implementation schemas."""
    from tools.agent_policy.workflow_privilege_composition import scan_repository as compose

    return compose(root, service=WorkflowPrivilegeService)


def project_umbrella_findings(
    root: Path,
    *,
    semantic_service: Callable[[Path], Mapping[str, Any]] | None = None,
    serialize_finding: Callable[[Mapping[str, Any]], str] | None = None,
) -> tuple[str, ...]:
    import json

    schema = json.loads(REPORT_SCHEMA.read_text(encoding="utf-8"))
    if not isinstance(schema, dict):
        raise TypeError("semantic report schema must be a mapping")
    report = (semantic_service or scan_repository)(root)
    report_contract.validate_report(report, schema)
    encoder = serialize_finding or report_contract.canonical_finding_json
    return tuple(f"semantic-pr-privilege={encoder(item)}" for item in report["findings"])


def _minimal_findings(findings: tuple[contracts.Finding, ...]) -> tuple[contracts.Finding, ...]:
    failure = next((item for item in _FindingSeal(findings).finish()[0] if item.status == "FAIL"), None)
    return tuple(item for item in (failure, contracts.RESOURCE_LIMIT_FINDING) if item is not None)


__all__ = ["WorkflowPrivilegeService", "project_umbrella_findings", "scan_repository"]
