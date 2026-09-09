"""Provider-neutral data product access enforcement planning and certification."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness import data_product_access_enforcement_support as support

PLAN_SCHEMA = "dpone.data_product_access_enforcement_plan.v1"
RUN_SCHEMA = "dpone.data_product_access_enforcement_run.v1"
DRIFT_SCHEMA = "dpone.data_product_access_drift_report.v1"
CERTIFICATE_SCHEMA = "dpone.data_product_access_enforcement_certificate.v1"


class TargetAccessDialect(Protocol):
    """Target-specific port for access operation rendering."""

    sink_type: str

    def render_operations(
        self,
        *,
        target: Mapping[str, Any],
        requirements: Sequence[Mapping[str, Any]],
        options: AccessEnforcementOptions,
        target_connection: Mapping[str, Any],
    ) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...], tuple[str, ...]]: ...


@dataclass(frozen=True, slots=True)
class AccessEnforcementOptions:
    enabled: bool
    mode: str
    profile: str
    require_access_gate: bool
    require_authority_gate: bool
    require_target_fingerprint: bool
    require_lock: bool
    drift_policy: str
    strategies: Mapping[str, Any]
    clickhouse: Mapping[str, Any]

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> AccessEnforcementOptions:
        raw = support.options_mapping(manifest)
        return cls(
            enabled=support.bool_value(raw.get("enabled"), False),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            require_access_gate=support.bool_value(raw.get("require_access_gate"), True),
            require_authority_gate=support.bool_value(raw.get("require_authority_gate"), True),
            require_target_fingerprint=support.bool_value(raw.get("require_target_fingerprint"), True),
            require_lock=support.bool_value(raw.get("require_lock"), True),
            drift_policy=str(raw.get("drift_policy") or "block"),
            strategies=_mapping(raw.get("strategies")),
            clickhouse=_mapping(raw.get("clickhouse")),
        )


class AccessEnforcementPlanner:
    """Builds target-bound desired access state and operations."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        classification: Mapping[str, Any],
        entitlement_plan: Mapping[str, Any],
        privacy_impact: Mapping[str, Any],
        access_gate: Mapping[str, Any] | None,
        authority_gate: Mapping[str, Any] | None,
        target_connection: Mapping[str, Any],
        dialect: TargetAccessDialect,
        environment: str,
    ) -> dict[str, Any]:
        options = AccessEnforcementOptions.from_manifest(manifest)
        product = support.product_ref(manifest)
        target = support.target_from_connection(target_connection, product.get("id"))
        if not options.enabled:
            return _plan_payload(
                status="disabled",
                options=options,
                product=product,
                target=target,
                environment=environment,
                classification=classification,
                entitlement_plan=entitlement_plan,
                privacy_impact=privacy_impact,
                access_gate=access_gate,
                authority_gate=authority_gate,
            )
        requirements = support.normalized_requirements(entitlement_plan)
        desired = support.desired_state(requirements)
        blockers = [
            *support.evidence_blockers(
                access_gate=access_gate,
                authority_gate=authority_gate,
                require_access_gate=options.require_access_gate,
                require_authority_gate=options.require_authority_gate,
            ),
            *support.precondition_blockers(
                target,
                require_fingerprint=options.require_target_fingerprint,
                require_lock=options.require_lock,
            ),
        ]
        warnings: list[str] = []
        operations: tuple[dict[str, Any], ...] = ()
        if target.get("sink_type") != dialect.sink_type:
            blockers.append(f"data_product_access_enforcement.unsupported_target:{target.get('sink_type')}")
        else:
            operations, dialect_blockers, dialect_warnings = dialect.render_operations(
                target=target,
                requirements=requirements,
                options=options,
                target_connection=target_connection,
            )
            blockers.extend(dialect_blockers)
            warnings.extend(dialect_warnings)
        blockers, warnings = support.blocked_or_warning(blockers, warnings, options.profile)
        status = support.profile_status(blockers, warnings, "ready")
        return _plan_payload(
            status=status,
            options=options,
            product=product,
            target=target,
            environment=environment,
            classification=classification,
            entitlement_plan=entitlement_plan,
            privacy_impact=privacy_impact,
            access_gate=access_gate,
            authority_gate=authority_gate,
            requirements=requirements,
            desired_state=desired,
            operations=operations,
            blockers=blockers,
            warnings=warnings,
        )


class AccessEnforcementRunner:
    """Executes approved access operations through an injected executor."""

    def apply(
        self,
        *,
        plan: Mapping[str, Any],
        approval: Mapping[str, Any] | None,
        execute: bool,
        executor: Any,
    ) -> dict[str, Any]:
        blockers = list(support.strings(plan.get("blockers")))
        warnings = list(support.strings(plan.get("warnings")))
        operations = [dict(item) for item in support.mappings(plan.get("operations"))]
        results: list[dict[str, Any]] = []
        if not execute:
            results = [{**operation, "status": "planned"} for operation in operations]
            status = "dry_run" if not blockers else "failed"
            return _run_payload(plan=plan, status=status, operations=results, blockers=blockers, warnings=warnings)
        if plan.get("profile") in {"prod_strict", "regulated"} and (
            not approval or approval.get("status") != "approved"
        ):
            blockers.append("data_product_access_enforcement.approval_required")
        if executor is None:
            blockers.append("data_product_access_enforcement.executor_required")
        if blockers:
            return _run_payload(plan=plan, status="failed", operations=results, blockers=blockers, warnings=warnings)
        for operation in operations:
            result = _execute_operation(executor, operation)
            results.append(result)
            if result.get("status") == "failed":
                blockers.append(f"data_product_access_enforcement.operation_failed:{operation.get('name')}")
                break
        status = "failed" if blockers else "warning" if warnings else "applied"
        return _run_payload(plan=plan, status=status, operations=results, blockers=blockers, warnings=warnings)


class AccessDriftInspector:
    """Compares desired access state with actual target state evidence."""

    def inspect(self, *, plan: Mapping[str, Any], actual_state: Mapping[str, Any] | None) -> dict[str, Any]:
        desired = _mapping(plan.get("desired_state"))
        if not actual_state:
            warnings = ["data_product_access_drift.actual_state_unavailable"]
            return _drift_payload(plan=plan, actual_state={}, status="warning", blockers=[], warnings=warnings)
        blockers = [
            *support.missing_grants(desired, actual_state),
            *support.extra_sensitive_grants(desired, actual_state),
            *support.missing_items(desired, actual_state, "masks", "data_product_access_drift.mask_missing"),
            *support.missing_items(
                desired, actual_state, "row_filters", "data_product_access_drift.row_filter_missing"
            ),
        ]
        warnings: list[str] = []
        if plan.get("drift_policy") != "block":
            warnings.extend(blockers)
            blockers = []
        status = "blocked" if blockers else "warning" if warnings else "clean"
        return _drift_payload(
            plan=plan,
            actual_state=actual_state,
            status=status,
            blockers=blockers,
            warnings=warnings,
        )


class AccessEnforcementCertifier:
    """Certifies access enforcement from run and drift evidence."""

    def certify(self, *, run: Mapping[str, Any], drift_report: Mapping[str, Any], profile: str) -> dict[str, Any]:
        blockers = [*support.strings(run.get("blockers")), *support.strings(drift_report.get("blockers"))]
        warnings = [*support.strings(run.get("warnings")), *support.strings(drift_report.get("warnings"))]
        if run.get("status") not in support.PASSING_RUN_STATUSES:
            message = "data_product_access_enforcement.run_not_applied"
            (warnings if profile == "advisory" and run.get("status") == "dry_run" else blockers).append(message)
        if drift_report.get("status") not in support.PASSING_DRIFT_STATUSES:
            blockers.append("data_product_access_enforcement.drift_not_clean")
        blockers, warnings = support.blocked_or_warning(blockers, warnings, profile)
        status = "blocked" if blockers else "warning" if warnings else "certified"
        payload = {
            "schema_version": CERTIFICATE_SCHEMA,
            "status": status,
            "profile": profile,
            "access_enforcement_run_id": run.get("access_enforcement_run_id"),
            "access_drift_report_id": drift_report.get("access_drift_report_id"),
            "plan_id": run.get("plan_id") or drift_report.get("plan_id"),
            "product_id": run.get("product_id") or drift_report.get("product_id"),
            "pack_id": run.get("pack_id") or drift_report.get("pack_id"),
            "bundle_id": run.get("bundle_id") or drift_report.get("bundle_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(status),
        }
        return support.payload_id(payload, "access_enforcement_certificate_id")


def _plan_payload(
    *,
    status: str,
    options: AccessEnforcementOptions,
    product: Mapping[str, Any],
    target: Mapping[str, Any],
    environment: str,
    classification: Mapping[str, Any],
    entitlement_plan: Mapping[str, Any],
    privacy_impact: Mapping[str, Any],
    access_gate: Mapping[str, Any] | None,
    authority_gate: Mapping[str, Any] | None,
    requirements: Sequence[Mapping[str, Any]] = (),
    desired_state: Mapping[str, Any] | None = None,
    operations: Sequence[Mapping[str, Any]] = (),
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
) -> dict[str, Any]:
    payload = {
        "schema_version": PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "environment": environment,
        "product": dict(product),
        "product_id": product.get("id"),
        "target": dict(target),
        "drift_policy": options.drift_policy,
        "access_classification_id": classification.get("access_classification_id"),
        "entitlement_plan_id": entitlement_plan.get("entitlement_plan_id"),
        "privacy_impact_id": privacy_impact.get("privacy_impact_id"),
        "access_gate_id": (access_gate or {}).get("access_gate_id"),
        "authority_gate_id": (authority_gate or {}).get("authority_gate_id"),
        "requirements": [dict(item) for item in requirements],
        "desired_state": dict(desired_state or {"grants": [], "masks": [], "row_filters": [], "sensitive_columns": []}),
        "operations": [dict(item) for item in operations],
        "preconditions": _preconditions(options, target),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "recommendations": _recommendations(status),
    }
    return support.payload_id(payload, "access_enforcement_plan_id")


def _run_payload(
    *,
    plan: Mapping[str, Any],
    status: str,
    operations: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": RUN_SCHEMA,
        "status": status,
        "plan_id": plan.get("access_enforcement_plan_id"),
        "product_id": plan.get("product_id"),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "operations": [dict(item) for item in operations],
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    return support.payload_id(payload, "access_enforcement_run_id")


def _drift_payload(
    *,
    plan: Mapping[str, Any],
    actual_state: Mapping[str, Any],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": DRIFT_SCHEMA,
        "status": status,
        "plan_id": plan.get("access_enforcement_plan_id"),
        "product_id": plan.get("product_id"),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "target": dict(_mapping(plan.get("target"))),
        "desired_state": dict(_mapping(plan.get("desired_state"))),
        "actual_state": dict(actual_state),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    return support.payload_id(payload, "access_drift_report_id")


def _preconditions(options: AccessEnforcementOptions, target: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "require_access_gate": options.require_access_gate,
        "require_authority_gate": options.require_authority_gate,
        "require_target_fingerprint": options.require_target_fingerprint,
        "target_fingerprint": target.get("fingerprint"),
        "require_lock": options.require_lock,
        "lock_id": target.get("lock_id"),
    }


def _execute_operation(executor: Any, operation: Mapping[str, Any]) -> dict[str, Any]:
    try:
        result = executor.execute(dict(operation))
    except Exception as exc:  # pragma: no cover - target driver surface
        return {**dict(operation), "status": "failed", "error": str(exc)}
    return {**dict(operation), "status": str(result.get("status", "executed"))}


def _recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Resolve access enforcement blockers before regulated release closeout."]
    if status == "warning":
        return ["Review access enforcement warnings and record evidence in the registry."]
    if status == "disabled":
        return ["Enable access_enforcement to enforce access gates on target systems."]
    return ["Record access enforcement evidence in bundle and registry."]


def _mapping(raw: Any) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "CERTIFICATE_SCHEMA",
    "DRIFT_SCHEMA",
    "PLAN_SCHEMA",
    "RUN_SCHEMA",
    "AccessDriftInspector",
    "AccessEnforcementCertifier",
    "AccessEnforcementOptions",
    "AccessEnforcementPlanner",
    "AccessEnforcementRunner",
    "TargetAccessDialect",
]
