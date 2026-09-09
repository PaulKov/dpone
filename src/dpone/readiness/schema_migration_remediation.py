"""Provider-neutral controlled remediation for schema migration packs."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.schema_migration_remediation_support import (
    PROFILES,
    REMEDIATION_CERTIFICATE_SCHEMA,
    REMEDIATION_PLAN_SCHEMA,
    REMEDIATION_RUN_SCHEMA,
    already_remediated,
    approval_blockers,
    backup_restore_operations,
    certify_options,
    classify_capability,
    ledger_records,
    mapping,
    preconditions,
    recovery_point_restore_operations,
    remediation_options,
    rollback_operations,
    strings,
    target_blockers,
    target_connection_public,
    watch_blockers,
)


class RemediationOperationExecutor(Protocol):
    """Narrow target execution port used by controlled remediation."""

    def execute(self, operation: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MigrationRemediationPlanner:
    """Build deterministic remediation plans from watch evidence."""

    def plan(
        self,
        *,
        pack: Mapping[str, Any],
        watch_certificate: Mapping[str, Any],
        ledger: Mapping[str, Any] | None,
        manifest: Mapping[str, Any] | None,
        target_connection: Mapping[str, Any],
        environment: str,
        backup_certificate: Mapping[str, Any] | None = None,
        recovery_point: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        migration_pack = MigrationPack.from_mapping(dict(pack))
        options = remediation_options(manifest)
        records = ledger_records(ledger)
        if not bool(options.get("enabled", False)):
            return _disabled_plan(migration_pack, watch_certificate, target_connection, environment)
        capability, capability_blockers = classify_capability(
            pack=migration_pack,
            records=records,
            options=options,
            backup_certificate=backup_certificate,
            recovery_point=recovery_point,
        )
        blockers = [
            *(["schema_migration_remediation.pack_blocked"] if migration_pack.blockers else []),
            *watch_blockers(pack=migration_pack, watch_certificate=watch_certificate, options=options),
            *target_blockers(migration_pack, target_connection),
            *(
                ["schema_migration_remediation.already_remediated"]
                if already_remediated(records, migration_pack.pack_id)
                else []
            ),
            *capability_blockers,
        ]
        operations = _remediation_operations(migration_pack, capability, backup_certificate, recovery_point)
        payload: dict[str, Any] = {
            "schema_version": REMEDIATION_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "pack_id": migration_pack.pack_id,
            "watch_certificate_id": watch_certificate.get("certificate_id"),
            "environment": environment,
            "target": migration_pack.target.to_dict(),
            "target_connection": target_connection_public(target_connection),
            "desired_fingerprint": migration_pack.desired_fingerprint,
            "actual_fingerprint": migration_pack.actual_fingerprint,
            "capability": capability,
            "backup_certificate_id": backup_certificate.get("certificate_id") if backup_certificate else None,
            "recovery_point_id": recovery_point.get("restore_point_id") if recovery_point else None,
            "profile": _profile(options),
            "mode": str(options.get("mode") or "gate"),
            "operations": operations,
            "preconditions": preconditions(options, watch_certificate),
            "certify": certify_options(options),
            "ledger_summary": _ledger_summary(records, migration_pack.pack_id),
            "watch": dict(watch_certificate),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(migration_pack.warnings),
        }
        payload["remediation_plan_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "watch_certificate_id": payload["watch_certificate_id"],
                "backup_certificate_id": payload["backup_certificate_id"],
                "recovery_point_id": payload["recovery_point_id"],
                "environment": environment,
                "target": payload["target"],
                "capability": capability,
                "operations": operations,
                "preconditions": payload["preconditions"],
                "ledger_summary": payload["ledger_summary"],
                "blockers": payload["blockers"],
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class MigrationRemediationRunner:
    """Executes approved remediation operations through injected target ports."""

    def run(
        self,
        *,
        plan: Mapping[str, Any],
        executor: RemediationOperationExecutor | None,
        execute: bool = False,
        approval: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if plan.get("status") == "blocked":
            return _run_payload(
                plan=plan, status="blocked", execute=execute, blockers=strings(plan.get("blockers", []))
            )
        if not execute:
            return _run_payload(
                plan=plan,
                status="dry_run",
                execute=False,
                warnings=("schema_migration_remediation.not_executed",),
            )
        pack = _pack_from_plan(plan)
        blockers = list(
            approval_blockers(pack=pack, preconditions_payload=mapping(plan.get("preconditions")), approval=approval)
        )
        if executor is None:
            blockers.append("schema_migration_remediation.target_executor_required")
        if blockers:
            return _run_payload(plan=plan, status="blocked", execute=True, blockers=blockers)
        start = time.perf_counter()
        operations = _execute_operations(_operations(plan), executor)
        blockers = _execution_blockers(operations)
        status = "failed" if blockers else "remediated"
        return _run_payload(
            plan=plan,
            status=status,
            execute=True,
            operations=tuple(operations),
            blockers=blockers,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )


@dataclass(frozen=True, slots=True)
class MigrationRemediationCertifier:
    """Converts remediation run evidence into an audit-ready certificate."""

    def certify(self, *, run: Mapping[str, Any], profile: str = "stage") -> dict[str, Any]:
        profile = profile if profile in PROFILES else "stage"
        blockers = list(strings(run.get("blockers", [])))
        warnings = list(strings(run.get("warnings", [])))
        if run.get("status") == "dry_run":
            if profile == "advisory":
                warnings.append("schema_migration_remediation.dry_run_certificate")
            else:
                blockers.append("schema_migration_remediation.execution_required")
        if run.get("status") in {"blocked", "failed"}:
            blockers.append("schema_migration_remediation.run_blocked")
        if profile in {"prod_strict", "regulated"} and run.get("status") != "remediated":
            blockers.append("schema_migration_remediation.remediated_state_required")
        status = "blocked" if blockers else "warning" if warnings else "certified"
        payload: dict[str, Any] = {
            "schema_version": REMEDIATION_CERTIFICATE_SCHEMA,
            "status": status,
            "profile": profile,
            "pack_id": run.get("pack_id"),
            "watch_certificate_id": run.get("watch_certificate_id"),
            "environment": run.get("environment"),
            "target": dict(run.get("target", {})) if isinstance(run.get("target"), Mapping) else {},
            "remediation_run_id": run.get("remediation_run_id"),
            "checks": _certificate_checks(run, status),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "metrics": dict(run.get("metrics", {})) if isinstance(run.get("metrics"), Mapping) else {},
        }
        payload["certificate_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "run_id": payload["remediation_run_id"],
                "profile": profile,
                "status": status,
                "blockers": payload["blockers"],
                "warnings": payload["warnings"],
                "metrics": payload["metrics"],
            }
        )
        return payload


def _disabled_plan(
    pack: MigrationPack,
    watch_certificate: Mapping[str, Any],
    target_connection: Mapping[str, Any],
    environment: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": REMEDIATION_PLAN_SCHEMA,
        "status": "disabled",
        "pack_id": pack.pack_id,
        "watch_certificate_id": watch_certificate.get("certificate_id"),
        "environment": environment,
        "target": pack.target.to_dict(),
        "target_connection": target_connection_public(target_connection),
        "operations": [],
        "preconditions": {},
        "blockers": [],
        "warnings": ["schema_migration_remediation.disabled"],
    }
    payload["remediation_plan_id"] = stable_fingerprint(payload)
    return payload


def _remediation_operations(
    pack: MigrationPack,
    capability: str,
    backup_certificate: Mapping[str, Any] | None,
    recovery_point: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if capability in {"blocked_after_contract", "unsupported", "manual_only"}:
        return []
    if capability == "restore_from_backup":
        return backup_restore_operations(backup_certificate)
    if capability == "restore_to_point":
        return recovery_point_restore_operations(recovery_point)
    return rollback_operations(pack)


def _run_payload(
    *,
    plan: Mapping[str, Any],
    status: str,
    execute: bool,
    operations: tuple[dict[str, Any], ...] = (),
    blockers: tuple[str, ...] | list[str] = (),
    warnings: tuple[str, ...] | list[str] = (),
    duration_ms: int = 0,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": REMEDIATION_RUN_SCHEMA,
        "status": status,
        "executed": execute,
        "remediation_plan_id": plan.get("remediation_plan_id"),
        "pack_id": plan.get("pack_id"),
        "watch_certificate_id": plan.get("watch_certificate_id"),
        "environment": plan.get("environment"),
        "target": dict(plan.get("target", {})) if isinstance(plan.get("target"), Mapping) else {},
        "capability": plan.get("capability"),
        "operations": list(operations),
        "ledger_status": "rolled_back" if status == "remediated" else None,
        "blockers": list(dict.fromkeys(str(item) for item in blockers if str(item))),
        "warnings": list(dict.fromkeys(str(item) for item in warnings if str(item))),
        "metrics": {"duration_ms": duration_ms, "operations_executed": len(operations)},
    }
    payload["remediation_run_id"] = stable_fingerprint(
        {
            "plan_id": payload["remediation_plan_id"],
            "status": status,
            "executed": execute,
            "operations": payload["operations"],
            "blockers": payload["blockers"],
            "warnings": payload["warnings"],
        }
    )
    return {key: value for key, value in payload.items() if value is not None}


def _pack_from_plan(plan: Mapping[str, Any]) -> MigrationPack:
    target = mapping(plan.get("target"))
    return MigrationPack.from_mapping(
        {
            "pack_id": plan.get("pack_id"),
            "target": target,
            "desired_fingerprint": plan.get("desired_fingerprint") or stable_fingerprint({}),
            "actual_fingerprint": plan.get("actual_fingerprint"),
            "desired": {},
            "actual": {},
        }
    )


def _operations(plan: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    raw = plan.get("operations", [])
    return tuple(dict(item) for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


def _execute_operations(
    operations: tuple[dict[str, Any], ...],
    executor: RemediationOperationExecutor,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for operation in operations:
        try:
            result = executor.execute(dict(operation))
            status = str(result.get("status") or "executed") if isinstance(result, Mapping) else "executed"
            payload = {**operation, "status": status}
            if isinstance(result, Mapping):
                payload.update({key: value for key, value in result.items() if key not in payload})
        except Exception as exc:  # pragma: no cover - driver-specific execution surface
            payload = {**operation, "status": "failed", "error": str(exc)}
        results.append(payload)
    return results


def _execution_blockers(operations: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            f"schema_migration_remediation.operation_failed:{operation.get('name', 'operation')}"
            for operation in operations
            if operation.get("status") == "failed"
        )
    )


def _ledger_summary(records: tuple[Mapping[str, Any], ...], pack_id: str) -> dict[str, Any]:
    matched = [record for record in records if record.get("pack_id") == pack_id]
    return {
        "records": len(matched),
        "last_status": matched[-1].get("status") if matched else None,
        "last_phase": next((record.get("phase") for record in reversed(matched) if record.get("phase")), None),
    }


def _certificate_checks(run: Mapping[str, Any], status: str) -> list[dict[str, Any]]:
    return [
        {"name": "remediation_run", "status": "passed" if status in {"certified", "warning"} else "failed"},
        {
            "name": "operation_count",
            "status": "passed",
            "value": mapping(run.get("metrics")).get("operations_executed", 0),
        },
    ]


def _profile(options: Mapping[str, Any]) -> str:
    value = str(options.get("profile") or "stage")
    return value if value in PROFILES else "stage"


__all__ = [
    "REMEDIATION_CERTIFICATE_SCHEMA",
    "REMEDIATION_PLAN_SCHEMA",
    "REMEDIATION_RUN_SCHEMA",
    "MigrationRemediationCertifier",
    "MigrationRemediationPlanner",
    "MigrationRemediationRunner",
    "RemediationOperationExecutor",
]
