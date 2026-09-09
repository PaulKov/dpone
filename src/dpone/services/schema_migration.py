"""Facade for schema migration control-plane CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.readiness.migration_control import (
    ArtifactMigrationLedgerStore,
    MigrationLedgerRecord,
    MigrationPack,
    migration_blocked_result,
    migration_blocked_result_by_id,
    read_json_object,
    stable_fingerprint,
)
from dpone.services.readiness import ReadinessService
from dpone.services.schema_migration_execution import (
    MigrationOperationExecutor,
    ddl_operations,
    execute_migration_operations,
    load_migration_operation_executor,
    phase_operations,
)
from dpone.services.schema_migration_helpers import (
    attach_contract_compatibility_summary,
    default_changes,
    migration_apply_blockers,
    reconciliation_mode_override,
    target_from_plan,
)
from dpone.services.schema_migration_identity import build_identity_migration_plan
from dpone.services.schema_migration_impact import attach_impact_summary
from dpone.services.schema_migration_phases import (
    apply_phase_record,
    build_shadow_plan,
    last_phase,
    merge_shadow_blockers,
    migration_phase_payload,
    normalize_actual_state,
    resolve_migration_strategy,
)
from dpone.services.schema_migration_promotion import resolve_apply_environment_context


class MigrationControlFacade:
    """Thin application facade for migration pack and ledger operations."""

    def __init__(self, readiness: ReadinessService | None = None) -> None:
        self._readiness = readiness or ReadinessService()

    def plan(
        self,
        *,
        manifest_path: str,
        actual_path: str | None = None,
        source_path: str | None = None,
        table: str | None = None,
        sink_type: str | None = None,
        strategy: str | None = None,
    ) -> dict[str, Any]:
        desired_plan = self._readiness.physical_plan(
            manifest_path=manifest_path,
            source_path=source_path,
            table=table,
            sink_type=sink_type,
        )
        actual = self._load_actual(actual_path)
        identity = build_identity_migration_plan(
            identity_plan=self._readiness.schema_identity_plan(
                manifest_path=manifest_path,
                source_path=source_path,
                actual_path=actual_path,
            ),
            sink_type=str(desired_plan.get("sink_type", sink_type or "unknown")),
            table=str(desired_plan.get("table", "")),
        )
        desired_plan = {
            **desired_plan,
            "identity_decisions": identity.payload.get("identity_decisions", []),
            "schema_identity": identity.payload,
        }
        resolved_strategy = resolve_migration_strategy(desired_plan, strategy)
        diff = self._physical_diff(
            manifest_path=manifest_path,
            actual_path=actual_path,
            source_path=source_path,
            table=table,
            sink_type=sink_type,
            strategy=strategy,
            resolved_strategy=resolved_strategy,
        )
        shadow = build_shadow_plan(
            desired_plan=desired_plan,
            actual=actual,
            diff=diff,
            strategy=resolved_strategy,
        )
        blockers = tuple(str(item) for item in diff.get("blockers", [])) if diff else ()
        phases: tuple[dict[str, Any], ...] = ()
        rollback: dict[str, Any] | None = None
        if shadow is not None:
            phases = tuple(dict(item) for item in shadow.get("phases", []))
            blockers = merge_shadow_blockers(blockers, shadow)
            rollback = dict(shadow.get("rollback", {"supported": False, "ddl": []}))
        if identity.phases:
            if phases:
                blockers = (*blockers, "schema_identity.phase_conflict")
            else:
                phases = identity.phases
                rollback = {"supported": False, "ddl": []}
        changes = (tuple(diff.get("changes", [])) if diff else default_changes(desired_plan)) + identity.changes
        blockers = (*blockers, *identity.blockers)
        warnings = (
            tuple(str(item) for item in desired_plan.get("warnings", []))
            + (tuple(str(item) for item in diff.get("warnings", [])) if diff else ())
            + identity.warnings
        )
        pack = MigrationPack.build(
            target=target_from_plan(desired_plan),
            desired=desired_plan,
            actual=actual,
            changes=changes,
            blockers=blockers,
            warnings=warnings,
            ddl=tuple(str(item) for item in diff.get("ddl", [])) if diff else tuple(desired_plan.get("ddl", [])),
            strategy=resolved_strategy,
            phases=phases,
            rollback=rollback,
        )
        payload = attach_contract_compatibility_summary(
            payload=pack.to_dict(command="plan"),
            manifest_path=manifest_path,
        )
        payload = attach_impact_summary(payload=payload, manifest_path=manifest_path)
        if shadow:
            payload["shadow"] = shadow
        if identity.payload.get("identity_decisions"):
            payload["identity_decisions"] = list(identity.payload.get("identity_decisions", []))
            payload["identity_migration"] = identity.payload
        return payload

    def baseline(
        self,
        *,
        manifest_path: str,
        actual_path: str,
        ledger_path: str,
        source_path: str | None = None,
        table: str | None = None,
        sink_type: str | None = None,
    ) -> dict[str, Any]:
        desired_plan = self._readiness.physical_plan(
            manifest_path=manifest_path,
            source_path=source_path,
            table=table,
            sink_type=sink_type,
        )
        actual = self._load_actual(actual_path)
        target = target_from_plan(desired_plan)
        actual_fingerprint = stable_fingerprint(actual)
        record = MigrationLedgerRecord(
            pack_id=stable_fingerprint({"baseline": target.to_dict(), "actual": actual_fingerprint}),
            status="baseline",
            target=target,
            desired_fingerprint=stable_fingerprint(desired_plan),
            actual_fingerprint=actual_fingerprint,
            warnings=("baseline adopts existing target state without changing user tables",),
        )
        ArtifactMigrationLedgerStore(Path(ledger_path)).append(record)
        return record.to_dict()

    def apply(
        self,
        *,
        plan_path: str,
        ledger_path: str,
        actual_path: str | None = None,
        approval_path: str | None = None,
        phase: str | None = None,
        execute: bool = False,
        executor: MigrationOperationExecutor | None = None,
        target_connection_path: str | None = None,
        environment: str | None = None,
        environment_contract_path: str | None = None,
        promotion_path: str | None = None,
        bundle_gate_path: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        plan_payload = read_json_object(plan_path)
        pack = MigrationPack.from_mapping(plan_payload)
        env_context = resolve_apply_environment_context(
            pack=pack,
            ledger_path=ledger_path,
            actual_path=actual_path,
            target_connection_path=target_connection_path,
            environment=environment,
            environment_contract_path=environment_contract_path,
            promotion_path=promotion_path,
        )
        if env_context.blockers:
            return 2, migration_blocked_result("apply", pack, env_context.blockers)
        ledger_path = env_context.ledger_path
        actual_path = env_context.actual_path
        target_connection_path = env_context.target_connection_path
        blockers = migration_apply_blockers(
            load_actual=self._load_actual,
            pack=pack,
            plan_payload=plan_payload,
            actual_path=actual_path,
            approval_path=approval_path,
            ledger_path=ledger_path,
            phase=phase,
            bundle_gate_path=bundle_gate_path,
        )
        if blockers:
            return 2, migration_blocked_result("apply", pack, blockers)
        target_executor = executor
        if execute and target_executor is None:
            target_executor = load_migration_operation_executor(
                target=pack.target,
                target_connection_path=target_connection_path,
            )
        try:
            if pack.phases:
                executed_operations = None
                if execute:
                    report = execute_migration_operations(
                        operations=phase_operations(migration_phase_payload(pack, str(phase))),
                        executor=target_executor,
                    )
                    if report.blockers:
                        payload = migration_blocked_result("apply", pack, report.blockers)
                        payload["phase"] = str(phase)
                        payload["operations"] = list(report.operations)
                        return 2, payload
                    executed_operations = report.operations
                return apply_phase_record(
                    pack=pack,
                    ledger_path=ledger_path,
                    phase=str(phase),
                    executed_operations=executed_operations,
                    environment=env_context.environment,
                    promotion_id=env_context.promotion_id,
                )
            operations: tuple[dict[str, Any], ...] = ()
            if execute:
                report = execute_migration_operations(operations=ddl_operations(pack.ddl), executor=target_executor)
                if report.blockers:
                    payload = migration_blocked_result("apply", pack, report.blockers)
                    payload["operations"] = list(report.operations)
                    return 2, payload
                operations = report.operations
            record = MigrationLedgerRecord(
                pack_id=pack.pack_id,
                status="applied",
                target=pack.target,
                desired_fingerprint=pack.desired_fingerprint,
                actual_fingerprint=pack.actual_fingerprint,
                environment=env_context.environment,
                promotion_id=env_context.promotion_id,
                warnings=pack.warnings,
                operations=operations,
            )
            ArtifactMigrationLedgerStore(Path(ledger_path)).append(record)
            payload = record.to_dict()
            payload["command"] = "apply"
            payload["ddl"] = list(pack.ddl)
            payload["approval"] = str(approval_path) if approval_path else None
            return 0, payload
        finally:
            if execute and executor is None:
                close = getattr(target_executor, "close", None)
                if callable(close):
                    close()

    def history(self, *, ledger_path: str) -> dict[str, Any]:
        records = list(ArtifactMigrationLedgerStore(Path(ledger_path)).records())
        return {
            "schema_version": "dpone.schema_migration_history.v1",
            "command": "history",
            "ledger": str(ledger_path),
            "records": records,
        }

    def rollback(
        self,
        *,
        plan_path: str | None = None,
        pack_id: str | None = None,
        ledger_path: str | None = None,
        approval_path: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        pack = MigrationPack.from_mapping(read_json_object(plan_path)) if plan_path else None
        if pack is None:
            return 2, migration_blocked_result_by_id("rollback", pack_id, ("migration.rollback_plan_required",))
        rollback = pack.rollback
        if not rollback.get("supported"):
            return 2, migration_blocked_result("rollback", pack, ("migration.rollback_not_supported",))
        if last_phase(pack_id=pack.pack_id, ledger_path=ledger_path) == "contract":
            return 2, migration_blocked_result("rollback", pack, ("migration.rollback_not_supported_after_contract",))
        record = MigrationLedgerRecord(
            pack_id=pack.pack_id,
            status="rolled_back",
            target=pack.target,
            desired_fingerprint=pack.desired_fingerprint,
            actual_fingerprint=pack.actual_fingerprint,
            warnings=("rollback applied from reversible migration pack",),
        )
        if ledger_path:
            ArtifactMigrationLedgerStore(Path(ledger_path)).append(record)
        payload = record.to_dict()
        payload["command"] = "rollback"
        payload["ddl"] = list(rollback.get("ddl", []))
        payload["approval"] = str(approval_path) if approval_path else None
        return 0, payload

    def _physical_diff(
        self,
        *,
        manifest_path: str,
        actual_path: str | None,
        source_path: str | None,
        table: str | None,
        sink_type: str | None,
        strategy: str | None,
        resolved_strategy: str,
    ) -> dict[str, Any]:
        if not actual_path:
            return {}
        reconciliation_mode = reconciliation_mode_override(
            cli_strategy=strategy,
            resolved_strategy=resolved_strategy,
        )
        return self._readiness.physical_diff(
            manifest_path=manifest_path,
            actual_path=actual_path,
            source_path=source_path,
            table=table,
            sink_type=sink_type,
            reconciliation_mode=reconciliation_mode,
        )

    def _load_actual(self, actual_path: str | None) -> dict[str, Any] | None:
        if not actual_path:
            return None
        raw = read_json_object(actual_path)
        return normalize_actual_state(raw)


__all__ = ["MigrationControlFacade"]
