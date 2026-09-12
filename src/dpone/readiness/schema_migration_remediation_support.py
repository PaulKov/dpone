"""Internal helpers for provider-neutral schema migration remediation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.readiness.migration_control import MigrationPack

REMEDIATION_PLAN_SCHEMA = "dpone.schema_migration_remediation_plan.v1"
REMEDIATION_RUN_SCHEMA = "dpone.schema_migration_remediation_run.v1"
REMEDIATION_CERTIFICATE_SCHEMA = "dpone.schema_migration_remediation_certificate.v1"
PROFILES = {"advisory", "stage", "prod_strict", "regulated"}
REMEDIATION_STAGES = {"remediation_planned", "remediated", "rollback_certified"}


def remediation_options(manifest: Mapping[str, Any] | None) -> dict[str, Any]:
    options: Mapping[str, Any] | None = manifest or {}
    for key in ("sink", "options", "physical_design", "migration", "remediation"):
        value = options.get(key) if isinstance(options, Mapping) else {}
        options = value if isinstance(value, Mapping) else {}
    return dict(options or {})


def execution_options(options: Mapping[str, Any]) -> dict[str, bool]:
    raw = options.get("execution", {})
    raw = raw if isinstance(raw, Mapping) else {}
    return {
        "require_approval": bool(raw.get("require_approval", _profile(options) in {"prod_strict", "regulated"})),
        "require_watch_certificate": bool(raw.get("require_watch_certificate", True)),
        "require_target_fingerprint": bool(raw.get("require_target_fingerprint", True)),
        "require_lock": bool(raw.get("require_lock", True)),
    }


def rollback_options(options: Mapping[str, Any]) -> dict[str, Any]:
    raw = options.get("rollback", {})
    raw = raw if isinstance(raw, Mapping) else {}
    return {
        "strategy": str(raw.get("strategy") or "controlled"),
        "allow_after_contract": bool(raw.get("allow_after_contract", False)),
        "retain_backup_required": bool(raw.get("retain_backup_required", True)),
    }


def certify_options(options: Mapping[str, Any]) -> dict[str, bool]:
    raw = options.get("certify", {})
    raw = raw if isinstance(raw, Mapping) else {}
    defaults = {"physical_design": True, "canary_queries": True, "row_count": True, "rollback_window": True}
    return {key: bool(raw.get(key, default)) for key, default in defaults.items()}


def target_connection_public(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in raw.items()
        if key not in {"password", "token", "secret"} and isinstance(value, str | int | float | bool)
    }


def target_blockers(pack: MigrationPack, target_connection: Mapping[str, Any]) -> list[str]:
    sink_type = str(target_connection.get("type") or target_connection.get("sink_type") or pack.target.sink_type)
    if sink_type.lower() != pack.target.sink_type.lower():
        return [f"schema_migration_remediation.target_mismatch:{sink_type}"]
    if sink_type.lower() != "clickhouse":
        return [f"schema_migration_remediation.unsupported_target:{sink_type}"]
    return []


def watch_blockers(
    *,
    pack: MigrationPack,
    watch_certificate: Mapping[str, Any],
    options: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if execution_options(options)["require_watch_certificate"] and not watch_certificate:
        blockers.append("schema_migration_remediation.watch_certificate_required")
        return blockers
    if watch_certificate.get("pack_id") != pack.pack_id:
        blockers.append("schema_migration_remediation.watch_pack_id_mismatch")
    if str(options.get("mode") or "gate") == "gate" and not watch_requires_remediation(watch_certificate, options):
        blockers.append("schema_migration_remediation.watch_does_not_require_remediation")
    return blockers


def watch_requires_remediation(watch_certificate: Mapping[str, Any], options: Mapping[str, Any]) -> bool:
    status = str(watch_certificate.get("status") or "")
    decision = str(mapping(watch_certificate.get("remediation")).get("decision") or "")
    if status == "blocked" and decision == "rollback_required":
        return True
    if status == "warning" and decision == "rollback_recommended":
        return True
    strategy = rollback_options(options)["strategy"]
    return status == "stable" and strategy == "manual_only"


def ledger_records(ledger: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    records = ledger.get("records", []) if isinstance(ledger, Mapping) else []
    return tuple(item for item in records if isinstance(item, Mapping)) if isinstance(records, list) else ()


def last_phase(records: Sequence[Mapping[str, Any]], pack_id: str) -> str | None:
    phases = [
        str(record.get("phase")) for record in records if record.get("pack_id") == pack_id and record.get("phase")
    ]
    return phases[-1] if phases else None


def last_status(records: Sequence[Mapping[str, Any]], pack_id: str) -> str | None:
    statuses = [str(record.get("status")) for record in records if record.get("pack_id") == pack_id]
    return statuses[-1] if statuses else None


def already_remediated(records: Sequence[Mapping[str, Any]], pack_id: str) -> bool:
    return any(
        record.get("pack_id") == pack_id and record.get("status") in {"rolled_back", "remediation_applied"}
        for record in records
    )


def classify_capability(
    *,
    pack: MigrationPack,
    records: Sequence[Mapping[str, Any]],
    options: Mapping[str, Any],
    backup_certificate: Mapping[str, Any] | None = None,
    recovery_point: Mapping[str, Any] | None = None,
) -> tuple[str, list[str]]:
    rollback = pack.rollback if isinstance(pack.rollback, Mapping) else {}
    phase = last_phase(records, pack.pack_id)
    rollback_cfg = rollback_options(options)
    if phase == "contract" and not rollback_cfg["allow_after_contract"]:
        if _valid_backup_certificate(pack, backup_certificate):
            return "restore_from_backup", []
        if _valid_recovery_point(pack, recovery_point):
            return "restore_to_point", []
        return "blocked_after_contract", ["schema_migration_remediation.blocked_after_contract"]
    if not rollback.get("supported"):
        return "unsupported", ["schema_migration_remediation.rollback_not_supported"]
    ddl = rollback_ddl(pack)
    if not ddl:
        return "manual_only", ["schema_migration_remediation.manual_only"]
    if pack.strategy == "shadow" and phase == "cutover":
        return "shadow_exchange", []
    if pack.strategy == "shadow" and phase in {"create_shadow", "backfill", "validate"}:
        return "drop_shadow", []
    return "online_safe", []


def rollback_ddl(pack: MigrationPack) -> tuple[str, ...]:
    raw = pack.rollback.get("ddl", []) if isinstance(pack.rollback, Mapping) else []
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list | tuple) else ()


def rollback_operations(pack: MigrationPack) -> list[dict[str, str]]:
    return [
        {"name": f"rollback_{index}", "operation_type": "sql", "sql": statement}
        for index, statement in enumerate(rollback_ddl(pack), start=1)
    ]


def backup_restore_operations(backup_certificate: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    remediation = mapping(backup_certificate.get("remediation") if backup_certificate else {})
    raw = remediation.get("operations", [])
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def recovery_point_restore_operations(recovery_point: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not recovery_point:
        return []
    target = mapping(recovery_point.get("target"))
    table = str(target.get("table") or "")
    destination = str(recovery_point.get("destination") or "")
    if not table or not destination:
        return []
    restored = _recovery_restore_table_name(table, str(recovery_point.get("restore_point_id") or ""))
    return [
        {
            "name": "restore_to_point",
            "operation_type": "sql",
            "sql": f"RESTORE TABLE {_quote_table(table)} AS {_quote_table(restored)} FROM {destination}",
        },
        {
            "name": "exchange_restored_point",
            "operation_type": "sql",
            "sql": f"EXCHANGE TABLES {_quote_table(table)} AND {_quote_table(restored)}",
        },
    ]


def _valid_backup_certificate(pack: MigrationPack, backup_certificate: Mapping[str, Any] | None) -> bool:
    if not backup_certificate:
        return False
    if backup_certificate.get("pack_id") != pack.pack_id:
        return False
    if backup_certificate.get("status") != "certified":
        return False
    return bool(backup_restore_operations(backup_certificate))


def _valid_recovery_point(pack: MigrationPack, recovery_point: Mapping[str, Any] | None) -> bool:
    if not recovery_point:
        return False
    if recovery_point.get("pack_id") != pack.pack_id:
        return False
    return recovery_point.get("status") == "usable"


def _recovery_restore_table_name(table: str, restore_point_id: str) -> str:
    parts = [part for part in table.split(".") if part]
    name = parts[-1] if parts else "table"
    prefix = ".".join(parts[:-1])
    restored = f"__dpone_recovery_{name}_{restore_point_id.split(':', 1)[-1][:12]}"
    return f"{prefix}.{restored}" if prefix else restored


def _quote_table(table: str) -> str:
    return ".".join("`" + part.strip("`").replace("`", "``") + "`" for part in str(table).split(".") if part)


def preconditions(options: Mapping[str, Any], watch_certificate: Mapping[str, Any]) -> dict[str, Any]:
    execution = execution_options(options)
    return {
        "approval_required": execution["require_approval"],
        "watch_certificate_id": watch_certificate.get("certificate_id"),
        "target_fingerprint_required": execution["require_target_fingerprint"],
        "lock_required": execution["require_lock"],
    }


def approval_blockers(
    *,
    pack: MigrationPack,
    preconditions_payload: Mapping[str, Any],
    approval: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    if not preconditions_payload.get("approval_required"):
        return ()
    if approval is None:
        return ("schema_migration_remediation.approval_required",)
    if approval.get("pack_id") != pack.pack_id:
        return ("schema_migration_remediation.approval_pack_id_mismatch",)
    return ()


def strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _profile(options: Mapping[str, Any]) -> str:
    value = str(options.get("profile") or "stage")
    return value if value in PROFILES else "stage"


__all__ = [
    "PROFILES",
    "REMEDIATION_CERTIFICATE_SCHEMA",
    "REMEDIATION_PLAN_SCHEMA",
    "REMEDIATION_RUN_SCHEMA",
    "REMEDIATION_STAGES",
    "already_remediated",
    "approval_blockers",
    "backup_restore_operations",
    "certify_options",
    "classify_capability",
    "execution_options",
    "last_phase",
    "last_status",
    "ledger_records",
    "mapping",
    "preconditions",
    "recovery_point_restore_operations",
    "remediation_options",
    "rollback_operations",
    "strings",
    "target_blockers",
    "target_connection_public",
    "watch_blockers",
]
