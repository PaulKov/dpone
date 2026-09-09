"""Internal helpers for provider-neutral schema migration backup contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.migration_control import MigrationPack

BACKUP_PLAN_SCHEMA = "dpone.schema_migration_backup_plan.v1"
BACKUP_RUN_SCHEMA = "dpone.schema_migration_backup_run.v1"
RESTORE_PLAN_SCHEMA = "dpone.schema_migration_restore_plan.v1"
RESTORE_RUN_SCHEMA = "dpone.schema_migration_restore_run.v1"
BACKUP_CERTIFICATE_SCHEMA = "dpone.schema_migration_backup_certificate.v1"
PROFILES = {"advisory", "stage", "prod_strict", "regulated"}
STRATEGIES = {"none", "target_native", "artifact_export", "manual"}
DEFAULT_REQUIRE_FOR = ("data_destructive", "shadow_contract", "physical_layout_change", "direct_rename")
BACKUP_STAGES = {"backup_planned", "backup_created", "restore_rehearsed", "backup_certified"}


def backup_options(manifest: Mapping[str, Any] | None) -> dict[str, Any]:
    options: Mapping[str, Any] | None = manifest or {}
    for key in ("sink", "options", "physical_design", "migration", "backup"):
        value = options.get(key) if isinstance(options, Mapping) else {}
        options = value if isinstance(value, Mapping) else {}
    raw = dict(options or {})
    raw.setdefault("mode", "gate")
    raw.setdefault("profile", "stage")
    raw.setdefault("strategy", "target_native")
    raw.setdefault("require_for", list(DEFAULT_REQUIRE_FOR))
    return raw


def profile(options: Mapping[str, Any]) -> str:
    value = str(options.get("profile") or "stage")
    return value if value in PROFILES else "stage"


def strategy(options: Mapping[str, Any]) -> str:
    value = str(options.get("strategy") or "target_native")
    return value if value in STRATEGIES else "target_native"


def require_for(options: Mapping[str, Any]) -> tuple[str, ...]:
    raw = options.get("require_for", DEFAULT_REQUIRE_FOR)
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list | tuple) else DEFAULT_REQUIRE_FOR


def retention_options(options: Mapping[str, Any]) -> dict[str, Any]:
    raw = options.get("retention", {})
    return dict(raw) if isinstance(raw, Mapping) else {}


def restore_rehearsal_options(options: Mapping[str, Any]) -> dict[str, Any]:
    raw = options.get("restore_rehearsal", {})
    raw = dict(raw) if isinstance(raw, Mapping) else {}
    return {
        "enabled": bool(raw.get("enabled", profile(options) in {"prod_strict", "regulated"})),
        "environment": str(raw.get("environment") or "stage"),
        "require_clean_target": bool(raw.get("require_clean_target", True)),
    }


def clickhouse_options(options: Mapping[str, Any]) -> dict[str, Any]:
    raw = options.get("clickhouse", {})
    raw = dict(raw) if isinstance(raw, Mapping) else {}
    incremental = raw.get("incremental", False)
    incremental_options = dict(incremental) if isinstance(incremental, Mapping) else {"enabled": bool(incremental)}
    return {
        "destination": str(raw.get("destination") or "Disk('backups', 'dpone/{pack_id}/{table}.zip')"),
        "incremental": incremental_options,
    }


def recovery_catalog_options(manifest: Mapping[str, Any] | None) -> dict[str, Any]:
    options: Mapping[str, Any] | None = manifest or {}
    for key in ("sink", "options", "physical_design", "migration", "recovery"):
        value = options.get(key) if isinstance(options, Mapping) else {}
        options = value if isinstance(value, Mapping) else {}
    raw = dict(options or {})
    catalog = raw.get("catalog", {})
    clickhouse = raw.get("clickhouse", {})
    incremental = dict(clickhouse.get("incremental", {})) if isinstance(clickhouse, Mapping) else {}
    return {
        "enabled": bool(raw.get("enabled", False)),
        "profile": str(raw.get("profile") or "stage"),
        "catalog": dict(catalog) if isinstance(catalog, Mapping) else {},
        "incremental": incremental,
    }


def target_connection_public(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in raw.items()
        if key not in {"password", "token", "secret"} and isinstance(value, str | int | float | bool)
    }


def sink_type(pack: MigrationPack, target_connection: Mapping[str, Any]) -> str:
    return str(target_connection.get("type") or target_connection.get("sink_type") or pack.target.sink_type).lower()


def target_blockers(pack: MigrationPack, target_connection: Mapping[str, Any]) -> tuple[str, ...]:
    actual = sink_type(pack, target_connection)
    if actual != pack.target.sink_type.lower():
        return (f"schema_migration_backup.target_mismatch:{actual}",)
    if actual != "clickhouse":
        return (f"schema_migration_backup.unsupported_target:{actual}",)
    return ()


def requirement_reasons(pack: MigrationPack, required_classes: Sequence[str]) -> tuple[str, ...]:
    detected = set(_detected_reasons(pack))
    return tuple(reason for reason in required_classes if reason in detected)


def destination(options: Mapping[str, Any], pack: MigrationPack) -> str:
    raw = clickhouse_options(options)["destination"]
    pack_safe = pack.pack_id.replace(":", "_")
    pack_short = pack.pack_id.split(":", 1)[-1][:12]
    return raw.format(pack_id=pack_safe, pack_short=pack_short, table=pack.target.table)


def restore_table_name(table: str, pack_id: str) -> str:
    parts = [part for part in table.split(".") if part]
    name = parts[-1] if parts else "table"
    prefix = ".".join(parts[:-1])
    short = pack_id.split(":", 1)[-1][:12]
    restored = f"__dpone_restore_{name}_{short}"
    return f"{prefix}.{restored}" if prefix else restored


def preconditions(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "approval_required": profile(options) in {"prod_strict", "regulated"},
        "lock_required": True,
        "target_fingerprint_required": True,
    }


def backup_recovery_metadata(
    *,
    options: Mapping[str, Any],
    base_restore_point: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    incremental_options = clickhouse_options(options)["incremental"]
    enabled = bool(incremental_options.get("enabled", False))
    if not enabled or not base_restore_point:
        return {
            "backup_kind": "full",
            "base_restore_point_id": None,
            "base_backup_destination": None,
            "chain_id": None,
            "chain_depth": 0,
            "rpo_seconds": 0,
        }
    return {
        "backup_kind": "incremental",
        "base_restore_point_id": base_restore_point.get("restore_point_id"),
        "base_backup_destination": base_restore_point.get("destination"),
        "chain_id": base_restore_point.get("chain_id"),
        "chain_depth": int(base_restore_point.get("chain_depth") or 0) + 1,
        "rpo_seconds": 0,
    }


def approval_blockers(
    pack: MigrationPack, plan: Mapping[str, Any], approval: Mapping[str, Any] | None
) -> tuple[str, ...]:
    if not mapping(plan.get("preconditions")).get("approval_required"):
        return ()
    if approval is None:
        return ("schema_migration_backup.approval_required",)
    if approval.get("pack_id") != pack.pack_id:
        return ("schema_migration_backup.approval_pack_id_mismatch",)
    return ()


def strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _detected_reasons(pack: MigrationPack) -> tuple[str, ...]:
    reasons: list[str] = []
    change_types = {str(change.get("change_type") or change.get("kind") or "") for change in pack.changes}
    risk_tags = {
        str(tag)
        for change in pack.changes
        for tag in (change.get("risk_tags", []) if isinstance(change.get("risk_tags"), list) else [])
    }
    ddl = " ".join(pack.ddl).lower()
    if risk_tags & {"data_destructive"} or change_types & {"drop_column", "drop_table", "truncate", "destructive"}:
        reasons.append("data_destructive")
    if any(token in ddl for token in ("drop table", "drop column", "truncate table")):
        reasons.append("data_destructive")
    if pack.strategy == "shadow" and any(str(phase.get("name")) == "contract" for phase in pack.phases):
        reasons.append("shadow_contract")
    if change_types & {"engine", "partition_by", "order_by", "primary_key", "ttl", "column_type"}:
        reasons.append("physical_layout_change")
    if risk_tags & {"direct_rename"} or change_types & {"direct_rename", "rename"}:
        reasons.append("direct_rename")
    return tuple(dict.fromkeys(reasons))


__all__ = [
    "BACKUP_CERTIFICATE_SCHEMA",
    "BACKUP_PLAN_SCHEMA",
    "BACKUP_RUN_SCHEMA",
    "BACKUP_STAGES",
    "PROFILES",
    "RESTORE_PLAN_SCHEMA",
    "RESTORE_RUN_SCHEMA",
    "approval_blockers",
    "backup_options",
    "backup_recovery_metadata",
    "clickhouse_options",
    "destination",
    "mapping",
    "preconditions",
    "profile",
    "require_for",
    "requirement_reasons",
    "restore_rehearsal_options",
    "restore_table_name",
    "retention_options",
    "recovery_catalog_options",
    "sink_type",
    "strategy",
    "strings",
    "target_blockers",
    "target_connection_public",
]
