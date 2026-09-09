"""Provider-neutral recovery point catalog and restore contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint
from dpone.readiness.schema_migration_recovery_store import RecoveryCatalogStore

RECOVERY_POINT_SCHEMA = "dpone.schema_migration_recovery_point.v1"
CHAIN_VERIFICATION_SCHEMA = "dpone.schema_migration_recovery_chain_verification.v1"
RESTORE_PLAN_SCHEMA = "dpone.schema_migration_recovery_restore_plan.v1"
RESTORE_RUN_SCHEMA = "dpone.schema_migration_recovery_restore_run.v1"
RESTORE_CERTIFICATE_SCHEMA = "dpone.schema_migration_recovery_restore_certificate.v1"
RETENTION_PLAN_SCHEMA = "dpone.schema_migration_recovery_retention_plan.v1"
PROFILES = {"advisory", "stage", "prod_strict", "regulated"}


@dataclass(frozen=True, slots=True)
class RecoveryPointRecorder:
    def record(
        self,
        *,
        backup_certificate: Mapping[str, Any],
        environment: str | None = None,
        mode: str = "gate",
        base_restore_point: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings = _strings(backup_certificate.get("warnings", []))
        if backup_certificate.get("schema_version") != "dpone.schema_migration_backup_certificate.v1":
            blockers.append("schema_migration_recovery.invalid_backup_certificate")
        if backup_certificate.get("status") != "certified":
            code = "schema_migration_recovery.backup_certificate_not_certified"
            (warnings if mode == "observe" else blockers).append(code)
        kind = str(backup_certificate.get("backup_kind") or backup_certificate.get("kind") or "full")
        base_id = _optional_string(backup_certificate.get("base_restore_point_id"))
        chain_id = _chain_id(kind, backup_certificate, base_restore_point)
        chain_depth = _chain_depth(kind, backup_certificate, base_restore_point)
        expired = _is_expired(backup_certificate.get("valid_until"))
        status = "blocked" if blockers else "expired" if expired else "warning" if warnings else "usable"
        point: dict[str, Any] = {
            "schema_version": RECOVERY_POINT_SCHEMA,
            "pack_id": backup_certificate.get("pack_id"),
            "backup_certificate_id": backup_certificate.get("certificate_id"),
            "target": _mapping(backup_certificate.get("target")),
            "environment": environment or backup_certificate.get("environment"),
            "kind": kind,
            "status": status,
            "destination": backup_certificate.get("backup_destination"),
            "base_restore_point_id": base_id,
            "chain_id": chain_id,
            "chain_depth": chain_depth,
            "created_at": str(backup_certificate.get("created_at") or _utc_now()),
            "valid_until": backup_certificate.get("valid_until"),
            "rpo_seconds": int(backup_certificate.get("rpo_seconds") or 0),
            "restore_rehearsal": _restore_rehearsal(backup_certificate),
            "retention": _mapping(backup_certificate.get("retention")),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        point["restore_point_id"] = stable_fingerprint(point)
        return point


@dataclass(frozen=True, slots=True)
class RecoveryPointSelector:
    store: RecoveryCatalogStore

    def latest(self, *, target: str, environment: str, profile: str = "stage") -> dict[str, Any]:
        del profile
        points = [
            point
            for point in self.store.query(target=target, environment=environment)
            if point.get("status") == "usable" and not _is_expired(point.get("valid_until"))
        ]
        points.sort(key=lambda item: (str(item.get("created_at") or ""), str(item.get("restore_point_id") or "")))
        if not points:
            return {
                "schema_version": RECOVERY_POINT_SCHEMA,
                "status": "blocked",
                "target": {"key": target},
                "environment": environment,
                "blockers": ["schema_migration_recovery.restore_point_not_found"],
                "warnings": [],
            }
        return points[-1]


@dataclass(frozen=True, slots=True)
class RecoveryChainVerifier:
    store: RecoveryCatalogStore

    def verify(
        self,
        *,
        restore_point_id: str,
        require_restore_rehearsal: bool = False,
        seed_points: tuple[Mapping[str, Any], ...] = (),
    ) -> dict[str, Any]:
        by_id = {str(point.get("restore_point_id")): dict(point) for point in seed_points}
        chain: list[dict[str, Any]] = []
        blockers: list[str] = []
        current = by_id.get(restore_point_id) or self.store.get(restore_point_id)
        if current is None:
            blockers.append("schema_migration_recovery.restore_point_not_found")
        seen: set[str] = set()
        while current is not None and current.get("restore_point_id") not in seen:
            seen.add(str(current.get("restore_point_id")))
            chain.append(current)
            blockers.extend(_point_blockers(current, require_restore_rehearsal))
            base_id = current.get("base_restore_point_id")
            if not base_id:
                break
            current = by_id.get(str(base_id)) or self.store.get(str(base_id))
            if current is None:
                blockers.append("schema_migration_recovery.base_restore_point_missing")
                break
        chain.reverse()
        blockers.extend(_chain_order_blockers(chain))
        status = "blocked" if blockers else "verified"
        payload: dict[str, Any] = {
            "schema_version": CHAIN_VERIFICATION_SCHEMA,
            "status": status,
            "restore_point_id": restore_point_id,
            "chain_id": chain[-1].get("chain_id") if chain else None,
            "target": _mapping(chain[-1].get("target")) if chain else {},
            "environment": chain[-1].get("environment") if chain else None,
            "chain": chain,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [],
        }
        payload["chain_verification_id"] = stable_fingerprint(payload)
        return payload


@dataclass(frozen=True, slots=True)
class RecoveryRetentionPlanner:
    store: RecoveryCatalogStore

    def plan(self, *, target: str, environment: str) -> dict[str, Any]:
        points = self.store.query(target=target, environment=environment)
        dependent_base_ids = {
            str(point.get("base_restore_point_id")) for point in points if point.get("base_restore_point_id")
        }
        actions = [
            {
                "restore_point_id": point.get("restore_point_id"),
                "action": "protect_base_with_dependents",
                "reason": "incremental_chain_dependency",
            }
            for point in points
            if point.get("restore_point_id") in dependent_base_ids
        ]
        payload: dict[str, Any] = {
            "schema_version": RETENTION_PLAN_SCHEMA,
            "status": "warning" if actions else "planned",
            "target": target,
            "environment": environment,
            "restore_points_count": len(points),
            "actions": actions,
            "blockers": [],
            "warnings": ["schema_migration_recovery.retention_has_protected_bases"] if actions else [],
        }
        payload["retention_plan_id"] = stable_fingerprint(payload)
        return payload


def _chain_id(kind: str, certificate: Mapping[str, Any], base_restore_point: Mapping[str, Any] | None) -> str:
    if kind == "incremental" and base_restore_point and base_restore_point.get("chain_id"):
        return str(base_restore_point["chain_id"])
    return str(certificate.get("chain_id") or stable_fingerprint({"backup": certificate.get("certificate_id")}))


def _chain_depth(kind: str, certificate: Mapping[str, Any], base_restore_point: Mapping[str, Any] | None) -> int:
    if kind == "incremental" and base_restore_point:
        return int(base_restore_point.get("chain_depth") or 0) + 1
    return int(certificate.get("chain_depth") or 0)


def _restore_rehearsal(certificate: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(certificate.get("restore_rehearsal"))
    if raw:
        return raw
    restore_run_id = certificate.get("restore_run_id")
    return {"status": "passed" if restore_run_id else "missing", "restore_run_id": restore_run_id}


def _point_blockers(point: Mapping[str, Any], require_restore_rehearsal: bool) -> list[str]:
    blockers: list[str] = []
    if point.get("status") != "usable":
        blockers.append("schema_migration_recovery.restore_point_not_usable")
    if _is_expired(point.get("valid_until")):
        blockers.append("schema_migration_recovery.restore_point_expired")
    if require_restore_rehearsal and _mapping(point.get("restore_rehearsal")).get("status") != "passed":
        blockers.append("schema_migration_recovery.restore_rehearsal_required")
    return blockers


def _chain_order_blockers(chain: list[Mapping[str, Any]]) -> list[str]:
    depths = [int(point.get("chain_depth") or 0) for point in chain]
    return ["schema_migration_recovery.chain_depth_invalid"] if depths != sorted(depths) else []


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def _optional_string(raw: object) -> str | None:
    return str(raw) if raw not in {None, ""} else None


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _is_expired(raw: object) -> bool:
    if raw in {None, ""}:
        return False
    try:
        value = str(raw).replace("Z", "+00:00")
        valid_until = datetime.fromisoformat(value)
    except ValueError:
        return False
    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=UTC)
    return valid_until <= datetime.now(UTC)


_EXECUTION_EXPORTS = (
    "RecoveryOperationExecutor",
    "RecoveryRestoreCertifier",
    "RecoveryRestorePlanner",
    "RecoveryRestoreRunner",
    "TargetRecoveryDialect",
)


def __getattr__(name: str) -> Any:
    if name in _EXECUTION_EXPORTS:
        module = import_module("dpone.readiness.schema_migration_recovery_execution")
        return getattr(module, name)
    raise AttributeError(name)


__all__ = [
    *_EXECUTION_EXPORTS,
    "RecoveryChainVerifier",
    "RecoveryPointRecorder",
    "RecoveryPointSelector",
    "RecoveryRetentionPlanner",
]
