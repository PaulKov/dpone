"""Generic schema/DDL migration control-plane models.

The module is intentionally target-agnostic: it knows how to fingerprint,
package, and ledger migration evidence, but not how to connect to a database.
Target-backed execution can plug in behind the same package/ledger contracts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

MIGRATION_PACK_SCHEMA = "dpone.schema_migration_pack.v1"
LEDGER_RECORD_SCHEMA = "dpone.schema_migration_ledger_record.v1"
LEDGER_SCHEMA = "dpone.schema_migration_ledger.v1"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True, slots=True)
class MigrationTarget:
    sink_type: str
    table: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MigrationPack:
    pack_id: str
    target: MigrationTarget
    desired_fingerprint: str
    actual_fingerprint: str | None
    desired: dict[str, Any]
    actual: dict[str, Any] | None = None
    strategy: str = "block"
    changes: tuple[dict[str, Any], ...] = ()
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    ddl: tuple[str, ...] = ()
    phases: tuple[dict[str, Any], ...] = ()
    rollback: dict[str, Any] = field(default_factory=lambda: {"supported": False, "ddl": []})
    created_at: str = field(default_factory=_utc_now)

    @classmethod
    def build(
        cls,
        *,
        target: MigrationTarget,
        desired: dict[str, Any],
        actual: dict[str, Any] | None = None,
        changes: tuple[dict[str, Any], ...] = (),
        blockers: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
        ddl: tuple[str, ...] = (),
        strategy: str = "block",
        phases: tuple[dict[str, Any], ...] = (),
        rollback: dict[str, Any] | None = None,
    ) -> MigrationPack:
        desired_fingerprint = stable_fingerprint(desired)
        actual_fingerprint = stable_fingerprint(actual) if actual is not None else None
        identity = {
            "target": target.to_dict(),
            "desired_fingerprint": desired_fingerprint,
            "actual_fingerprint": actual_fingerprint,
            "strategy": strategy,
            "changes": list(changes),
            "blockers": list(blockers),
            "ddl": list(ddl),
            "phases": list(phases),
            "rollback": rollback or {"supported": False, "ddl": []},
        }
        return cls(
            pack_id=stable_fingerprint(identity),
            target=target,
            desired_fingerprint=desired_fingerprint,
            actual_fingerprint=actual_fingerprint,
            desired=desired,
            actual=actual,
            strategy=strategy,
            changes=changes,
            blockers=blockers,
            warnings=warnings,
            ddl=ddl,
            phases=phases,
            rollback=rollback or {"supported": False, "ddl": []},
        )

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> MigrationPack:
        target = raw.get("target", {})
        if not isinstance(target, dict):
            raise ValueError("migration pack target must be an object")
        return cls(
            pack_id=str(raw["pack_id"]),
            target=MigrationTarget(sink_type=str(target.get("sink_type", "")), table=str(target.get("table", ""))),
            desired_fingerprint=str(raw["desired_fingerprint"]),
            actual_fingerprint=_optional_str(raw.get("actual_fingerprint")),
            desired=_object(raw.get("desired")),
            actual=_optional_object(raw.get("actual")),
            strategy=str(raw.get("strategy", "block")),
            changes=tuple(_objects(raw.get("changes", []))),
            blockers=tuple(str(item) for item in raw.get("blockers", [])),
            warnings=tuple(str(item) for item in raw.get("warnings", [])),
            ddl=tuple(str(item) for item in raw.get("ddl", [])),
            phases=tuple(_objects(raw.get("phases", []))),
            rollback=_object(raw.get("rollback", {"supported": False, "ddl": []})),
            created_at=str(raw.get("created_at") or _utc_now()),
        )

    def to_dict(self, *, command: str = "plan") -> dict[str, Any]:
        return {
            "schema_version": MIGRATION_PACK_SCHEMA,
            "command": command,
            "pack_id": self.pack_id,
            "created_at": self.created_at,
            "target": self.target.to_dict(),
            "desired_fingerprint": self.desired_fingerprint,
            "actual_fingerprint": self.actual_fingerprint,
            "desired": self.desired,
            "actual": self.actual,
            "strategy": self.strategy,
            "changes": list(self.changes),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "ddl": list(self.ddl),
            "phases": list(self.phases),
            "rollback": dict(self.rollback),
        }


@dataclass(frozen=True, slots=True)
class MigrationLedgerRecord:
    pack_id: str
    status: str
    target: MigrationTarget
    desired_fingerprint: str | None = None
    actual_fingerprint: str | None = None
    phase: str | None = None
    environment: str | None = None
    promotion_id: str | None = None
    certification_id: str | None = None
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    operations: tuple[dict[str, Any], ...] = ()
    applied_at: str = field(default_factory=_utc_now)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": LEDGER_RECORD_SCHEMA,
            "pack_id": self.pack_id,
            "status": self.status,
            "target": self.target.to_dict(),
            "desired_fingerprint": self.desired_fingerprint,
            "actual_fingerprint": self.actual_fingerprint,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "applied_at": self.applied_at,
        }
        if self.phase:
            payload["phase"] = self.phase
        if self.environment:
            payload["environment"] = self.environment
        if self.promotion_id:
            payload["promotion_id"] = self.promotion_id
        if self.certification_id:
            payload["certification_id"] = self.certification_id
        if self.operations:
            payload["operations"] = list(self.operations)
        return payload


class MigrationLedgerStore(Protocol):
    def append(self, record: MigrationLedgerRecord) -> None: ...

    def records(self) -> tuple[dict[str, Any], ...]: ...


@dataclass(frozen=True, slots=True)
class ArtifactMigrationLedgerStore:
    path: Path

    def append(self, record: MigrationLedgerRecord) -> None:
        payload = {"schema_version": LEDGER_SCHEMA, "records": list(self.records())}
        payload["records"].append(record.to_dict())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(_canonical_json(payload) + "\n", encoding="utf-8")

    def records(self) -> tuple[dict[str, Any], ...]:
        if not self.path.exists():
            return ()
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("migration ledger must be a JSON object")
        records = raw.get("records", [])
        if not isinstance(records, list):
            raise ValueError("migration ledger records must be a list")
        return tuple(item for item in records if isinstance(item, dict))


def stable_fingerprint(payload: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def read_json_object(path: str | Path) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return raw


def migration_blocked_result(command: str, pack: MigrationPack, blockers: tuple[str, ...]) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_migration_result.v1",
        "command": command,
        "status": "blocked",
        "pack_id": pack.pack_id,
        "target": pack.target.to_dict(),
        "blockers": list(blockers),
        "warnings": list(pack.warnings),
    }


def migration_blocked_result_by_id(
    command: str,
    pack_id: str | None,
    blockers: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_migration_result.v1",
        "command": command,
        "status": "blocked",
        "pack_id": pack_id,
        "blockers": list(blockers),
        "warnings": [],
    }


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _objects(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, dict)]


def _object(raw: Any) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, dict) else {}


def _optional_object(raw: Any) -> dict[str, Any] | None:
    return dict(raw) if isinstance(raw, dict) else None


def _optional_str(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw)
    return text or None


__all__ = [
    "ArtifactMigrationLedgerStore",
    "LEDGER_RECORD_SCHEMA",
    "LEDGER_SCHEMA",
    "MIGRATION_PACK_SCHEMA",
    "MigrationLedgerRecord",
    "MigrationLedgerStore",
    "MigrationPack",
    "MigrationTarget",
    "migration_blocked_result",
    "migration_blocked_result_by_id",
    "read_json_object",
    "stable_fingerprint",
]
