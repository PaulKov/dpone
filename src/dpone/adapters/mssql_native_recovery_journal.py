"""Read-only SQLite adapter for private MSSQL native recovery journals."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone.adapters.mssql_native_chunks_journal_v2_events import event_key, validate_projection
from dpone.adapters.mssql_native_publication_journal import validate_publication_state
from dpone.contracts.mssql_native_recovery import (
    NativeTargetCustodyRecord,
    NativeVerificationIdentityV2,
    canonical_json_bytes,
    classify_mssql_native_recovery,
    strict_json_object,
)

_PREFIX = "mssql-native-chunks-v2/"
_MAX_RECORD_BYTES = 8 << 20
_MAX_INVOCATIONS = 1000
_CUSTODY_PREFIX = "mssql-native-target-custody-v1/"
_PLAN_PREFIX = "mssql-native/recovery-plan-v1/"


@dataclass(frozen=True, slots=True)
class MssqlNativeRecoverySnapshot:
    """Validated private recovery authority for one exact invocation."""

    identity: NativeVerificationIdentityV2
    projection: dict[str, Any]
    custody: NativeTargetCustodyRecord | None
    artifact_sha256: str
    recovery_plan: dict[str, Any] | None = None
    recovery_plan_sha256: str | None = None


class MssqlNativeRecoveryJournalReader:
    """Validate private authority and expose only opaque, bounded projections."""

    def __init__(self, journal_root: Path) -> None:
        path = journal_root / "window-state.sqlite" if journal_root.is_dir() else journal_root
        if not path.is_file():
            raise ValueError("mssql_native.recovery_journal_unavailable")
        self._path = path.resolve()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self._path}?mode=ro", uri=True, timeout=5)

    @property
    def path(self) -> Path:
        """Return the exact validated journal path for fenced mutation composition."""

        return self._path

    def _record(self, database: sqlite3.Connection, key: str) -> str:
        row = database.execute("SELECT payload FROM window_records WHERE key=?", (key,)).fetchone()
        if row is None or not isinstance(row[0], str) or len(row[0].encode()) > _MAX_RECORD_BYTES:
            raise ValueError("mssql_native.recovery_journal_unavailable")
        return row[0]

    def load(self, invocation_id: str) -> MssqlNativeRecoverySnapshot:
        """Authenticate one complete private authority without mutating it."""

        if len(invocation_id) != 64 or any(character not in "0123456789abcdef" for character in invocation_id):
            raise ValueError("mssql_native.recovery_identity_invalid")
        root_key = _PREFIX + invocation_id
        with self._connect() as database:
            payload = self._record(database, root_key)
            projection = strict_json_object(payload)
            identity = NativeVerificationIdentityV2.from_document(projection.get("identity"))
            if identity.invocation_key != invocation_id:
                raise ValueError("mssql_native.recovery_identity_mismatch")
            for event in validate_projection(identity, projection):
                stored = self._record(
                    database,
                    event_key(root_key, event["ordinal"], event["attempt_id"], event["sequence"]),
                )
                if stored.encode() != canonical_json_bytes(event):
                    raise ValueError("mssql_native.recovery_event_changed")
            validate_publication_state(projection)
            custody = self._custody(database, identity)
            recovery_plan, recovery_plan_sha256 = self._recovery_plan(database, identity)
        return MssqlNativeRecoverySnapshot(
            identity=identity,
            projection=projection,
            custody=custody,
            artifact_sha256=hashlib.sha256(payload.encode()).hexdigest(),
            recovery_plan=recovery_plan,
            recovery_plan_sha256=recovery_plan_sha256,
        )

    def inspect(self, invocation_id: str) -> dict[str, Any]:
        """Verify one complete event authority and return a coordinate-free result."""

        snapshot = self.load(invocation_id)
        identity, projection, custody = snapshot.identity, snapshot.projection, snapshot.custody
        state, diagnostic, actions = classify_mssql_native_recovery(
            projection,
            custody_state=None if custody is None else custody.state,
        )
        return {
            "schema_version": 2,
            "kind": "dpone.mssql-native-recovery.v2",
            "invocation_id": invocation_id,
            "identity_sha256": identity.invocation_key,
            "state": state,
            "diagnostic_code": diagnostic,
            "permitted_actions": actions,
            "artifact_refs": [
                snapshot.artifact_sha256,
                *([] if snapshot.recovery_plan_sha256 is None else [snapshot.recovery_plan_sha256]),
            ],
            "attempts": _attempt_summaries(projection),
        }

    def inspect_all(self) -> dict[str, Any]:
        """List bounded root authorities without exposing target or stage coordinates."""
        with self._connect() as database:
            rows = database.execute(
                "SELECT key FROM window_records WHERE key LIKE ? AND key NOT LIKE ? ORDER BY key LIMIT ?",
                (_PREFIX + "%", _PREFIX + "%/%", _MAX_INVOCATIONS + 1),
            ).fetchall()
            retained_custody_count = self._held_custody_count(database)
        roots = [key for (key,) in rows if isinstance(key, str)]
        if len(roots) > _MAX_INVOCATIONS:
            raise ValueError("mssql_native.recovery_inventory_limit")
        items = [self.inspect(key.removeprefix(_PREFIX)) for key in roots]
        return {
            "schema_version": 1,
            "kind": "dpone.mssql-native-recovery-index.v1",
            "retained_custody_count": retained_custody_count,
            "items": items,
        }

    def _custody(
        self, database: sqlite3.Connection, identity: NativeVerificationIdentityV2
    ) -> NativeTargetCustodyRecord | None:
        target_sha256 = hashlib.sha256(identity.plan.target_id.encode()).hexdigest()
        row = database.execute(
            "SELECT payload FROM window_records WHERE key=?",
            (_CUSTODY_PREFIX + target_sha256,),
        ).fetchone()
        if row is None:
            return None
        if not isinstance(row[0], str) or len(row[0].encode()) > _MAX_RECORD_BYTES:
            raise ValueError("mssql_native.recovery_custody_invalid")
        record = NativeTargetCustodyRecord.decode(row[0])
        if record.target_id_sha256 != target_sha256:
            raise ValueError("mssql_native.recovery_custody_invalid")
        return record

    def _held_custody_count(self, database: sqlite3.Connection) -> int:
        rows = database.execute(
            "SELECT key, payload FROM window_records WHERE key LIKE ? ORDER BY key LIMIT ?",
            (_CUSTODY_PREFIX + "%", _MAX_INVOCATIONS + 1),
        ).fetchall()
        if len(rows) > _MAX_INVOCATIONS:
            raise ValueError("mssql_native.recovery_inventory_limit")
        held = 0
        for key, payload in rows:
            if not isinstance(key, str) or not isinstance(payload, str) or len(payload.encode()) > _MAX_RECORD_BYTES:
                raise ValueError("mssql_native.recovery_custody_invalid")
            record = NativeTargetCustodyRecord.decode(payload)
            if key != _CUSTODY_PREFIX + record.target_id_sha256:
                raise ValueError("mssql_native.recovery_custody_invalid")
            held += record.state == "held"
        return held

    def _recovery_plan(
        self, database: sqlite3.Connection, identity: NativeVerificationIdentityV2
    ) -> tuple[dict[str, Any] | None, str | None]:
        row = database.execute(
            "SELECT payload FROM window_records WHERE key=?",
            (_PLAN_PREFIX + identity.invocation_key,),
        ).fetchone()
        if row is None:
            return None, None
        if not isinstance(row[0], str) or len(row[0].encode()) > _MAX_RECORD_BYTES:
            raise ValueError("mssql_native.recovery_plan_invalid")
        plan = strict_json_object(row[0])
        version = plan.get("schema_version")
        fields = {"schema_version", "kind", "invocation_id", "schema", "recovery_authority_v1"}
        if version == 2:
            fields.add("window")
        if (
            set(plan) != fields
            or version not in {1, 2}
            or plan.get("kind") != f"dpone.mssql-native-recovery-plan.v{version}"
            or plan.get("invocation_id") != identity.invocation_key
            or not isinstance(plan.get("schema"), list)
            or not isinstance(plan.get("recovery_authority_v1"), dict)
            or (version == 2 and not _valid_window(plan.get("window")))
        ):
            raise ValueError("mssql_native.recovery_plan_invalid")
        return plan, hashlib.sha256(row[0].encode()).hexdigest()


def _attempt_summaries(projection: dict[str, Any]) -> list[dict[str, Any]]:
    summaries = []
    for attempt_id, chain in projection["events"].items():
        if not chain:
            continue
        terminal = chain[-1]
        stage = terminal.get("stage_binding")
        stage_id = stage.get("stage_id") if isinstance(stage, dict) else None
        summaries.append(
            {
                "ordinal": terminal["ordinal"],
                "attempt_id_sha256": hashlib.sha256(attempt_id.encode()).hexdigest(),
                "stage_id_sha256": None if stage_id is None else hashlib.sha256(stage_id.encode()).hexdigest(),
                "terminal_event": terminal["event"],
            }
        )
    return sorted(summaries, key=lambda item: (item["ordinal"], item["attempt_id_sha256"]))


def _valid_window(value: object) -> bool:
    if value is None:
        return True
    if not isinstance(value, dict) or set(value) != {"column", "start", "end"}:
        return False
    if not all(isinstance(value[field], str) and value[field] for field in ("column", "start", "end")):
        return False
    try:
        start = datetime.fromisoformat(value["start"])
        end = datetime.fromisoformat(value["end"])
    except ValueError:
        return False
    return start.tzinfo is not None and end.tzinfo is not None and start < end


__all__ = ["MssqlNativeRecoveryJournalReader", "MssqlNativeRecoverySnapshot"]
