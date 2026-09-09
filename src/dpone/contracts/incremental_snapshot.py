"""Contracts for one-snapshot incremental extraction.

The envelope deliberately owns artifacts rather than database clients.  A
source may produce the payload and complete-key artifacts under one MVCC
snapshot, while the target decides how and where to materialize them.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

CheckpointT = TypeVar("CheckpointT")
StateKeyT = TypeVar("StateKeyT")
KEY_HASH_COLUMN = "__dpone__key_hash"
DELTA_HASH_COLUMN = "__dpone__delta_hash"
MSSQL_TEXT_KEY_COLLATION = "Latin1_General_100_BIN2"


def is_mssql_text_key_type(dtype: str) -> bool:
    """Return whether a resolved SQL Server key type requires collation."""

    normalized = re.sub(r"\s+", "", str(dtype).strip().lower())
    return bool(re.fullmatch(r"n?(?:var)?char\((?:max|\d+)\)", normalized))


@dataclass(frozen=True, slots=True)
class KeySnapshotReconciliationPolicy:
    """Portable fail-closed policy normalized from runtime options."""

    enabled: bool = False
    mode: str = "disabled"
    cadence: str = "every_run"
    consistency: str = "same_source_snapshot"
    delete_policy: str = "soft_delete"
    empty_snapshot_policy: str = "fail"
    max_delete_ratio: float = 0.05
    max_delete_rows: int = 100_000

    @classmethod
    def from_runtime(
        cls,
        options: Mapping[str, Any] | None,
        *,
        legacy_enabled: bool = False,
    ) -> KeySnapshotReconciliationPolicy:
        values = options if isinstance(options, Mapping) else {}
        raw = values.get("reconciliation")
        if not isinstance(raw, Mapping):
            return cls(enabled=bool(legacy_enabled), mode="legacy" if legacy_enabled else "disabled")
        empty = raw.get("empty_snapshot")
        empty_values = empty if isinstance(empty, Mapping) else {}
        guards = raw.get("guards")
        guard_values = guards if isinstance(guards, Mapping) else {}
        policy = cls(
            enabled=_strict_bool(raw.get("enabled", True), field="reconciliation.enabled"),
            mode=str(raw.get("mode", "key_snapshot")).strip().lower(),
            cadence=str(raw.get("cadence", "every_run")).strip().lower(),
            consistency=str(raw.get("consistency", "same_source_snapshot")).strip().lower(),
            delete_policy=str(raw.get("delete_policy", "soft_delete")).strip().lower(),
            empty_snapshot_policy=str(empty_values.get("policy", "fail")).strip().lower(),
            max_delete_ratio=float(guard_values.get("max_delete_ratio", 0.05)),
            max_delete_rows=int(guard_values.get("max_delete_rows", 100_000)),
        )
        policy.validate()
        return policy

    @property
    def key_snapshot_enabled(self) -> bool:
        return self.enabled and self.mode == "key_snapshot"

    def validate(self) -> None:
        if self.mode not in {"disabled", "legacy", "key_snapshot"}:
            raise ValueError("reconciliation.mode must be key_snapshot")
        if self.key_snapshot_enabled and self.consistency != "same_source_snapshot":
            raise ValueError("key_snapshot reconciliation requires consistency=same_source_snapshot")
        if self.key_snapshot_enabled and self.cadence != "every_run":
            raise ValueError("key_snapshot reconciliation v1 requires cadence=every_run")
        if self.key_snapshot_enabled and self.delete_policy != "soft_delete":
            raise ValueError("key_snapshot reconciliation v1 requires delete_policy=soft_delete")
        if self.empty_snapshot_policy != "fail":
            raise ValueError("reconciliation.empty_snapshot.policy supports fail only in v1")
        if not 0 <= self.max_delete_ratio <= 1:
            raise ValueError("reconciliation.guards.max_delete_ratio must be between 0 and 1")
        if self.max_delete_rows < 0:
            raise ValueError("reconciliation.guards.max_delete_rows cannot be negative")

    def as_runtime_options(self) -> dict[str, Any]:
        """Return the canonical public/runtime mapping for this policy.

        Keeping serialization next to the immutable policy prevents the
        manifest parser and runtime from maintaining subtly different models.
        """

        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "cadence": self.cadence,
            "consistency": self.consistency,
            "delete_policy": self.delete_policy,
            "empty_snapshot": {"policy": self.empty_snapshot_policy},
            "guards": {
                "max_delete_ratio": self.max_delete_ratio,
                "max_delete_rows": self.max_delete_rows,
            },
        }


@dataclass(frozen=True, slots=True)
class KeySnapshotReceipt:
    """Integrity and completeness evidence for a full source key artifact."""

    snapshot_token: str
    scope_hash: str
    key_columns: tuple[str, ...]
    row_count: int
    checksum: str
    complete: bool

    def __post_init__(self) -> None:
        if not self.snapshot_token.startswith("sha256:"):
            raise ValueError("key_snapshot.snapshot_token must be a sha256 digest")
        if not self.scope_hash.startswith("sha256:"):
            raise ValueError("key_snapshot.scope_hash must be a sha256 digest")
        if not self.key_columns or any(not item for item in self.key_columns):
            raise ValueError("key_snapshot.key_columns must be non-empty")
        if self.row_count < 0:
            raise ValueError("key_snapshot.row_count cannot be negative")
        if not self.checksum.startswith("sha256:"):
            raise ValueError("key_snapshot.checksum must be a sha256 digest")


@dataclass(frozen=True, slots=True)
class DeltaSnapshotReceipt:
    """Immutable completeness evidence for the exported XMin payload."""

    snapshot_token: str
    scope_hash: str
    columns: tuple[str, ...]
    row_count: int
    checksum: str
    complete: bool

    def __post_init__(self) -> None:
        if not self.snapshot_token.startswith("sha256:"):
            raise ValueError("delta_snapshot.snapshot_token must be a sha256 digest")
        if not self.scope_hash.startswith("sha256:"):
            raise ValueError("delta_snapshot.scope_hash must be a sha256 digest")
        if not self.columns or any(not item for item in self.columns):
            raise ValueError("delta_snapshot.columns must be non-empty")
        if self.row_count < 0:
            raise ValueError("delta_snapshot.row_count cannot be negative")
        if not self.checksum.startswith("sha256:"):
            raise ValueError("delta_snapshot.checksum must be a sha256 digest")


@dataclass(frozen=True, slots=True)
class IncrementalSnapshotEnvelope(Generic[CheckpointT, StateKeyT]):
    """Bounded delta plus complete keys captured from one source snapshot.

    ``delta_artifact`` and ``key_artifact`` follow the normal extraction
    artifact protocol.  Keeping them together prevents a sink from applying a
    delete reconciliation against keys from a different source horizon.
    """

    delta_artifact: Any
    key_artifact: Any
    delta_schema: tuple[tuple[str, str], ...]
    key_schema: tuple[tuple[str, str], ...]
    previous_checkpoint: CheckpointT | None
    candidate_checkpoint: CheckpointT
    snapshot_token: str
    scope_hash: str
    state_key: StateKeyT
    baseline: bool = False
    visible_horizon: int | None = None

    def __post_init__(self) -> None:
        if not self.snapshot_token.startswith("sha256:"):
            raise ValueError("incremental_snapshot.snapshot_token must be a sha256 digest")
        if not self.scope_hash.startswith("sha256:"):
            raise ValueError("incremental_snapshot.scope_hash must be a sha256 digest")
        if not self.key_schema:
            raise ValueError("incremental_snapshot.key_schema must be non-empty")
        delta_receipt = self.delta_receipt
        if delta_receipt.snapshot_token != self.snapshot_token:
            raise ValueError("incremental_snapshot.snapshot_token does not match delta receipt")
        if delta_receipt.scope_hash != self.scope_hash:
            raise ValueError("incremental_snapshot.scope_hash does not match delta receipt")
        if delta_receipt.columns != tuple(str(name) for name, _dtype in self.delta_schema):
            raise ValueError("incremental_snapshot.delta_schema does not match delta receipt")
        receipt = self.key_receipt
        if receipt.snapshot_token != self.snapshot_token:
            raise ValueError("incremental_snapshot.snapshot_token does not match key receipt")
        if receipt.scope_hash != self.scope_hash:
            raise ValueError("incremental_snapshot.scope_hash does not match key receipt")
        if self.visible_horizon is not None and self.visible_horizon < 0:
            raise ValueError("incremental_snapshot.visible_horizon cannot be negative")
        safe_value = getattr(self.candidate_checkpoint, "xmin_value", None)
        if self.visible_horizon is not None and safe_value is not None and self.visible_horizon < int(safe_value):
            raise ValueError("incremental_snapshot.visible_horizon precedes safe checkpoint")

    @property
    def safe_checkpoint(self) -> CheckpointT:
        """Return the conservative checkpoint persisted after target commit."""

        return self.candidate_checkpoint

    @property
    def replay_amplification_xids(self) -> int | None:
        """XIDs exported beyond the safe checkpoint and eligible for replay."""

        safe_value = getattr(self.candidate_checkpoint, "xmin_value", None)
        if self.visible_horizon is None or safe_value is None:
            return None
        return max(0, self.visible_horizon - int(safe_value))

    @property
    def estimated_rows(self) -> int | None:
        """Expose the delta estimate through the normal artifact contract."""

        return getattr(self.delta_artifact, "estimated_rows", None)

    @property
    def key_receipt(self) -> KeySnapshotReceipt:
        receipt = getattr(self.key_artifact, "receipt", None)
        if not isinstance(receipt, KeySnapshotReceipt):
            raise ValueError("incremental_snapshot.key_artifact has no complete receipt")
        return receipt

    @property
    def delta_receipt(self) -> DeltaSnapshotReceipt:
        receipt = getattr(self.delta_artifact, "receipt", None)
        if not isinstance(receipt, DeltaSnapshotReceipt):
            raise ValueError("incremental_snapshot.delta_artifact has no complete receipt")
        return receipt

    def materialize(self, staging_manager: Any, load_config: Any, schema: Sequence[tuple[str, str]]) -> Any:
        """Materialize the delta using the regular extraction-artifact port."""

        staged = self.delta_artifact.materialize(staging_manager, load_config, schema)
        if int(getattr(staged, "row_count", -1)) != self.delta_receipt.row_count:
            staged.cleanup()
            raise ValueError("delta_snapshot.staged_row_count does not match receipt")
        return staged

    def materialize_keys(self, staging_manager: Any, load_config: Any) -> Any:
        """Materialize complete keys and revalidate immutable file evidence."""

        staged = self.key_artifact.materialize(staging_manager, load_config, self.key_schema)
        if int(getattr(staged, "row_count", -1)) != self.key_receipt.row_count:
            staged.cleanup()
            raise ValueError("key_snapshot.staged_row_count does not match receipt")
        return staged

    def cleanup(self) -> None:
        """Clean both attempt-local artifacts; cleanup implementations are idempotent."""

        self.delta_artifact.cleanup()
        self.key_artifact.cleanup()


def snapshot_scope_hash(
    *,
    source_database: str,
    source_schema: str,
    source_table: str,
    key_columns: Sequence[str],
    predicate: str | None,
) -> str:
    """Build a deterministic digest for the exact relation scope."""

    return _canonical_digest(
        {
            "source_database": source_database,
            "source_schema": source_schema,
            "source_table": source_table,
            "key_columns": list(key_columns),
            "predicate": predicate or "",
        }
    )


def schema_identity_hash(schema: Sequence[tuple[str, str]]) -> str:
    """Build a deterministic digest for the ordered source-to-target schema."""

    return _canonical_digest({"columns": [[name, dtype] for name, dtype in schema]})


def snapshot_token_digest(raw_snapshot: str) -> str:
    """Reduce a potentially large PostgreSQL XIP list to fixed receipt evidence."""

    value = str(raw_snapshot)
    if not value:
        raise ValueError("postgres snapshot token is empty")
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _canonical_digest(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _strict_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field} must be boolean")


__all__ = [
    "DELTA_HASH_COLUMN",
    "DeltaSnapshotReceipt",
    "IncrementalSnapshotEnvelope",
    "KEY_HASH_COLUMN",
    "KeySnapshotReconciliationPolicy",
    "KeySnapshotReceipt",
    "MSSQL_TEXT_KEY_COLLATION",
    "is_mssql_text_key_type",
    "schema_identity_hash",
    "snapshot_token_digest",
    "snapshot_scope_hash",
]
