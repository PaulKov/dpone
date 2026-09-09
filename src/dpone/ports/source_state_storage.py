"""Source-state storage contracts shared by runtime sinks."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SourceStateKey:
    """Collision-safe identity for one source checkpoint authority."""

    environment: str
    process: str
    source_connection: str
    source_database: str
    source_schema: str
    source_table: str
    target_database: str
    target_schema: str
    target_table: str
    target_identity: bytes
    unique_key: tuple[str, ...]
    schema_hash: str
    scope_hash: str
    contract_version: str = "postgres_xmin_key_snapshot_v2"

    def __post_init__(self) -> None:
        values = (
            self.environment,
            self.process,
            self.source_connection,
            self.source_database,
            self.source_schema,
            self.source_table,
            self.target_database,
            self.target_schema,
            self.target_table,
            self.schema_hash,
            self.scope_hash,
            self.contract_version,
        )
        if any(not str(value).strip() for value in values) or not self.unique_key:
            raise ValueError("source_state.identity is incomplete")
        if not isinstance(self.target_identity, bytes) or len(self.target_identity) != 32:
            raise ValueError("source_state.target_identity must be a 32-byte physical-target digest")

    @property
    def digest(self) -> bytes:
        payload = {
            "contract_version": self.contract_version,
            "environment": self.environment,
            "process": self.process,
            "source_connection": self.source_connection,
            "source_database": self.source_database,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            # Diagnostic target coordinates deliberately do not participate in
            # the checkpoint digest.  SQL Server resolves their physical
            # equivalence under the target database's own collation and binds
            # that result to this binary identity before any source I/O.
            "target_identity": self.target_identity.hex(),
            "unique_key": list(self.unique_key),
            "schema_hash": self.schema_hash,
            "scope_hash": self.scope_hash,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).digest()

    @property
    def unique_key_json(self) -> str:
        return json.dumps(list(self.unique_key), separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class MssqlStateLocation:
    """Three-part SQL Server location for checkpoints and commit receipts."""

    database: str
    schema: str
    table: str
    receipt_table: str
    repair_authority_table: str = "dpone_repair_authority"
    repair_consumption_table: str = "dpone_repair_authority_consumption"

    def __post_init__(self) -> None:
        values = (
            self.database,
            self.schema,
            self.table,
            self.receipt_table,
            self.repair_authority_table,
            self.repair_consumption_table,
        )
        if any(not str(value).strip() for value in values):
            raise ValueError("mssql state location requires all state and repair table names")

    @property
    def state_table_name(self) -> str:
        return _qualified(self.database, self.schema, self.table)

    @property
    def receipt_table_name(self) -> str:
        return _qualified(self.database, self.schema, self.receipt_table)

    @property
    def repair_authority_table_name(self) -> str:
        return _qualified(self.database, self.schema, self.repair_authority_table)

    @property
    def repair_consumption_table_name(self) -> str:
        return _qualified(self.database, self.schema, self.repair_consumption_table)


@dataclass(frozen=True, slots=True)
class CheckpointCommitOutcome:
    """Receipt proving that one candidate checkpoint was accepted."""

    receipt_id: str
    candidate_xmin: int
    committed: bool = True
    candidate_revision: int = 0
    publication_receipt_id: str | None = None

    def __post_init__(self) -> None:
        if self.publication_receipt_id is not None and not self.publication_receipt_id.strip():
            raise ValueError("checkpoint publication receipt is empty")


class SourceStateTransactionPort(Protocol):
    """Advance state using a transaction executor owned by the target sink."""

    def compare_and_set_with_receipt(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        expected: Any | None,
        candidate: Any,
        load_id: str,
        snapshot_token: str,
        publication_receipt_id: str | None = None,
    ) -> CheckpointCommitOutcome: ...

    def probe_receipt(
        self,
        *,
        key: SourceStateKey,
        load_id: str,
        executor: Any | None = None,
    ) -> CheckpointCommitOutcome | None: ...

    def bind_seed_publication_receipt(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        load_id: str,
        receipt_id: str,
        candidate_xmin: int,
        snapshot_token: str,
        publication_receipt_id: str,
    ) -> CheckpointCommitOutcome: ...

    def assert_target_authority(self, *, executor: Any, key: SourceStateKey) -> None: ...

    def assert_physical_target_identity(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
    ) -> None: ...

    def assert_or_transfer_target_authority(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        authority: Any | None,
    ) -> None: ...

    def admit_repair_authority(self, **kwargs: Any) -> Any | None: ...

    def consume_repair_authority(self, **kwargs: Any) -> None: ...

    def preview_repair_authority(self, **kwargs: Any) -> Any: ...


class SourceStateStoragePort(Protocol):
    """Persist source state by source object identity after successful loads."""

    def save_state(self, source_schema: str, source_table: str, state: Any) -> None:
        """Persist state for a source schema/table pair."""


def _qualified(database: str, schema: str, table: str) -> str:
    return ".".join(_quote_identifier(value) for value in (database, schema, table))


def _quote_identifier(value: str) -> str:
    return f"[{str(value).replace(']', ']]')}]"


__all__ = [
    "CheckpointCommitOutcome",
    "MssqlStateLocation",
    "SourceStateKey",
    "SourceStateStoragePort",
    "SourceStateTransactionPort",
]
