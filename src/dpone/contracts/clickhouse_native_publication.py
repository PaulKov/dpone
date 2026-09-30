"""Fixed native DDL values; neither requests nor completion DTOs grant authority."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from dpone.contracts.clickhouse_authority import OperationBinding
from dpone.contracts.clickhouse_publication import PublicationIntent


class NativePublicationError(RuntimeError):
    """No successful transport proof; inspect the original authority, never retry."""

    safe_to_retry = False


@dataclass(frozen=True)
class NativePublicationRequest:
    """One fixed method for original simple identifiers, without caller SQL."""

    binding: OperationBinding
    intent: PublicationIntent

    def __post_init__(self) -> None:
        if not isinstance(self.binding, OperationBinding) or not isinstance(self.intent, PublicationIntent):
            raise ValueError("Expected original publication binding and intent")
        subject = self.binding.subject
        if (
            self.intent.operation_id != self.binding.operation_id
            or self.intent.before.subject
            != (subject.server_id, subject.database, subject.target, self.binding.candidate)
            or self.intent.method not in ("replace_partition", "exchange", "rename", "noop")
        ):
            raise ValueError("Publication request differs from its registered binding")
        self.intent.before.require_supported()
        partition = self.intent.partition_id
        if self.intent.method == "replace_partition":
            if type(partition) is not str or not re.fullmatch(r"[A-Za-z0-9_-]+", partition):
                raise ValueError("Partition ID is outside the native publication profile")
        elif partition is not None:
            raise ValueError("Only partition replacement accepts a partition ID")

    @property
    def statement(self) -> str | None:
        """Deterministic SQL; canonical ID 'all' never implies tuple() syntax."""
        subject = self.binding.subject
        target = f"`{subject.database}`.`{subject.target}`"
        candidate = f"`{subject.database}`.`{self.binding.candidate}`"
        match self.intent.method:
            case "replace_partition":
                return f"ALTER TABLE {target} REPLACE PARTITION ID '{self.intent.partition_id}' FROM {candidate}"
            case "exchange":
                return f"EXCHANGE TABLES {target} AND {candidate}"
            case "rename":
                return f"RENAME TABLE {candidate} TO {target}"
            case _:
                return None


@dataclass(frozen=True)
class NativePublicationCompletion:
    """Trusted transport output after EOS, not reconstructible from its digest."""

    operation_id: str
    query_id: str
    server_id: str
    statement_digest: str
    server_version: tuple[int, int, int]
    server_revision: int
    driver_version: str

    def __post_init__(self) -> None:
        if (
            any(
                type(value) is not str or not value or value != value.strip() or "\x00" in value
                for value in (self.operation_id, self.query_id, self.server_id, self.driver_version)
            )
            or not re.fullmatch(r"[0-9a-f]{64}", self.statement_digest)
            or type(self.server_version) is not tuple
            or len(self.server_version) != 3
            or any(type(value) is not int or value < 0 for value in self.server_version)
            or type(self.server_revision) is not int
            or self.server_revision <= 0
        ):
            raise ValueError("Invalid native completion identity")

    @property
    def digest(self) -> str:
        payload = asdict(self) | {"profile": "dpone.clickhouse.native-completion.v1"}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
