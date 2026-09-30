"""Immutable native publication wire values; none grant dispatch rights."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Literal

from dpone.contracts.clickhouse_authority import OperationBinding


class NativePublicationError(RuntimeError):
    """No successful transport proof; inspect the original authority, never retry."""

    safe_to_retry = False


@dataclass(frozen=True)
class NativePublicationRequest:
    """One command for validated identifiers, without SQL or catalog evidence.

    The publisher copies fields only after validating the protected original.
    This value is not a selector, authority proof or permission to send SQL.
    """

    binding: OperationBinding
    method: Literal["replace_partition", "exchange", "rename", "noop"]
    query_id: str
    partition_id: str | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.binding, OperationBinding)
            or self.method not in ("replace_partition", "exchange", "rename", "noop")
            or type(self.query_id) is not str
            or not re.fullmatch(r"dpone-publication-[0-9a-f]{64}", self.query_id)
        ):
            raise ValueError("Invalid native publication command or correlation identity")
        partition = self.partition_id
        if self.method == "replace_partition":
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
        match self.method:
            case "replace_partition":
                return f"ALTER TABLE {target} REPLACE PARTITION ID '{self.partition_id}' FROM {candidate}"
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
