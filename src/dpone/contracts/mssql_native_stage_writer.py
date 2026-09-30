"""Backend-neutral contracts for one sealed MSSQL native stage write."""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from dpone.contracts.mssql_native_writer import (
    BCP_STAGE_PROOF,
    SQLCLIENT_SESSION_PROOF,
    WRITER_OUTCOMES,
    is_qualified_native_stage,
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COLUMN_NAME = re.compile(r"[^\x00-\x1f\x7f]+\Z")
_SQLCLIENT_V1_TYPES = frozenset({"bigint", "float(53)", "nvarchar(max)", "datetime2(6)"})
_PROOF_CAPABILITIES = frozenset({BCP_STAGE_PROOF, SQLCLIENT_SESSION_PROOF})
_PROTOCOLS = frozenset({"dpone.mssql-bcp.canonical.v1", "dpone.mssql-sqlclient.ipc.v1", "dpone.mssql-sqlclient.ipc.v2"})


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _is_seconds(value: object) -> bool:
    return value is None or type(value) is float and math.isfinite(value) and value >= 0.0


@dataclass(frozen=True, slots=True)
class NativeStageColumnMapping:
    """One ordered native-file field mapped to one SQL Server stage column."""

    ordinal: int
    target_name: str
    target_type: str
    nullable: bool

    def __post_init__(self) -> None:
        if (
            type(self.ordinal) is not int
            or self.ordinal < 0
            or type(self.target_name) is not str
            or _COLUMN_NAME.fullmatch(self.target_name) is None
            or not self.target_name.strip()
            or len(self.target_name) > 128
            or self.target_type not in _SQLCLIENT_V1_TYPES
            or type(self.nullable) is not bool
        ):
            raise ValueError("mssql_native.invalid_stage_column_mapping")


@dataclass(frozen=True, slots=True)
class NativeStageWriteRequest:
    """Immutable authority for one writer to consume one sealed native file."""

    attempt_id: str
    qualified_stage: str
    stage_id_sha256: str
    owner_binding_sha256: str
    object_id: int
    schema_sha256: str
    file_path: Path
    expected_rows: int
    encoded_bytes: int
    max_row_bytes: int
    file_sha256: str
    grant_token_sha256: str
    proof_capability: str
    wire_layout_sha256: str
    columns: tuple[NativeStageColumnMapping, ...]
    layout_version: int = 1

    def __post_init__(self) -> None:
        ordinals = tuple(column.ordinal for column in self.columns)
        names = tuple(column.target_name.casefold() for column in self.columns)
        if (
            type(self.attempt_id) is not str
            or not self.attempt_id
            or not is_qualified_native_stage(self.qualified_stage)
            or not all(
                _is_sha256(value)
                for value in (
                    self.stage_id_sha256,
                    self.owner_binding_sha256,
                    self.schema_sha256,
                    self.file_sha256,
                    self.grant_token_sha256,
                    self.wire_layout_sha256,
                )
            )
            or type(self.object_id) is not int
            or self.object_id < 1
            or not isinstance(self.file_path, Path)
            or type(self.expected_rows) is not int
            or self.expected_rows < 0
            or type(self.encoded_bytes) is not int
            or self.encoded_bytes < 0
            or type(self.max_row_bytes) is not int
            or self.max_row_bytes < 1
            or (self.expected_rows > 0 and self.max_row_bytes > self.encoded_bytes)
            or self.proof_capability not in _PROOF_CAPABILITIES
            or type(self.layout_version) is not int
            or self.layout_version not in {1, 2}
            or (self.layout_version == 2 and self.proof_capability != SQLCLIENT_SESSION_PROOF)
            or type(self.columns) is not tuple
            or not self.columns
            or any(not isinstance(column, NativeStageColumnMapping) for column in self.columns)
            or ordinals != tuple(range(len(self.columns)))
            or len(set(names)) != len(names)
        ):
            raise ValueError("mssql_native.invalid_stage_write_request")


@dataclass(frozen=True, slots=True)
class OperationDeadline:
    """One absolute monotonic deadline shared by every phase of a writer attempt."""

    expires_at_monotonic: float
    clock: Callable[[], float] = field(default=time.monotonic, repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.expires_at_monotonic) is not float or not math.isfinite(self.expires_at_monotonic):
            raise ValueError("mssql_native.invalid_writer_deadline")

    def remaining_seconds(self) -> float:
        """Return a positive remaining budget or signal deadline exhaustion."""
        remaining = self.expires_at_monotonic - self.clock()
        if remaining <= 0.0:
            raise TimeoutError("mssql_native.writer_deadline_expired")
        return remaining


@dataclass(frozen=True, slots=True)
class NativeStageWriteMetrics:
    """Non-sensitive phase durations; ``None`` means the phase was unavailable."""

    launch_seconds: float | None
    write_seconds: float | None
    dispose_seconds: float | None

    def __post_init__(self) -> None:
        if not all(_is_seconds(value) for value in (self.launch_seconds, self.write_seconds, self.dispose_seconds)):
            raise ValueError("mssql_native.invalid_stage_write_metrics")

    @property
    def total_seconds(self) -> float | None:
        values = (self.launch_seconds, self.write_seconds, self.dispose_seconds)
        if any(value is None for value in values):
            return None
        return float(sum(value for value in values if value is not None))


@dataclass(frozen=True, slots=True)
class NativeStageWriteObservation:
    """Writer terminal observation; it never proves target content equality."""

    attempt_id: str
    input_rows_consumed: int | None
    classification: str
    writer_identity_sha256: str
    runtime_identity_sha256: str
    protocol: str
    metrics: NativeStageWriteMetrics

    def __post_init__(self) -> None:
        if (
            type(self.attempt_id) is not str
            or not self.attempt_id
            or (
                self.input_rows_consumed is not None
                and (type(self.input_rows_consumed) is not int or self.input_rows_consumed < 0)
            )
            or self.classification not in WRITER_OUTCOMES
            or (self.classification == "success") != (self.input_rows_consumed is not None)
            or not _is_sha256(self.writer_identity_sha256)
            or not _is_sha256(self.runtime_identity_sha256)
            or self.protocol not in _PROTOCOLS
            or not isinstance(self.metrics, NativeStageWriteMetrics)
        ):
            raise ValueError("mssql_native.invalid_stage_write_observation")

    @property
    def positive_terminal(self) -> bool:
        return self.classification == "success"


__all__ = [
    "NativeStageColumnMapping",
    "NativeStageWriteMetrics",
    "NativeStageWriteObservation",
    "NativeStageWriteRequest",
    "OperationDeadline",
]
