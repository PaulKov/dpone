"""Immutable physical-chunk identities for one acquired ClickHouse stream."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

LEGACY_NATIVE_LIMIT_FIELDS = (
    "max_total_encoded_bytes",
    "stage_allocated_bytes_stop_threshold",
    "max_rows",
    "max_bytes",
    "max_row_bytes",
    "max_pending",
    "max_staging_tables",
    "parallelism",
)
NATIVE_STAGE_LIMIT_FIELDS = ("encoding_parallelism", "import_parallelism")


@dataclass(frozen=True)
class NativeBulkTransportPolicy:
    """Explicit TDS input and finite worker bounds; omission continues to use BCP.

    Values are never coerced, inferred from installed drivers, or clamped. The
    resolved mapping forms part of durable invocation identity, so changing a
    limit requires settling the previous invocation before starting another.
    """

    backend: str
    input: str
    max_worker_address_space_bytes: int
    batch_rows: int = 65536
    startup_timeout_seconds: int = 30
    operation_timeout_seconds: int = 300
    terminate_timeout_seconds: int = 10
    drop_timeout_seconds: int = 30
    max_input_batch_bytes: int | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        for name, admitted in (("backend", ("mssql_python", "mssql_sqlclient")), ("input", ("rows", "arrow"))):
            if type(getattr(self, name)) is not str or getattr(self, name) not in admitted:
                raise ValueError(f"mssql_native.transport_invalid:{name}")
        bounds = {
            "max_worker_address_space_bytes": (8 << 30 if self.backend == "mssql_sqlclient" else 64 << 20, 16 << 30),
            "batch_rows": (1, 65536),
            "startup_timeout_seconds": (1, 120),
            "operation_timeout_seconds": (1, 3600),
            "terminate_timeout_seconds": (1, 60),
            "drop_timeout_seconds": (1, 300),
        }
        for name, (low, high) in bounds.items():
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"mssql_native.transport_invalid:{name}")
        if self.operation_timeout_seconds < self.startup_timeout_seconds:
            raise ValueError("mssql_native.transport_invalid:operation_timeout_seconds")
        if self.backend == "mssql_sqlclient":
            if self.max_input_batch_bytes is None:
                object.__setattr__(self, "max_input_batch_bytes", 64 << 20)
            if type(self.max_input_batch_bytes) is not int or not 1 << 20 <= self.max_input_batch_bytes <= 256 << 20:
                raise ValueError("mssql_native.transport_invalid:max_input_batch_bytes")
        elif self.max_input_batch_bytes is not None:
            raise ValueError("mssql_native.transport_invalid:max_input_batch_bytes")

    @classmethod
    def from_mapping(cls, value: Any) -> NativeBulkTransportPolicy:
        """Parse a closed explicit opt-in without leaking authored values in errors."""
        if not isinstance(value, Mapping):
            raise ValueError("mssql_native.transport_invalid:transport")
        if set(value) - set(cls.__dataclass_fields__):
            raise ValueError("mssql_native.transport_invalid:unknown_field")
        for required in ("backend", "input", "max_worker_address_space_bytes"):
            if required not in value:
                raise ValueError(f"mssql_native.transport_required:{required}")
        if "max_input_batch_bytes" in value and (
            value.get("backend") != "mssql_sqlclient" or type(value["max_input_batch_bytes"]) is not int
        ):
            raise ValueError("mssql_native.transport_invalid:max_input_batch_bytes")
        return cls(**dict(value))

    def to_dict(self) -> dict[str, str | int]:
        """Return a detached canonical record including every resolved bound."""
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
            if name != "max_input_batch_bytes" or self.backend == "mssql_sqlclient"
        }


@dataclass(frozen=True)
class NativeChunkLimits:
    """Encoded/IPC bounds; SQL allocation is an observed stop threshold."""

    max_total_encoded_bytes: int
    stage_allocated_bytes_stop_threshold: int
    max_rows: int = 65536
    max_bytes: int = 16777216
    max_row_bytes: int = 1048576
    max_pending: int = 2
    max_staging_tables: int = 1024
    parallelism: int = 1
    encoding_parallelism: int | None = field(default=None, kw_only=True)
    import_parallelism: int | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        caps = {
            "max_rows": 1000000,
            "max_bytes": 1073741824,
            "max_pending": 64,
            "parallelism": 64,
            **dict.fromkeys(NATIVE_STAGE_LIMIT_FIELDS, 64),
        }
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if name in NATIVE_STAGE_LIMIT_FIELDS and value is None:
                continue
            if type(value) is not int or value < 1 or value > caps.get(name, value):
                raise ValueError(f"mssql_native.invalid_limit:{name}")
        if self.max_row_bytes > self.max_bytes:
            raise ValueError("mssql_native.max_row_bytes_exceeds_chunk")

    @property
    def effective_encoding_parallelism(self) -> int:
        """Resolve the CPU worker count independently of import concurrency."""
        return self.parallelism if self.encoding_parallelism is None else self.encoding_parallelism

    @property
    def effective_import_parallelism(self) -> int:
        """Resolve concurrent import/verify tasks, not all target connections."""
        return self.parallelism if self.import_parallelism is None else self.import_parallelism

    @property
    def retained_work_capacity(self) -> int:
        """Shared ceiling for encoding, sealed files, importing and retries."""
        return max(self.effective_encoding_parallelism, self.effective_import_parallelism) + self.max_pending

    def to_dict(self) -> dict[str, int]:
        """Preserve the exact legacy durable record for legacy-effective settings.

        Extended records contain both resolved counts. Authored fallback remains
        significant; journal readers never rewrite previously persisted records.
        """
        values = {name: getattr(self, name) for name in LEGACY_NATIVE_LIMIT_FIELDS}
        if (self.effective_encoding_parallelism, self.effective_import_parallelism) != (self.parallelism,) * 2:
            values.update(
                encoding_parallelism=self.effective_encoding_parallelism,
                import_parallelism=self.effective_import_parallelism,
            )
        return values

    @property
    def spool_payload_bound(self) -> int:
        """Payload reservation excluding separately accounted format/receipt files."""
        return (self.retained_work_capacity + 1) * self.max_bytes


@dataclass(frozen=True)
class NativeChunkPlan:
    """Stable bindings; the source query ID is not a reusable snapshot token."""

    run_id: str
    target_id: str
    source_query_id: str
    window_fingerprint: str
    schema_fingerprint: str
    wire_fingerprint: str
    transport: NativeBulkTransportPolicy | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if self.transport is not None and type(self.transport) is not NativeBulkTransportPolicy:
            raise ValueError("mssql_native.transport_invalid:transport")

    def to_dict(self) -> dict[str, Any]:
        """Preserve legacy identity bytes and order; absence is not a default mode."""
        identity: dict[str, Any] = {
            "run_id": self.run_id,
            "target_id": self.target_id,
            "source_query_id": self.source_query_id,
            "window_fingerprint": self.window_fingerprint,
            "schema_fingerprint": self.schema_fingerprint,
            "wire_fingerprint": self.wire_fingerprint,
        }
        if self.transport is not None:
            identity["transport"] = self.transport.to_dict()
        return identity


@dataclass(frozen=True)
class EncodedNativeFile:
    """Sealed bounded bytes, counted and digested before vendor ingestion."""

    path: Path
    ordinal: int
    rows: int
    encoded_bytes: int
    file_sha256: str
    typed_digest: str


@dataclass(frozen=True)
class NativeChunkReceipt:
    """Independently verified owned staging attempt and consumed-file authority."""

    ordinal: int
    attempt_id: str
    stage_id: str
    rows: int
    encoded_bytes: int
    file_sha256: str
    typed_digest: str
    consumed_part_evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NativeStageComplete:
    """Source-free authority created only after EOF and contiguous verification."""

    receipts: tuple[NativeChunkReceipt, ...]
    rows: int
    receipt_digest: str
    observations: tuple[dict[str, Any], ...] = ()
    metadata_digest: str = ""
