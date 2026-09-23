"""Closed deployment capabilities for native MSSQL route selection."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Protocol

from dpone.contracts.mssql_native_parent_journal import (
    NativeCheckpointReceipt,
    NativeChunkRetirementReceipt,
    NativeParentAuthority,
    NativeParentRetirementReceipt,
    canonical_digest,
)
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientNativeChunkProjection
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.ports.mssql_native_chunks import NativeChunkImporter


class NativeMssqlBackend(StrEnum):
    """The only native MSSQL backends understood by the runtime contract."""

    BCP = "mssql_python"
    SQLCLIENT = "mssql_sqlclient"


@dataclass(frozen=True, slots=True)
class NativeActorCapacity:
    """Finite actor budget published by one exact backend installation."""

    available: int
    fresh_peak: int
    retirement_peak: int
    parent_reserve: int

    def required(self, parallelism: int) -> int:
        if type(parallelism) is not int or parallelism < 1:
            raise ValueError("mssql_native.import_parallelism_invalid")
        return parallelism * self.fresh_peak + parallelism * self.retirement_peak + self.parent_reserve

    def admit(self, parallelism: int) -> None:
        values = (self.available, self.fresh_peak, self.retirement_peak, self.parent_reserve)
        if any(type(value) is not int or value < 0 for value in values) or self.available < self.required(parallelism):
            raise ValueError("mssql_native.backend_capacity_insufficient")


class NativeParentSettlement(Protocol):
    """Finish parent cleanup and checkpoint without reopening the source."""

    def settle(self) -> object: ...


@dataclass(frozen=True, slots=True)
class NativeParentSettlementBinding:
    """Durable journal identity required before any settlement effect."""

    target_id: str
    window_fingerprint: str
    fence: int
    chunk_count: int

    def __post_init__(self) -> None:
        if (
            any(type(value) is not str or not value for value in (self.target_id, self.window_fingerprint))
            or type(self.fence) is not int
            or self.fence < 1
            or type(self.chunk_count) is not int
            or self.chunk_count < 1
        ):
            raise ValueError("mssql_native.parent_settlement_binding_invalid")


class ParentSettlementJournal(Protocol):
    """The v4 publication-journal surface used by parent settlement."""

    def state(self) -> dict[str, Any] | None: ...
    def authority(self) -> NativeParentAuthority | None: ...
    def settlement_binding(self) -> NativeParentSettlementBinding: ...
    def retirement_required(self, authority: NativeParentAuthority) -> None: ...
    def retiring(self) -> None: ...
    def chunk_retired(self, receipt: NativeChunkRetirementReceipt) -> None: ...
    def retired(self) -> NativeParentRetirementReceipt: ...
    def retirement_receipt(self) -> NativeParentRetirementReceipt | None: ...
    def checkpoint_required(self) -> None: ...
    def succeeded(self, receipt: NativeCheckpointReceipt) -> None: ...


class ParentChunkRetirer(Protocol):
    def retire(
        self, projection: SqlClientNativeChunkProjection, authority: NativeParentAuthority
    ) -> NativeChunkRetirementReceipt: ...


class ParentInputCustody(Protocol):
    """Durably release once; replay returns the identical acknowledged receipt."""

    def release(self, request: NativeInputCustodyRequest) -> NativeInputCustodyReceipt: ...


class ParentCheckpointCas(Protocol):
    """Observe-or-advance the exact CAS; lost acknowledgements never create another effect."""

    def advance(self, request: NativeCheckpointRequest) -> NativeCheckpointReceipt: ...


@dataclass(frozen=True, slots=True)
class NativeInputCustodyRequest:
    retirement_digest: str
    target_id: str
    window_fingerprint: str
    fence: int

    def __post_init__(self) -> None:
        _validate_parent_effect_identity(self.retirement_digest, self.target_id, self.window_fingerprint, self.fence)

    @classmethod
    def bind(
        cls, retirement: NativeParentRetirementReceipt, identity: NativeParentSettlementBinding
    ) -> NativeInputCustodyRequest:
        return cls(retirement.digest, identity.target_id, identity.window_fingerprint, identity.fence)


@dataclass(frozen=True, slots=True)
class NativeInputCustodyReceipt:
    request_sha256: str
    release_sha256: str

    def __post_init__(self) -> None:
        for value in (self.request_sha256, self.release_sha256):
            if type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise ValueError("mssql_native.input_custody_receipt_invalid")


@dataclass(frozen=True, slots=True)
class NativeCheckpointRequest:
    retirement_digest: str
    target_id: str
    window_fingerprint: str
    fence: int

    def __post_init__(self) -> None:
        _validate_parent_effect_identity(self.retirement_digest, self.target_id, self.window_fingerprint, self.fence)

    @classmethod
    def bind(
        cls,
        retirement: NativeParentRetirementReceipt,
        identity: NativeParentSettlementBinding,
    ) -> NativeCheckpointRequest:
        return cls(
            retirement.digest,
            identity.target_id,
            identity.window_fingerprint,
            identity.fence,
        )


def custody_request_digest(value: NativeInputCustodyRequest) -> str:
    return canonical_digest(asdict(value))


def _validate_parent_effect_identity(retirement: str, target: str, window: str, fence: int) -> None:
    if (
        type(retirement) is not str
        or len(retirement) != 64
        or any(char not in "0123456789abcdef" for char in retirement)
        or any(type(value) is not str or not value for value in (target, window))
        or type(fence) is not int
        or fence < 1
    ):
        raise ValueError("mssql_native.parent_effect_identity_invalid")


@dataclass(frozen=True, slots=True)
class BcpNativeRouteBackend:
    """Existing BCP capability; its settlement implementation retains legacy order."""

    settlement: NativeParentSettlement


@dataclass(frozen=True, slots=True)
class SqlClientNativeRouteBackend:
    """Complete explicit SqlClient capability bundle; partial bundles are impossible."""

    importer: NativeChunkImporter
    settlement: NativeParentSettlement
    capacity: NativeActorCapacity
    implementation_sha256: str

    def admit(self, parallelism: int) -> None:
        importer_methods = ("import_file", "inspect", "settle", "allocated_bytes")
        if any(not callable(getattr(self.importer, name, None)) for name in importer_methods) or not callable(
            getattr(self.settlement, "settle", None)
        ):
            raise ValueError("mssql_native.sqlclient_capability_incomplete")
        if (
            type(self.implementation_sha256) is not str
            or len(self.implementation_sha256) != 64
            or any(char not in "0123456789abcdef" for char in self.implementation_sha256)
        ):
            raise ValueError("mssql_native.sqlclient_implementation_invalid")
        self.capacity.admit(parallelism)


@dataclass(frozen=True, slots=True)
class SqlClientNativeRuntimeBinding:
    """One immutable installation and run authority with independent sessions."""

    backend: SqlClientNativeRouteBackend
    open_backend: Callable[[], AbstractContextManager[SqlClientNativeRouteBackend]]
    physical_stage: Callable[[object], object]
    abort_parent: Callable[[object], None]
    terminal_result: Callable[[object], object]

    def admit(self, parallelism: int) -> None:
        if type(self.backend) is not SqlClientNativeRouteBackend or any(
            not callable(value)
            for value in (self.open_backend, self.physical_stage, self.abort_parent, self.terminal_result)
        ):
            raise ValueError("mssql_native.sqlclient_runtime_binding_incomplete")
        self.backend.admit(parallelism)
        with self.open_backend() as candidate:
            self._require_same(candidate, parallelism)

    @contextmanager
    def importer_session(self, parallelism: int) -> Iterator[NativeChunkImporter]:
        with self.open_backend() as candidate:
            self._require_same(candidate, parallelism)
            yield candidate.importer

    def _require_same(self, candidate: object, parallelism: int) -> None:
        if (
            type(candidate) is not SqlClientNativeRouteBackend
            or candidate.implementation_sha256 != self.backend.implementation_sha256
            or candidate.capacity != self.backend.capacity
            or candidate.settlement is not self.backend.settlement
        ):
            raise ValueError("mssql_native.sqlclient_runtime_binding_changed")
        candidate.admit(parallelism)


NativeRouteBackend = BcpNativeRouteBackend | SqlClientNativeRouteBackend


def select_native_route_backend(
    authored: NativeMssqlBackend,
    *,
    bcp: BcpNativeRouteBackend,
    sqlclient: SqlClientNativeRouteBackend | None,
    parallelism: int,
) -> NativeRouteBackend:
    """Resolve the authored backend exactly; never substitute another backend."""
    if authored is NativeMssqlBackend.BCP:
        return bcp
    if authored is not NativeMssqlBackend.SQLCLIENT or sqlclient is None:
        raise ValueError("mssql_native.authored_backend_unavailable")
    sqlclient.admit(parallelism)
    return sqlclient


__all__ = (
    "BcpNativeRouteBackend",
    "NativeActorCapacity",
    "NativeCheckpointRequest",
    "NativeInputCustodyReceipt",
    "NativeInputCustodyRequest",
    "NativeMssqlBackend",
    "NativeParentSettlement",
    "NativeParentSettlementBinding",
    "NativeRouteBackend",
    "SqlClientNativeRouteBackend",
    "SqlClientNativeRuntimeBinding",
    "NativeCheckpointReceipt",
    "NativeChunkRetirementReceipt",
    "NativeParentAuthority",
    "NativeParentRetirementReceipt",
    "ParentCheckpointCas",
    "ParentChunkRetirer",
    "ParentInputCustody",
    "ParentSettlementJournal",
    "TdsDirectoryLimits",
    "select_native_route_backend",
    "custody_request_digest",
)
