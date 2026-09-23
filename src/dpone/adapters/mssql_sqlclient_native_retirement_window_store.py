"""Fenced WindowStore persistence for exact SqlClient chunk retirement progress."""

from __future__ import annotations

import json
import math
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any
from uuid import UUID

from dpone.adapters.mssql_sqlclient_native_retirement_state import (
    CasSqlClientNativeRetirementState,
    NativeChunkAbsenceProof,
    NativeChunkCapacityProof,
    NativeChunkClosedDirectory,
    NativeChunkContainmentProof,
    NativeChunkDropIntent,
    NativeChunkDropObservation,
    NativeChunkDropOutcome,
    NativeChunkDropProof,
    NativeChunkLifecyclePhase,
    NativeChunkLifecycleProof,
    NativeChunkRetiredTerminal,
    NativeChunkRetirementProgress,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
    NativeChunkSettlementProof,
    bind_native_chunk_lifecycle,
)
from dpone.ports.bounded_window import WindowLease, WindowRecord, WindowStore
from dpone.ports.mssql_tds_directory import TdsDirectoryLimits, TdsDirectoryObserver, TdsDirectorySnapshot
from dpone.ports.mssql_tds_journal import TdsAttemptObserver, TdsAttemptPhase, TdsAttemptSnapshot

_SCHEMA = "dpone.mssql-sqlclient.native-retirement-state.v1"
_KEY_PREFIX = "mssql-sqlclient-native-retirement-v1/"
_MAX_RECORD_CHARS = 131_072


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _strict_object(payload: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite constant: {value}")

    def finite(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("non-finite number")
        return parsed

    decoded = json.loads(payload, object_pairs_hook=unique, parse_constant=reject_constant, parse_float=finite)
    if type(decoded) is not dict:
        raise ValueError("record root")
    return decoded


def _ack(record: object, expected: int | None, payload: str) -> bool:
    return (
        type(record) is WindowRecord
        and type(record.revision) is int
        and (expected or 0) < record.revision <= 2**63 - 1
        and record.payload == payload
    )


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if is_dataclass(value):
        return {field.name: _plain(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    return value


def _record(cls: type[Any], value: object, **converters: Any) -> Any:
    names = {field.name for field in fields(cls)}
    if type(value) is not dict or set(value) != names:
        raise ValueError("record shape")
    data = dict(value)
    for name, converter in converters.items():
        data[name] = converter(data[name])
    return cls(**data)


def _optional(cls: type[Any], value: object, **converters: Any) -> Any:
    return None if value is None else _record(cls, value, **converters)


def _decode_progress(value: object) -> NativeChunkRetirementProgress:
    names = {field.name for field in fields(NativeChunkRetirementProgress)}
    if type(value) is not dict or set(value) != names:
        raise ValueError("progress shape")
    data = dict(value)
    lifecycle = data["lifecycle"]
    if type(lifecycle) is not list or not lifecycle:
        raise ValueError("lifecycle shape")
    data["lifecycle"] = tuple(
        _record(NativeChunkLifecycleProof, item, phase=NativeChunkLifecyclePhase) for item in lifecycle
    )
    data["containment"] = _optional(NativeChunkContainmentProof, data["containment"])
    data["reservation"] = _optional(NativeChunkRetirementReservation, data["reservation"], operation_id=UUID)
    data["drop_intent"] = _optional(NativeChunkDropIntent, data["drop_intent"])
    data["drop"] = _optional(
        NativeChunkDropProof,
        data["drop"],
        observation=NativeChunkDropObservation,
        outcome=NativeChunkDropOutcome,
    )
    data["settlement"] = _optional(
        NativeChunkSettlementProof,
        data["settlement"],
        operation_id=UUID,
        outcome=NativeChunkDropOutcome,
    )
    data["absence"] = _optional(NativeChunkAbsenceProof, data["absence"])
    data["terminal"] = _optional(NativeChunkRetiredTerminal, data["terminal"])
    data["directory"] = _optional(NativeChunkClosedDirectory, data["directory"])
    data["capacity"] = _optional(NativeChunkCapacityProof, data["capacity"])
    return NativeChunkRetirementProgress(**data)


def _encode(progress: NativeChunkRetirementProgress) -> str:
    return _canonical({"schema": _SCHEMA, "progress": _plain(progress)})


def _decode(payload: str) -> NativeChunkRetirementProgress:
    if type(payload) is not str or len(payload) > _MAX_RECORD_CHARS:
        raise ValueError("record size")
    value = _strict_object(payload)
    if set(value) != {"schema", "progress"} or value["schema"] != _SCHEMA:
        raise ValueError("record envelope")
    if _canonical(value) != payload:
        raise ValueError("noncanonical record")
    progress = _decode_progress(value["progress"])
    if _encode(progress) != payload:
        raise ValueError("record round trip")
    return progress


class WindowStoreSqlClientNativeRetirementState:
    """Initialize and advance P10g state from exact durable parent snapshots."""

    def __init__(
        self,
        store: WindowStore,
        lease: WindowLease,
        *,
        attempts: TdsAttemptObserver,
        directories: TdsDirectoryObserver,
        directory_limits: TdsDirectoryLimits,
    ) -> None:
        if type(lease) is not WindowLease or type(directory_limits) is not TdsDirectoryLimits:
            raise ValueError("mssql_sqlclient.retirement_state_configuration_invalid")
        self._store = store
        self._lease = lease
        self._attempts = attempts
        self._directories = directories
        self._limits = directory_limits

    @staticmethod
    def storage_key(request: NativeChunkRetirementRequest) -> str:
        if type(request) is not NativeChunkRetirementRequest:
            raise ValueError("mssql_sqlclient.retirement_state_request_invalid")
        return _KEY_PREFIX + request.projection.projection_sha256

    def _adapter(self, request: NativeChunkRetirementRequest) -> CasSqlClientNativeRetirementState:
        if type(request) is not NativeChunkRetirementRequest:
            raise ValueError("mssql_sqlclient.retirement_state_request_invalid")
        return CasSqlClientNativeRetirementState(
            load=lambda digest: self._load(request, digest),
            compare_swap=lambda digest, revision, progress: self._save(request, digest, revision, progress),
        )

    def observe(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        return self._adapter(request).observe(request)

    def seal_work(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        return self._adapter(request).seal_work(request)

    def require_containment(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        return self._adapter(request).require_containment(request)

    def record_containment(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkContainmentProof
    ) -> NativeChunkRetirementProgress:
        return self._adapter(request).record_containment(request, proof)

    def reserve_retirement(
        self, request: NativeChunkRetirementRequest, reservation: NativeChunkRetirementReservation
    ) -> NativeChunkRetirementProgress:
        return self._adapter(request).reserve_retirement(request, reservation)

    def record_drop_intent(
        self, request: NativeChunkRetirementRequest, intent: NativeChunkDropIntent
    ) -> NativeChunkRetirementProgress:
        return self._adapter(request).record_drop_intent(request, intent)

    def record_drop_outcome(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkDropProof
    ) -> NativeChunkRetirementProgress:
        return self._adapter(request).record_drop_outcome(request, proof)

    def record_settlement(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkSettlementProof
    ) -> NativeChunkRetirementProgress:
        return self._adapter(request).record_settlement(request, proof)

    def record_retired(
        self, request: NativeChunkRetirementRequest, absence: NativeChunkAbsenceProof
    ) -> NativeChunkRetirementProgress:
        return self._adapter(request).record_retired(request, absence)

    def close_admission(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        return self._adapter(request).close_admission(request)

    def record_capacity(
        self, request: NativeChunkRetirementRequest, proof: NativeChunkCapacityProof
    ) -> NativeChunkRetirementProgress:
        return self._adapter(request).record_capacity(request, proof)

    def _assert_lease(self, request: NativeChunkRetirementRequest) -> None:
        if self._lease.target_id != request.projection.attempt.target_key:
            raise ValueError("mssql_sqlclient.retirement_state_lease_mismatch")
        self._store.assert_lease(self._lease)

    def _load(self, request: NativeChunkRetirementRequest, digest: str) -> tuple[int, NativeChunkRetirementProgress]:
        if digest != request.projection.projection_sha256:
            raise ValueError("mssql_sqlclient.retirement_state_key_mismatch")
        self._assert_lease(request)
        key = self.storage_key(request)
        try:
            record = self._store.load(key)
        except Exception as error:
            raise RuntimeError("mssql_sqlclient.retirement_state_read_unknown") from error
        if record is None:
            return self._initialize(request, key)
        return self._checked_record(request, record)

    def _initialize(self, request: NativeChunkRetirementRequest, key: str) -> tuple[int, NativeChunkRetirementProgress]:
        progress = self._initial_progress(request)
        payload = _encode(progress)
        try:
            record = self._store.save(key, None, payload, self._lease)
        except Exception as error:
            observed = self._reload(request, key)
            if self._matches(request, observed, payload, after=0):
                assert observed is not None
                return self._checked_record(request, observed)
            if observed is not None:
                return self._checked_record(request, observed)
            raise RuntimeError("mssql_sqlclient.retirement_state_initialize_unknown") from error
        if not _ack(record, None, payload):
            observed = self._reload(request, key)
            if self._matches(request, observed, payload, after=0):
                assert observed is not None
                return self._checked_record(request, observed)
            raise RuntimeError("mssql_sqlclient.retirement_state_initialize_ack_unknown")
        return record.revision, progress

    def _initial_progress(self, request: NativeChunkRetirementRequest) -> NativeChunkRetirementProgress:
        try:
            attempt = self._attempts.read(request.projection.attempt)
            directory = self._directories.read(request.projection.attempt, self._limits)
        except Exception as error:
            raise RuntimeError("mssql_sqlclient.retirement_state_initial_evidence_unknown") from error
        projection = request.projection
        if (
            type(attempt) is not TdsAttemptSnapshot
            or attempt.state.identity != projection.attempt
            or attempt.state.phase is not TdsAttemptPhase.VERIFIED
            or attempt.state.object_identity != projection.object_identity
            or attempt.state.verification_sha256 != projection.lifecycle_verification_sha256
            or attempt.revision != projection.lifecycle_revision
            or type(directory) is not TdsDirectorySnapshot
            or directory.state.parent != projection.attempt
            or directory.state.limits != self._limits
        ):
            raise ValueError("mssql_sqlclient.retirement_state_initial_evidence_mismatch")
        lifecycle = bind_native_chunk_lifecycle(
            phase=NativeChunkLifecyclePhase.VERIFIED,
            projection_sha256=projection.projection_sha256,
            authorization_sha256=request.authorization.authorization_sha256,
            directory_key=projection.directory_key,
            directory_revision=directory.revision,
            authority_digest=request.authority.digest,
            authority_fence=request.authority.fence,
            lifecycle_revision=attempt.revision,
        )
        return NativeChunkRetirementProgress(
            projection_sha256=projection.projection_sha256,
            parent_authority_digest=request.authority.digest,
            authorization_sha256=request.authorization.authorization_sha256,
            lifecycle=(lifecycle,),
        )

    def _save(
        self,
        request: NativeChunkRetirementRequest,
        digest: str,
        revision: int,
        progress: NativeChunkRetirementProgress,
    ) -> bool:
        if digest != request.projection.projection_sha256:
            raise ValueError("mssql_sqlclient.retirement_state_key_mismatch")
        self._assert_lease(request)
        key, payload = self.storage_key(request), _encode(progress)
        try:
            record = self._store.save(key, revision, payload, self._lease)
        except Exception as error:
            observed = self._reload(request, key)
            if self._matches(request, observed, payload, after=revision):
                return True
            if observed is not None and observed.revision != revision:
                return False
            raise RuntimeError("mssql_sqlclient.retirement_state_write_unknown") from error
        if _ack(record, revision, payload):
            return True
        observed = self._reload(request, key)
        if self._matches(request, observed, payload, after=revision):
            return True
        raise RuntimeError("mssql_sqlclient.retirement_state_write_ack_unknown")

    def _reload(self, request: NativeChunkRetirementRequest, key: str) -> WindowRecord | None:
        self._assert_lease(request)
        try:
            return self._store.load(key)
        except Exception as error:
            raise RuntimeError("mssql_sqlclient.retirement_state_read_unknown") from error

    @classmethod
    def _matches(
        cls,
        request: NativeChunkRetirementRequest,
        record: WindowRecord | None,
        payload: str,
        *,
        after: int,
    ) -> bool:
        if record is None or record.payload != payload:
            return False
        revision, progress = cls._checked_record(request, record)
        return revision > after and _encode(progress) == payload

    @staticmethod
    def _checked_record(
        request: NativeChunkRetirementRequest, record: WindowRecord
    ) -> tuple[int, NativeChunkRetirementProgress]:
        try:
            if (
                type(record) is not WindowRecord
                or type(record.revision) is not int
                or not 1 <= record.revision <= 2**63 - 1
            ):
                raise ValueError("record revision")
            progress = _decode(record.payload)
        except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
            raise ValueError("mssql_sqlclient.retirement_state_record_invalid") from None
        if (
            progress.projection_sha256 != request.projection.projection_sha256
            or progress.parent_authority_digest != request.authority.digest
            or progress.authorization_sha256 != request.authorization.authorization_sha256
        ):
            raise ValueError("mssql_sqlclient.retirement_state_binding")
        return record.revision, progress


__all__ = ("WindowStoreSqlClientNativeRetirementState",)
