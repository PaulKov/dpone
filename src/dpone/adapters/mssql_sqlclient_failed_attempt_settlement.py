"""Fenced WindowStore journal for failed-attempt settlement intent and receipt."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from dataclasses import asdict

from dpone.adapters.mssql_sqlclient_failed_retirement_operator import (
    SqlClientFailedRetirementProgress,
    SqlClientFailedRetirementTerminal,
)
from dpone.ports.bounded_window import WindowContractError, WindowLease, WindowRecord, WindowStore
from dpone.ports.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptDisposition,
    SqlClientFailedAttemptSettlementReceipt,
    SqlClientFailedRetirementRequest,
    decode_failed_retirement_request,
    encode_failed_retirement_request,
)

_ERROR = "mssql_native.sqlclient_failed_settlement_unknown"
_SCHEMA = "dpone.sqlclient.failed-settlement-journal.v2"


class WindowStoreSqlClientFailedSettlementJournal:
    """Persist exact intent before resuming the idempotent P10g retirement suffix."""

    def __init__(self, store: WindowStore, lease: WindowLease) -> None:
        if type(lease) is not WindowLease:
            raise ValueError(_ERROR)
        self._store = store
        self._lease = lease

    @staticmethod
    def _key(operation_key: str) -> str:
        if type(operation_key) is not str or len(operation_key) != 64:
            raise ValueError(_ERROR)
        return f"mssql-sqlclient-failed-settlement-v2/{operation_key}"

    def observe_or_resume(
        self,
        operation_key: str,
        observe_request: Callable[[], SqlClientFailedRetirementRequest],
        resume: Callable[[SqlClientFailedRetirementRequest], SqlClientFailedAttemptSettlementReceipt],
    ) -> SqlClientFailedAttemptSettlementReceipt:
        """Create intent once, then let P10g observe/reconcile its durable suffix."""
        key = self._key(operation_key)
        if not callable(observe_request) or not callable(resume):
            raise ValueError(_ERROR)
        self._store.assert_lease(self._lease)
        record = self._store.load(key)
        if record is None:
            request = observe_request()
            payload = self._encode("intent", request, None)
            record = self._store.save(key, None, payload, self._lease)
        phase, request, receipt = self._decode(record.payload)
        if phase == "receipt":
            if receipt is None:
                raise RuntimeError(_ERROR)
            return receipt
        if phase != "intent" or receipt is not None:
            raise RuntimeError(_ERROR)
        candidate = resume(request)
        if (
            type(candidate) is not SqlClientFailedAttemptSettlementReceipt
            or candidate.request_sha256 != request.request_sha256
            or candidate.operation_key != operation_key
        ):
            raise RuntimeError(_ERROR)
        candidate.__post_init__()
        payload = self._encode("receipt", request, candidate)
        try:
            self._store.save(key, record.revision, payload, self._lease)
            return candidate
        except WindowContractError:
            observed = self._store.load(key)
            if observed is None:
                raise RuntimeError(_ERROR) from None
            phase, bound_request, receipt = self._decode(observed.payload)
            if phase != "receipt" or bound_request != request or receipt != candidate:
                raise RuntimeError(_ERROR) from None
            return receipt

    @staticmethod
    def _encode(
        phase: str,
        request: SqlClientFailedRetirementRequest,
        receipt: SqlClientFailedAttemptSettlementReceipt | None,
    ) -> str:
        return json.dumps(
            {
                "schema": _SCHEMA,
                "phase": phase,
                "request": base64.b64encode(encode_failed_retirement_request(request)).decode(),
                "receipt": None if receipt is None else asdict(receipt),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _decode(
        payload: str,
    ) -> tuple[str, SqlClientFailedRetirementRequest, SqlClientFailedAttemptSettlementReceipt | None]:
        try:
            value = json.loads(payload)
            if type(value) is not dict or set(value) != {"schema", "phase", "request", "receipt"}:
                raise ValueError
            if value["schema"] != _SCHEMA or value["phase"] not in {"intent", "receipt"}:
                raise ValueError
            raw = value["receipt"]
            receipt = None
            if raw is not None:
                if type(raw) is not dict:
                    raise ValueError
                raw["disposition"] = SqlClientFailedAttemptDisposition(raw["disposition"])
                receipt = SqlClientFailedAttemptSettlementReceipt(**raw)
            if (value["phase"] == "intent") != (receipt is None):
                raise ValueError
            request = decode_failed_retirement_request(base64.b64decode(value["request"], validate=True))
            return value["phase"], request, receipt
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            raise RuntimeError(_ERROR) from None


_PROGRESS_ERROR = "mssql_native.sqlclient_failed_retirement_progress_unknown"
_PROGRESS_SCHEMA = "dpone.sqlclient.failed-retirement-progress.v1"
_PROGRESS_PREFIX = "mssql-sqlclient-failed-retirement-v1/"


def _encode(value: SqlClientFailedRetirementProgress) -> str:
    if type(value) is not SqlClientFailedRetirementProgress:
        raise ValueError(_PROGRESS_ERROR)
    return json.dumps(
        {"schema": _PROGRESS_SCHEMA, "progress": asdict(value)},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _decode(payload: str) -> SqlClientFailedRetirementProgress:
    try:
        raw = json.loads(payload)
        if type(raw) is not dict or set(raw) != {"schema", "progress"} or raw["schema"] != _PROGRESS_SCHEMA:
            raise ValueError
        data = raw["progress"]
        if type(data) is not dict or set(data) != set(SqlClientFailedRetirementProgress.__dataclass_fields__):
            raise ValueError
        value = SqlClientFailedRetirementProgress(**data)
        if _encode(value) != payload:
            raise ValueError
        return value
    except (TypeError, ValueError, json.JSONDecodeError):
        raise ValueError("mssql_native.sqlclient_failed_retirement_progress_invalid") from None


class WindowStoreSqlClientFailedRetirementProgress:
    """Persist one monotonic request-bound suffix with CAS acknowledgement recovery."""

    def __init__(self, store: WindowStore, lease: WindowLease) -> None:
        if type(lease) is not WindowLease:
            raise ValueError(_PROGRESS_ERROR)
        self._store = store
        self._lease = lease

    def observe(self, request_sha256: str) -> SqlClientFailedRetirementProgress:
        key = self._key(request_sha256)
        self._store.assert_lease(self._lease)
        record = self._store.load(key)
        if record is not None:
            return self._record(record, request_sha256)
        initial = SqlClientFailedRetirementProgress(request_sha256)
        payload = _encode(initial)
        try:
            saved = self._store.save(key, None, payload, self._lease)
        except Exception as error:
            observed = self._store.load(key)
            if observed is not None and observed.payload == payload:
                return self._record(observed, request_sha256)
            raise RuntimeError(_PROGRESS_ERROR) from error
        if type(saved) is not WindowRecord or saved.payload != payload:
            raise RuntimeError(_PROGRESS_ERROR)
        return initial

    def record(
        self,
        before: SqlClientFailedRetirementProgress,
        after: SqlClientFailedRetirementProgress,
    ) -> SqlClientFailedRetirementProgress:
        if (
            type(before) is not SqlClientFailedRetirementProgress
            or type(after) is not SqlClientFailedRetirementProgress
            or before.request_sha256 != after.request_sha256
        ):
            raise ValueError(_PROGRESS_ERROR)
        key = self._key(before.request_sha256)
        self._store.assert_lease(self._lease)
        current = self._store.load(key)
        if current is None or self._record(current, before.request_sha256) != before:
            raise RuntimeError(_PROGRESS_ERROR)
        payload = _encode(after)
        try:
            saved = self._store.save(key, current.revision, payload, self._lease)
        except Exception as error:
            observed = self._store.load(key)
            if observed is not None and observed.revision > current.revision and observed.payload == payload:
                return self._record(observed, after.request_sha256)
            raise RuntimeError(_PROGRESS_ERROR) from error
        if type(saved) is not WindowRecord or saved.revision <= current.revision or saved.payload != payload:
            raise RuntimeError(_PROGRESS_ERROR)
        return after

    @staticmethod
    def _key(request_sha256: str) -> str:
        if type(request_sha256) is not str or len(request_sha256) != 64:
            raise ValueError(_PROGRESS_ERROR)
        return _PROGRESS_PREFIX + request_sha256

    @staticmethod
    def _record(record: WindowRecord, request_sha256: str) -> SqlClientFailedRetirementProgress:
        value = _decode(record.payload)
        if value.request_sha256 != request_sha256:
            raise ValueError(_PROGRESS_ERROR)
        return value


__all__ = (
    "SqlClientFailedRetirementTerminal",
    "WindowStoreSqlClientFailedRetirementProgress",
    "WindowStoreSqlClientFailedSettlementJournal",
)
