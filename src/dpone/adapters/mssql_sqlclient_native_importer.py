"""Injected adapter from the native scheduler to one exact SqlClient P7-P10f chain."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Never, cast

from dpone.adapters.mssql_sqlclient_native_receipt_contracts import (
    SqlClientFailedAttemptSettlement,
    SqlClientFailedEligibility,
    SqlClientInputCustody,
    bind_native_chunk_receipt,
    native,
    plan_sha256,
    validate_native_chunk_receipt,
)
from dpone.ports.mssql_sqlclient_failed_attempt_settlement import (
    SqlClientFailedAttemptSettlementCapability,
    SqlClientFailedAttemptSettlementReceipt,
)

ERROR = "mssql_native.sqlclient_native_import_unknown"


class SqlClientNativeImportUnknown(RuntimeError):
    """The exact import, inspection, or failed-attempt settlement is not proven."""

    def __init__(self) -> None:
        super().__init__(ERROR)


def _unknown() -> Never:
    raise SqlClientNativeImportUnknown() from None


def _digest(value: object) -> None:
    if type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        _unknown()


class SqlClientNativeChunkImporter:
    """Bridge the scheduler to injected one-shot execution and durable observers."""

    def __init__(
        self,
        *,
        projection_type: type,
        retain_input: Callable[[native.NativeChunkPlan, native.EncodedNativeFile, str, object], object],
        observe_custody: Callable[[native.NativeChunkPlan, native.NativeChunkReceipt, object], object],
        execute: Callable[
            [native.NativeChunkPlan, native.EncodedNativeFile, str, object, SqlClientInputCustody], object
        ],
        inspect_projection: Callable[[native.NativeChunkPlan, native.NativeChunkReceipt, object], object],
        failed_settlement: SqlClientFailedAttemptSettlementCapability,
        allocated_bytes: Callable[[], int],
    ) -> None:
        dependencies = (
            retain_input,
            observe_custody,
            execute,
            inspect_projection,
            allocated_bytes,
        )
        if (
            type(projection_type) is not type
            or not all(callable(value) for value in dependencies)
            or not callable(getattr(failed_settlement, "settle", None))
        ):
            raise ValueError("mssql_native.sqlclient_native_importer_invalid")
        self._projection_type = projection_type
        self._retain_input = retain_input
        self._observe_custody = observe_custody
        self._execute = execute
        self._inspect_projection = inspect_projection
        self._failed_settlement = failed_settlement
        self._allocated_bytes = allocated_bytes

    @staticmethod
    def _coordinates(plan: native.NativeChunkPlan, attempt_id: str, lease: Any) -> tuple[int, int]:
        try:
            if type(plan) is not native.NativeChunkPlan or type(attempt_id) is not str:
                _unknown()
            if plan.transport is None or plan.transport.backend != "mssql_sqlclient":
                _unknown()
            if getattr(lease, "target_id", None) != plan.target_id:
                _unknown()
            prefix = f"{plan.run_id}-"
            suffix = attempt_id.removeprefix(prefix)
            if suffix == attempt_id:
                _unknown()
            ordinal_text, attempt_text = suffix.rsplit("-", 1)
            ordinal, attempt = int(ordinal_text), int(attempt_text)
            if ordinal < 0 or attempt not in (0, 1, 2) or attempt_id != f"{plan.run_id}-{ordinal}-{attempt}":
                _unknown()
            return ordinal, attempt
        except (AttributeError, TypeError, ValueError, OverflowError):
            _unknown()

    @staticmethod
    def _custody(plan, file, attempt_id, ordinal, value) -> SqlClientInputCustody:
        if type(value) is not SqlClientInputCustody:
            _unknown()
        try:
            value.__post_init__()
        except (TypeError, ValueError, OverflowError):
            _unknown()
        if (
            value.plan_sha256,
            value.target_id,
            value.run_id,
            value.window_fingerprint,
            value.attempt_id,
            value.ordinal,
        ) != (plan_sha256(plan), plan.target_id, plan.run_id, plan.window_fingerprint, attempt_id, ordinal) or (
            value.rows,
            value.encoded_bytes,
            value.file_sha256,
            value.typed_digest,
        ) != (file.rows, file.encoded_bytes, file.file_sha256, file.typed_digest):
            _unknown()
        return value

    def import_file(
        self, plan: native.NativeChunkPlan, file: native.EncodedNativeFile, attempt_id: str, lease: object
    ) -> native.NativeChunkReceipt:
        ordinal, attempt = self._coordinates(plan, attempt_id, lease)
        if (
            type(file) is not native.EncodedNativeFile
            or not isinstance(file.path, Path)
            or file.ordinal != ordinal
            or type(file.ordinal) is not int
            or file.ordinal < 0
            or type(file.rows) is not int
            or not 0 <= file.rows <= 2**63 - 1
            or type(file.encoded_bytes) is not int
            or not 0 <= file.encoded_bytes <= 2**63 - 1
        ):
            _unknown()
        _digest(file.file_sha256)
        _digest(file.typed_digest)
        custody_value = self._retain_input(plan, file, attempt_id, lease)
        custody = self._custody(plan, file, attempt_id, ordinal, custody_value)
        projection = self._execute(plan, file, attempt_id, lease, custody)
        return self._receipt(plan, file, attempt_id, attempt, custody, projection)

    def _receipt(self, plan, file, attempt_id, attempt, custody, projection) -> native.NativeChunkReceipt:
        if type(projection) is not self._projection_type:
            _unknown()
        projection = cast(Any, projection)
        try:
            projection.__post_init__()
        except (AttributeError, TypeError, ValueError, OverflowError):
            _unknown()
        identity = projection.attempt
        if (identity.target_key, identity.run_id, identity.ordinal, identity.attempt) != (
            plan.target_id,
            plan.run_id,
            file.ordinal,
            attempt,
        ) or (file.rows, file.encoded_bytes, file.file_sha256, file.typed_digest) != (
            projection.rows,
            projection.encoded_bytes,
            projection.file_sha256,
            projection.typed_digest,
        ):
            _unknown()
        return bind_native_chunk_receipt(
            projection=projection,
            plan_sha256=plan_sha256(plan),
            window_fingerprint=plan.window_fingerprint,
            attempt_id=attempt_id,
            custody=custody,
        )

    def inspect(
        self, plan: native.NativeChunkPlan, receipt: native.NativeChunkReceipt, lease: object
    ) -> native.NativeChunkReceipt:
        if type(receipt) is not native.NativeChunkReceipt:
            _unknown()
        try:
            validate_native_chunk_receipt(receipt)
        except ValueError:
            _unknown()
        _, attempt = self._coordinates(plan, receipt.attempt_id, lease)
        custody_value = self._observe_custody(plan, receipt, lease)
        custody = self._custody(plan, receipt, receipt.attempt_id, receipt.ordinal, custody_value)
        projection = self._inspect_projection(plan, receipt, lease)
        observed = self._receipt(plan, receipt, receipt.attempt_id, attempt, custody, projection)
        if observed != receipt:
            _unknown()
        return receipt

    def settle(self, plan: native.NativeChunkPlan, attempt_id: str, lease: Any) -> None:
        self._coordinates(plan, attempt_id, lease)
        settlement = self._failed_settlement.settle(plan, attempt_id, lease)
        if type(settlement) is not SqlClientFailedAttemptSettlementReceipt:
            _unknown()
        try:
            settlement.__post_init__()
        except (TypeError, ValueError, OverflowError):
            _unknown()
        if settlement.disposition.value != "retry_ready":
            _unknown()

    def allocated_bytes(self) -> int:
        value = self._allocated_bytes()
        if type(value) is not int or value < 0:
            _unknown()
        return value


__all__ = (
    "SqlClientFailedAttemptSettlement",
    "SqlClientFailedEligibility",
    "SqlClientInputCustody",
    "SqlClientNativeChunkImporter",
    "SqlClientNativeImportUnknown",
)
