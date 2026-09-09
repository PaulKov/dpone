"""Serializable reconciliation of a later Airflow task try with an original dbt receipt."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_attempt_quiescence import (
    MssqlSemanticRefreshAttemptQuiescenceObserver,
)
from dpone.adapters.semantic_refresh_mssql_attempt_quiescence import (
    SemanticRefreshMssqlAttemptQuiescenceError as SemanticRefreshMssqlAttemptContinuationError,
)
from dpone.adapters.semantic_refresh_mssql_continuation_target import (
    SemanticRefreshMssqlContinuationTargetError,
    observe_continuation_target,
    require_continuation_query_timeout,
)
from dpone.contracts.semantic_refresh_attempt_continuation import (
    SemanticRefreshAttemptContinuationReceipt,
    semantic_refresh_engine_quiescence_sha256,
)
from dpone.ports.semantic_refresh_attempt_quiescence import (
    SemanticRefreshClickHouseAttemptQuiescencePort,
)
from dpone.ports.semantic_refresh_mssql_authority_models import MssqlCanonicalAdmissionBundle
from dpone.ports.semantic_refresh_mssql_worker_admission import MssqlTrustedAttemptCoordinate

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UTC = timezone.utc  # noqa: UP017 - package supports Python 3.10.


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...


class MssqlSemanticRefreshAttemptContinuationStore:
    """Verify target/receipt/guard closure and persist a create-only continuation receipt."""

    def __init__(
        self,
        *,
        control_schema: str,
        clock: Callable[[], datetime],
        clickhouse_quiescence: SemanticRefreshClickHouseAttemptQuiescencePort,
    ) -> None:
        if _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._control_schema = control_schema
        self._clock = clock
        self._quiescence = MssqlSemanticRefreshAttemptQuiescenceObserver(
            control_schema=control_schema,
            clickhouse=clickhouse_quiescence,
        )

    def reconcile(
        self,
        cursor: _Cursor,
        *,
        bundle: MssqlCanonicalAdmissionBundle,
        coordinates: tuple[MssqlTrustedAttemptCoordinate, ...],
    ) -> tuple[SemanticRefreshAttemptContinuationReceipt, ...]:
        """Return the exact continuation closure, creating receipts only after reconciliation."""

        try:
            require_continuation_query_timeout(cursor, bundle)
        except SemanticRefreshMssqlContinuationTargetError as exc:
            raise SemanticRefreshMssqlAttemptContinuationError(str(exc)) from exc
        coordinate_by_operation = {item.operation_id: item for item in coordinates}
        operation_by_id = {item.operation_id: item for item in bundle.operation_plans}
        resource_by_model = {item.model_unique_id: item for item in bundle.model_resources}
        receipts: list[SemanticRefreshAttemptContinuationReceipt] = []
        for original in bundle.attempt_bindings:
            current = coordinate_by_operation[original.operation_id]
            if (current.task_id, current.try_number, current.pod_uid) == (
                original.task_id,
                original.try_number,
                original.pod_uid,
            ):
                continue
            if current.task_id != original.task_id or current.try_number < original.try_number:
                raise SemanticRefreshMssqlAttemptContinuationError(
                    "continuation task identity does not advance the original try"
                )
            if current.try_number == original.try_number:
                raise SemanticRefreshMssqlAttemptContinuationError(
                    "continuation task identity does not advance the original try"
                )
            if current.try_number != original.try_number + 1:
                raise SemanticRefreshMssqlAttemptContinuationError(
                    "DPONE_SEMANTIC_REFRESH_CONTINUATION_CHAIN_UNSUPPORTED: "
                    "attempt-continuation receipt v1 permits only the immediate successor try"
                )
            operation = operation_by_id[original.operation_id]
            resource = resource_by_model[operation.model_unique_id]
            verified_at = _timestamp(self._clock())
            quiescence = self._quiescence.prove(
                cursor,
                bundle=bundle,
                operation_id=operation.operation_id,
                original=original,
                guard_resource_id=resource.target_resource_id,
                clickhouse_cluster_authority_id=resource.clickhouse_cluster_authority_id,
                observed_at=verified_at,
            )
            build_receipt_sha256, after_image_sha256 = self._locked_original_receipt(
                cursor,
                operation_id=operation.operation_id,
                operation_plan_sha256=operation.operation_plan_sha256,
                attempt_binding_sha256=original.attempt_binding_sha256,
                fencing_epoch=original.fencing_epoch,
                owner_id=original.owner_id,
                guard_resource_id=resource.target_resource_id,
            )
            try:
                observed_sha256, observed_rows = observe_continuation_target(
                    cursor,
                    target_resource_id=resource.target_resource_id,
                    writable_columns=resource.writable_columns,
                    effective_keys=tuple(
                        item for item in resource.writable_columns if item.writable_role != "MUTABLE_VALUE"
                    ),
                    event_time_column=operation.event_time_column,
                    scope_start=operation.scope_start,
                    scope_end=operation.scope_end,
                    resource_policy=resource.resource_policy,
                )
            except SemanticRefreshMssqlContinuationTargetError as exc:
                raise SemanticRefreshMssqlAttemptContinuationError(str(exc)) from exc
            if after_image_sha256 != observed_sha256:
                raise SemanticRefreshMssqlAttemptContinuationError(
                    "current target scope differs from the committed after-image"
                )
            receipt = SemanticRefreshAttemptContinuationReceipt.build(
                workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
                operation_id=operation.operation_id,
                operation_plan_sha256=operation.operation_plan_sha256,
                original_attempt_binding_sha256=original.attempt_binding_sha256,
                fencing_epoch=original.fencing_epoch,
                guard_resource_id=resource.target_resource_id,
                build_receipt_sha256=build_receipt_sha256,
                after_image_sha256=after_image_sha256,
                original_attempt_termination_receipt_sha256=(quiescence.termination.termination_receipt_sha256),
                clickhouse_cluster_authority_id=(resource.clickhouse_cluster_authority_id),
                clickhouse_query_id_prefix=quiescence.clickhouse.query_id_prefix,
                clickhouse_quiescence_observation_sha256=(quiescence.clickhouse.quiescence_observation_sha256),
                mssql_active_session_count=quiescence.mssql_active_session_count,
                mssql_guard_lock_status=quiescence.mssql_guard_lock_status,
                engine_quiescence_receipt_sha256=semantic_refresh_engine_quiescence_sha256(
                    workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
                    operation_id=operation.operation_id,
                    original_attempt_binding_sha256=original.attempt_binding_sha256,
                    original_attempt_termination_receipt_sha256=(quiescence.termination.termination_receipt_sha256),
                    clickhouse_quiescence_observation_sha256=(quiescence.clickhouse.quiescence_observation_sha256),
                    observed_at=verified_at,
                ),
                task_id=current.task_id,
                try_number=current.try_number,
                pod_uid=current.pod_uid,
                verified_at=verified_at,
            )
            receipts.append(self._persist_exact(cursor, receipt))
        return tuple(receipts)

    def _locked_original_receipt(
        self,
        cursor: _Cursor,
        *,
        operation_id: str,
        operation_plan_sha256: str,
        attempt_binding_sha256: str,
        fencing_epoch: int,
        owner_id: str,
        guard_resource_id: str,
    ) -> tuple[str, str]:
        cursor.execute(
            f"""
SELECT journal.operation_id, journal.operation_plan_sha256,
       journal.attempt_binding_sha256, journal.fencing_epoch, journal.owner_id,
       journal.status, guard.resource_id, guard.fencing_epoch, guard.owner_id,
       guard.status, receipt.build_receipt_sha256, receipt.after_image_sha256
FROM {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_guards")} AS guard WITH (UPDLOCK, HOLDLOCK)
  ON guard.resource_id = journal.target_resource_id
JOIN {self._table("semantic_refresh_receipts")} AS receipt WITH (UPDLOCK, HOLDLOCK)
  ON receipt.operation_id = journal.operation_id
WHERE journal.operation_id COLLATE Latin1_General_100_BIN2 = ?;
""".strip(),
            operation_id,
        )
        row = cursor.fetchone()
        expected = (
            operation_id,
            operation_plan_sha256,
            attempt_binding_sha256,
            fencing_epoch,
            owner_id,
            "PREPARING",
            guard_resource_id,
            fencing_epoch,
            owner_id,
            "HELD",
        )
        if row is None or tuple(row[:10]) != expected or not all(isinstance(item, str) for item in row[10:12]):
            raise SemanticRefreshMssqlAttemptContinuationError("original receipt and active fence are not exact")
        return str(row[10]), str(row[11])

    def _persist_exact(
        self,
        cursor: _Cursor,
        receipt: SemanticRefreshAttemptContinuationReceipt,
    ) -> SemanticRefreshAttemptContinuationReceipt:
        cursor.execute(
            f"""
SELECT receipt_json, continuation_receipt_sha256
FROM {self._table("semantic_refresh_attempt_continuations")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id COLLATE Latin1_General_100_BIN2 = ?
  AND task_id COLLATE Latin1_General_100_BIN2 = ?
  AND try_number = ?
  AND pod_uid = ?;
""".strip(),
            receipt.operation_id,
            receipt.task_id,
            receipt.try_number,
            receipt.pod_uid,
        )
        row = cursor.fetchone()
        if row is not None:
            existing = _receipt(row[0])
            if not _same_continuation_subject(existing, receipt) or row[1] != existing.continuation_receipt_sha256:
                raise SemanticRefreshMssqlAttemptContinuationError("continuation receipt replay differs")
            return existing
        payload = json.dumps(receipt.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_attempt_continuations")} (
    operation_id, task_id, try_number, pod_uid, original_attempt_binding_sha256,
    receipt_json, continuation_receipt_sha256, status, verified_at
) VALUES (?, ?, ?, ?, ?, ?, ?, N'VERIFIED', ?);
""".strip(),
            receipt.operation_id,
            receipt.task_id,
            receipt.try_number,
            receipt.pod_uid,
            receipt.original_attempt_binding_sha256,
            payload,
            receipt.continuation_receipt_sha256,
            receipt.verified_at,
        )
        return receipt

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _receipt(value: object) -> SemanticRefreshAttemptContinuationReceipt:
    if not isinstance(value, str):
        raise SemanticRefreshMssqlAttemptContinuationError("continuation receipt JSON is invalid")
    try:
        parsed: Mapping[str, object] = json.loads(value)
        return SemanticRefreshAttemptContinuationReceipt.from_mapping(parsed)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SemanticRefreshMssqlAttemptContinuationError("continuation receipt JSON is invalid") from exc


def _same_continuation_subject(
    left: SemanticRefreshAttemptContinuationReceipt,
    right: SemanticRefreshAttemptContinuationReceipt,
) -> bool:
    ignored = {
        "clickhouse_quiescence_observation_sha256",
        "continuation_receipt_sha256",
        "engine_quiescence_receipt_sha256",
        "verified_at",
    }
    return {key: value for key, value in left.to_dict().items() if key not in ignored} == {
        key: value for key, value in right.to_dict().items() if key not in ignored
    }


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SemanticRefreshMssqlAttemptContinuationError("continuation clock must be timezone-aware")
    return value.astimezone(_UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


__all__ = [
    "MssqlSemanticRefreshAttemptContinuationStore",
    "SemanticRefreshMssqlAttemptContinuationError",
]
