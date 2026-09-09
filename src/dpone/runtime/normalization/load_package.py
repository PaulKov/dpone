"""Atomic lifecycle helpers for nested root/child load packages."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from dpone.contracts.quality_failure import (
    QualityGateFailure,
    QualityGateFailureOutcome,
    QualityGateReceiptError,
    QualityGateReceiptInvalid,
)
from dpone.runtime.governance.quality_execution import (
    QualityExecutionSnapshot,
    QualityGateExecution,
)
from dpone.runtime.normalization.staged_mutation import (
    NESTED_PACKAGE_ABORT_FAILED,
    NESTED_PACKAGE_MUTATION_PORT_REQUIRED,
    NESTED_PACKAGE_PARTIAL_FINALIZE,
    NESTED_PACKAGE_STAGED_ABORT_REQUIRED,
    NestedPackagePartialFinalizeError,
    NestedPackageStagedMutation,
    require_nested_package_abort,
    require_nested_package_mutation,
    sink_supports_nested_package_abort,
)
from dpone.runtime.sinks.load_result import LoadResult

SinglePayloadLoader = Callable[[Any, Any, Any, Any], LoadResult]
PackageFinalizer = Callable[[LoadResult], LoadResult]
PackageAbort = Callable[[], None]

__all__ = [
    "NESTED_PACKAGE_ABORT_FAILED",
    "NESTED_PACKAGE_MUTATION_PORT_REQUIRED",
    "NESTED_PACKAGE_PARTIAL_FINALIZE",
    "NESTED_PACKAGE_STAGED_ABORT_REQUIRED",
    "NestedLoadPackageCoordinator",
    "NestedLoadPackageTransaction",
    "NestedPackagePartialFinalizeError",
    "NestedPackageQualityController",
    "NestedPackageStagedMutation",
    "PackageAbort",
    "PackageFinalizer",
    "SinglePayloadLoader",
    "require_nested_package_abort",
    "require_nested_package_mutation",
    "sink_supports_nested_package_abort",
]


@dataclass(frozen=True, slots=True)
class _PendingTableLoad:
    load_config: Any
    payload: Any
    extract_result: Any
    load_record: Any


class NestedLoadPackageCoordinator:
    """Coordinate audit status for root/child table load packages."""

    def mark_staged(self, identity_service: Any, load_record: Any, *, extracted_rows: int) -> None:
        identity_service.mark_staged(load_record, extracted_rows=extracted_rows)

    def mark_committed(self, identity_service: Any, load_record: Any, aggregate: Any) -> None:
        identity_service.mark_committed(load_record, aggregate)

    def mark_failed(self, identity_service: Any, load_record: Any, error: Exception) -> None:
        """Record the exact failure without replacing it with an audit error."""

        marker = getattr(identity_service, "mark_failed", None)
        if not callable(marker):
            return
        try:
            marker(load_record, error)
        except Exception:
            return


class NestedPackageQualityController:
    """Project member config and consume one quality authority per package."""

    def __init__(
        self,
        *,
        governance_service: Any,
        quality_execution: QualityGateExecution,
    ) -> None:
        self._governance_service = governance_service
        self._quality_execution = quality_execution

    @classmethod
    def for_load(
        cls,
        *,
        governance_service: Any,
        quality_execution: QualityGateExecution | None,
        load_config: Any,
        load_record: Any,
    ) -> NestedPackageQualityController:
        execution = quality_execution
        if execution is None:
            snapshot = QualityExecutionSnapshot.from_load_config(load_config)
            create = getattr(governance_service, "create_quality_gate_execution", None)
            execution = (
                create(
                    snapshot=snapshot,
                    run_id=str(load_record.run_id),
                    load_id=str(load_record.load_id),
                )
                if callable(create)
                else QualityGateExecution(
                    snapshot,
                    run_id=str(load_record.run_id),
                    load_id=str(load_record.load_id),
                )
            )
        execution.assert_current(load_config=load_config)
        return cls(governance_service=governance_service, quality_execution=execution)

    def member_load_config(self, load_config: Any) -> Any:
        self._quality_execution.assert_current(load_config=load_config)
        return _package_member_load_config(load_config)

    def member_result(self, load_config: Any, load_result: LoadResult) -> LoadResult:
        del load_config  # Revalidation occurs before the next mutation or at package finalization.
        metrics = dict(load_result.reconciliation_metrics or {})
        metrics.pop("quality_gates", None)
        return replace(
            load_result,
            reconciliation_metrics=metrics or None,
            quality_gate_receipt=None,
        )

    def finalize(
        self,
        *,
        load_config: Any,
        extract_result: Any,
        load_result: LoadResult,
    ) -> LoadResult:
        # Package quality runs on staged members before finalize/abort.
        outcome = QualityGateFailureOutcome.from_load_result(
            failure_boundary="pre_commit",
            target_state="not_mutated",
            checkpoint_state="not_applicable",
            source_state="not_advanced",
            retry_classification="retry_before_target_mutation",
            load_result=load_result,
        )
        try:
            if load_result.quality_gate_receipt is not None:
                raise QualityGateReceiptInvalid
            self._quality_execution.select_boundary("pre_commit", load_config=load_config)
            receipt, evidence = self._governance_service.evaluate_quality_gate_receipt(
                load_config=load_config,
                extract_result=extract_result,
                load_result=load_result,
                boundary="pre_commit",
                quality_execution=self._quality_execution,
            )
            self._quality_execution.accept_payload(receipt, load_config=load_config)
            return replace(
                load_result,
                reconciliation_metrics={
                    **(load_result.reconciliation_metrics or {}),
                    "quality_gates": evidence,
                },
                quality_gate_receipt=receipt,
            )
        except QualityGateFailure as exc:
            raise QualityGateFailure(exc.report, outcome=outcome) from exc
        except QualityGateReceiptError as exc:
            raise exc.with_outcome(outcome) from exc


class NestedLoadPackageTransaction:
    """Own ordered table mutation, package quality, state, and audit commit."""

    def __init__(
        self,
        *,
        root_table: str,
        load_one: SinglePayloadLoader,
        finalize_package: PackageFinalizer,
        identity_service: Any,
        load_record: Any,
        package_coordinator: NestedLoadPackageCoordinator,
        snapshot_runtime: Any,
        abort_package: PackageAbort | None = None,
    ) -> None:
        self._root_table = root_table
        self._load_one = load_one
        self._finalize_package = finalize_package
        self._identity_service = identity_service
        self._load_record = load_record
        self._package_coordinator = package_coordinator
        self._snapshot_runtime = snapshot_runtime
        self._abort_package = abort_package
        self._pending_loads: list[_PendingTableLoad] = []
        self._pending_snapshot: tuple[Any, tuple[Any, ...]] | None = None
        self._result: LoadResult | None = None
        self._state = "OPEN"

    def stage_result(self, **kwargs: Any) -> tuple[Any, ...]:
        return self._snapshot_runtime.stage_result(**kwargs)

    def stage_rows(self, **kwargs: Any) -> tuple[Any, ...]:
        return self._snapshot_runtime.stage_rows(**kwargs)

    def mark_staged(self, identity_service: Any, load_record: Any, *, extracted_rows: int) -> None:
        self._assert_owner(identity_service, load_record)
        if self._state != "OPEN":
            raise RuntimeError("nested load package staging order is invalid")
        self._package_coordinator.mark_staged(identity_service, load_record, extracted_rows=extracted_rows)
        self._state = "STAGED"

    def load_table(
        self,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
    ) -> LoadResult:
        """Queue one table mutation for child-first, root-last execution."""

        self._assert_owner(self._identity_service, load_record)
        if self._state != "STAGED":
            raise RuntimeError("nested load package mutation order is invalid")
        self._pending_loads.append(_PendingTableLoad(load_config, payload, extract_result, load_record))
        return LoadResult(inserted_rows=0, updated_rows=0, total_rows=0, staging_rows=0)

    def commit(self, *, store: Any | None, stages: tuple[Any, ...]) -> None:
        """Defer child snapshot state until package quality has passed."""

        if self._state != "STAGED" or self._pending_snapshot is not None:
            raise RuntimeError("nested load package state commit order is invalid")
        self._pending_snapshot = (store, stages)

    def mark_committed(self, identity_service: Any, load_record: Any, aggregate: LoadResult) -> None:
        self._assert_owner(identity_service, load_record)
        if self._state != "STAGED" or self._pending_snapshot is None:
            raise RuntimeError("nested load package commit order is invalid")
        try:
            member_results = [
                self._load_one(item.load_config, item.payload, item.extract_result, item.load_record)
                for item in self._ordered_loads()
            ]
            package_result = self._finalize_package(_aggregate_member_results(aggregate, member_results))
            store, stages = self._pending_snapshot
            self._snapshot_runtime.commit(store=store, stages=stages)
            self._package_coordinator.mark_committed(identity_service, load_record, package_result)
            self._result = package_result
            self._state = "COMMITTED"
        except Exception as primary:
            if isinstance(primary, NestedPackagePartialFinalizeError):
                raise
            try:
                self._run_abort_package()
            except Exception as abort_exc:
                raise abort_exc from primary
            raise

    def rollback(self, *, store: Any | None, stages: tuple[Any, ...]) -> None:
        if self._state == "COMMITTED":
            return
        abort_error: Exception | None = None
        try:
            self._run_abort_package()
        except NestedPackagePartialFinalizeError:
            pass
        except Exception as exc:
            abort_error = exc
        try:
            self._snapshot_runtime.rollback(store=store, stages=stages)
        except Exception:
            pass
        if abort_error is not None:
            raise abort_error

    def _run_abort_package(self) -> None:
        if self._abort_package is None:
            return
        self._abort_package()

    def mark_failed(self, identity_service: Any, load_record: Any, error: Exception) -> None:
        if identity_service is not self._identity_service or load_record is not self._load_record:
            return
        try:
            self._package_coordinator.mark_failed(identity_service, load_record, error)
        except Exception:
            pass
        self._state = "FAILED"

    def result_or(self, fallback: LoadResult) -> LoadResult:
        return self._result if self._result is not None else fallback

    def _ordered_loads(self) -> tuple[_PendingTableLoad, ...]:
        return tuple(
            sorted(
                self._pending_loads,
                key=lambda item: item.load_config.target_table == self._root_table,
            )
        )

    def _assert_owner(self, identity_service: Any, load_record: Any) -> None:
        if identity_service is not self._identity_service or load_record is not self._load_record:
            raise RuntimeError("nested load package owner mismatch")


def _aggregate_member_results(aggregate: LoadResult, results: list[LoadResult]) -> LoadResult:
    return replace(
        aggregate,
        inserted_rows=sum(result.inserted_rows for result in results),
        updated_rows=sum(result.updated_rows for result in results),
        total_rows=sum(result.total_rows for result in results),
        staging_rows=sum(result.staging_rows or 0 for result in results),
        soft_deleted_rows=sum(result.soft_deleted_rows or 0 for result in results) or None,
        replaced_rows=sum(result.replaced_rows or 0 for result in results) or None,
        deleted_lookback_rows=sum(result.deleted_lookback_rows or 0 for result in results) or None,
        quality_gate_receipt=_single_quality_receipt(results),
    )


def _single_quality_receipt(results: list[LoadResult]) -> Any | None:
    receipts: list[Any] = []
    for result in results:
        receipt = result.quality_gate_receipt
        if receipt is not None and not any(receipt is existing for existing in receipts):
            receipts.append(receipt)
    if len(receipts) > 1:
        raise RuntimeError("nested load package produced multiple quality receipts")
    return receipts[0] if receipts else None


def _package_member_load_config(load_config: Any) -> Any:
    options = getattr(load_config, "options", None)
    if not isinstance(options, Mapping) or "quality" not in options:
        return load_config
    raw_quality = options.get("quality")
    member_options = dict(options)
    if isinstance(raw_quality, Mapping) and "acceptance" in raw_quality:
        member_options["quality"] = {"acceptance": raw_quality["acceptance"]}
    else:
        member_options.pop("quality", None)
    return replace(load_config, options=member_options)
