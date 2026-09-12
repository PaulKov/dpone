"""Receipt-backed finalizer for every governed generic MSSQL strategy."""

from __future__ import annotations

from contextlib import suppress
from typing import Any

from dpone.contracts.mssql_transaction_governance import (
    MssqlCleanupDisposition,
    MssqlGenericCommitReceipt,
    MssqlTransactionAdmission,
)
from dpone.ports.composition_mssql_transaction import CompositionMssqlTransactionFence
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sinks.mssql_receipt_projection import load_result_from_mssql_receipt
from dpone.runtime.sinks.mssql_shadow_append_target import (
    assert_shadow_append_target_identity,
)
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    assert_target_catalog_expectations,
)
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan
from dpone.runtime.sinks.strategies.mssql.mssql_generic_target_contract import (
    MssqlGenericTargetContract,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    capture_loaded_at_utc as _capture_loaded_at_utc,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    mutation_fence_evidence as _mutation_fence_evidence,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    project_loaded_at as _project_loaded_at,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    receipt_matches as _receipt_matches,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    receipt_metrics as _metrics,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    require_payload_evidence as _require_payload_evidence,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    require_source_lifecycle as _require_source_lifecycle,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
    with_target_fence_evidence as _with_target_fence_evidence,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    acquire_mutation_locks,
    acquire_operation_lock,
    acquire_target_lock,
)
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState
from dpone.runtime.state.mssql_target_identity import assert_mssql_physical_target_identity


class MssqlGenericCommitOutcomeUnknown(RuntimeError):
    """Target commit ACK was lost without exact fresh-session receipt evidence."""

    code = "mssql_transaction.commit_outcome_unknown"
    cleanup_disposition = MssqlCleanupDisposition.PRESERVE_STAGING_EVIDENCE

    def __init__(self) -> None:
        super().__init__(self.code)


class MssqlGenericTransactionFinalizer:
    """Fence, mutate, receipt, and commit on one target connection."""

    def __init__(
        self,
        strategy: Any,
        state_storage: Any,
        *,
        transaction_state: Any | None = None,
        lock_acquirer: Any = acquire_operation_lock,
        target_lock_acquirer: Any = acquire_target_lock,
        target_identity_assertion: Any = assert_mssql_physical_target_identity,
        catalog_revalidator: Any = assert_target_catalog_expectations,
        target_contract_validator: Any | None = None,
        composition_fence: CompositionMssqlTransactionFence | None = None,
    ) -> None:
        self._composition_fence = composition_fence
        self._strategy = strategy
        self._connector = strategy.connector
        self._state = transaction_state or MssqlGenericTransactionState.from_state_storage(state_storage)
        self._operation_lock_acquirer = lock_acquirer
        self._target_lock_acquirer = target_lock_acquirer
        self._target_identity_assertion = target_identity_assertion
        self._catalog_revalidator = catalog_revalidator
        self._target_contract_validator = target_contract_validator or self._validate_target_contract

    def replay_result(self, receipt: MssqlGenericCommitReceipt) -> LoadResult:
        """Project a durable receipt without repeating any business DML."""

        if self._composition_fence is not None:
            try:
                self._connector.begin()
                self._composition_fence.require_current(self._connector, receipt=receipt)
            finally:
                self._connector.rollback()
        return load_result_from_mssql_receipt(receipt, outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED)

    def finalize(
        self,
        load_config: Any,
        admission: MssqlTransactionAdmission,
        handler: Any,
        staging: Any,
        *,
        staging_rows: int,
        load_id: str,
        target_mutation_plan: MssqlTargetMutationPlan | None = None,
        target_projection: Any | None = None,
        source_lifecycle_receipt: ExtractionLifecycleReceipt | None = None,
    ) -> LoadResult:
        if admission.replay_receipt is not None:
            return self.replay_result(admission.replay_receipt)
        operation = admission.operation
        if operation is None:
            raise RuntimeError("mssql_transaction.admission_operation_missing")
        payload_evidence = _require_payload_evidence(staging, staging_rows=staging_rows)
        source_lifecycle = _require_source_lifecycle(source_lifecycle_receipt)
        mutation_plan = target_mutation_plan or MssqlTargetMutationPlan.from_admission(admission)
        _require_exact_mutation_target(mutation_plan, operation)
        transaction_started = False
        commit_attempted = False
        try:
            self._connector.begin()
            transaction_started = True
            self._connector.execute_query("SET XACT_ABORT ON")
            self._connector.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            composition_transaction = (
                self._composition_fence.require_current(
                    self._connector, operation, mutation_plan_sha256=mutation_plan.digest
                )
                if self._composition_fence is not None
                else None
            )
            acquire_mutation_locks(
                self._connector,
                load_config,
                operation,
                target_lock_acquirer=self._target_lock_acquirer,
                operation_lock_acquirer=self._operation_lock_acquirer,
            )
            fence_evidence = _mutation_fence_evidence(self._connector, load_config, operation)
            assert_shadow_append_target_identity(
                self._connector,
                load_config,
                operation,
                identity_assertion=self._target_identity_assertion,
            )
            self._state.assert_current(
                self._connector,
                operation,
                require_unexpired_lease=True,
            )
            concurrent = self._state.probe_receipt(operation, connector=self._connector)
            if concurrent is not None:
                if not _receipt_matches(
                    concurrent,
                    payload_evidence=payload_evidence,
                    source_lifecycle=source_lifecycle,
                    mutation_plan=mutation_plan,
                ):
                    raise RuntimeError("mssql_transaction.concurrent_receipt_payload_mismatch")
                # The durable receipt predates this read-only transaction.
                # Roll it back to release locks; no COMMIT acknowledgement is
                # needed to classify the already-proven business outcome.
                try:
                    self._connector.rollback()
                except Exception:
                    self._close_target()
                transaction_started = False
                return _with_target_fence_evidence(
                    load_result_from_mssql_receipt(concurrent, outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED),
                    fence_evidence,
                )
            self._catalog_revalidator(
                self._strategy,
                load_config,
                mutation_plan.expectations,
                boundary="before",
            )
            for phase, expectation_kind in (
                ("schema_evolution", "schema_columns"),
                ("physical_design", "physical_design"),
            ):
                phase_actions = tuple(action for action in mutation_plan.actions if action.kind == phase)
                for action in phase_actions:
                    self._connector.execute_query(action.sql)
                if phase_actions:
                    self._catalog_revalidator(
                        self._strategy,
                        load_config,
                        mutation_plan.expectations,
                        boundary="after",
                        kinds=frozenset({expectation_kind}),
                    )
            self._catalog_revalidator(
                self._strategy,
                load_config,
                mutation_plan.expectations,
                boundary="after",
            )
            loaded_at_utc = _capture_loaded_at_utc(self._connector)
            _project_loaded_at(self._connector, staging, loaded_at_utc)
            self._target_contract_validator(
                load_config,
                staging,
                target_projection,
            )
            business = handler(staging)
            metrics = _metrics(business, staging_rows=staging_rows)
            # Strategy implementations may perform target-local DDL (for
            # example a shadow swap).  Re-prove the complete after-image so
            # no unplanned catalog mutation can be receipted as ordinary DML.
            self._catalog_revalidator(
                self._strategy,
                load_config,
                mutation_plan.expectations,
                boundary="after",
            )
            # Re-evaluate lease/owner after all business DML. A chunk that
            # outlived its TTL rolls back instead of committing as a stale
            # owner while a replacement worker is waiting to reclaim it.
            self._state.assert_current(
                self._connector,
                operation,
                require_unexpired_lease=False,
            )
            if self._composition_fence is not None:
                self._composition_fence.require_current(
                    self._connector,
                    operation,
                    transaction_id=composition_transaction,
                    mutation_plan_sha256=mutation_plan.digest,
                )
            receipt = self._state.insert_receipt(
                self._connector,
                operation,
                load_id=load_id,
                payload_evidence=payload_evidence,
                source_lifecycle=source_lifecycle,
                mutation_plan_sha256=mutation_plan.digest,
                target_before_sha256=mutation_plan.expected_before_sha256,
                target_after_sha256=mutation_plan.expected_after_sha256,
                loaded_at_utc=loaded_at_utc,
                metrics=metrics,
            )
            result = _with_target_fence_evidence(
                load_result_from_mssql_receipt(receipt, outcome=AtomicCommitOutcome.COMMITTED),
                fence_evidence,
            )
            commit_attempted = True
            self._connector.commit_transaction()
            transaction_started = False
            return result
        except Exception as exc:
            if commit_attempted:
                transaction_started = False
                self._close_target()
                probed = self._state.probe_receipt_fresh(operation)
                if probed is not None and _receipt_matches(
                    probed,
                    payload_evidence=payload_evidence,
                    source_lifecycle=source_lifecycle,
                    mutation_plan=mutation_plan,
                ):
                    return _with_target_fence_evidence(
                        load_result_from_mssql_receipt(
                            probed,
                            outcome=AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE,
                        ),
                        fence_evidence,
                    )
                if probed is not None:
                    raise MssqlGenericCommitOutcomeUnknown() from RuntimeError(
                        "mssql_transaction.commit_receipt_payload_mismatch"
                    )
                raise MssqlGenericCommitOutcomeUnknown() from exc
            if transaction_started:
                with suppress(Exception):
                    self._connector.rollback()
            raise

    def _close_target(self) -> None:
        closer = getattr(self._connector, "close", None)
        if callable(closer):
            with suppress(Exception):
                closer()

    def _validate_target_contract(
        self,
        load_config: Any,
        staging: Any,
        target_projection: Any | None,
    ) -> None:
        MssqlGenericTargetContract(self._strategy).validate_existing(
            load_config,
            staging,
            target_projection=target_projection,
        )


def _require_exact_mutation_target(plan: MssqlTargetMutationPlan, operation: Any) -> None:
    request = operation.attempt.request
    exact = (
        plan.target_identity == operation.attempt.target_identity
        and plan.target_database == request.target_database
        and plan.target_schema == request.target_schema
        and plan.target_table == request.target_table
    )
    if not exact:
        raise RuntimeError("mssql_transaction.target_mutation_plan_mismatch")


__all__ = ["MssqlGenericCommitOutcomeUnknown", "MssqlGenericTransactionFinalizer"]
