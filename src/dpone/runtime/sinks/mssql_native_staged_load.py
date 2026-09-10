"""Owned native MSSQL staging lifecycle and receipt-backed publication.

Preparation is injected independently from publication. The service never reads
source rows during finalization and retains all stages while commit is unknown.
"""

from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import (
    MssqlGenericCommitOutcomeUnknown,
    MssqlGenericTransactionFinalizer,
)


@dataclass(frozen=True)
class NativePreparedStage:
    """Verified prepared table and frozen target/source receipt inputs.

    ``interval`` is the already resolved, authored UTC half-open interval.
    It is mandatory for partition replacement, including an empty replacement.
    """

    staging: Any
    admission: Any
    source_lifecycle: Any
    mutation_plan: Any
    target_projection: Any
    interval: Any = None
    resources: tuple[Any, ...] = ()


@dataclass
class _OwnedStage:
    config: Any
    handle: StagedLoadHandle
    prepared: NativePreparedStage
    status: str = "prepared"


class MssqlNativeStagedLoadService:
    """Keep stage ownership and finalization state behind one sink instance."""

    def __init__(self, sink: Any, preparer: Any) -> None:
        self._sink = sink
        self._preparer = preparer
        self._owned: dict[str, _OwnedStage] = {}

    def stage(self, load_config: Any, payload: Any) -> StagedLoadHandle:
        prepared = self._preparer.stage(load_config, payload)
        return self._register(load_config, prepared, tuple(getattr(payload, "schema", ())))

    def _register(self, load_config: Any, prepared: NativePreparedStage, schema: tuple[Any, ...]) -> StagedLoadHandle:
        token = uuid4().hex
        artifact = prepared.staging
        handle = StagedLoadHandle(
            staging_config=artifact,
            finalization_config=artifact,
            payload_schema=schema,
            staged_rows=artifact.row_count,
            metadata={"mssql_native_owner": token, "transport": "mssql_native"},
        )
        self._owned[token] = _OwnedStage(deepcopy(load_config), handle, prepared)
        return handle

    def finalize(self, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
        owned = self._require_owned(handle)
        scope = getattr(self._preparer, "publication_scope", None)
        with scope(owned.prepared) if scope is not None else nullcontext():
            return self._finalize_owned(load_config, handle, owned)

    def _finalize_owned(self, load_config: Any, handle: StagedLoadHandle, owned: _OwnedStage) -> LoadResult:
        if owned.config != load_config:
            raise ValueError("mssql_native.configuration_changed")
        if owned.status == "published":
            raise RuntimeError("mssql_native.already_published")
        if owned.status != "prepared":
            raise RuntimeError("mssql_native.publication_unresolved")
        self._preparer.reverify(owned.prepared)
        strategy = self._sink._strategy_map[load_config.load_strategy]
        factory = strategy.transaction_finalizer_factory or MssqlGenericTransactionFinalizer
        finalizer = factory(strategy, strategy.state_storage)
        prepared = owned.prepared
        operation = prepared.admission.operation
        if operation is None:
            raise RuntimeError("mssql_transaction.admission_operation_missing")
        started = getattr(self._preparer, "publication_started", None)
        if started is not None:
            started(prepared)
        owned.status = "publishing"
        try:
            result = finalizer.finalize(
                load_config,
                prepared.admission,
                self._handler(strategy, load_config, prepared),
                prepared.staging,
                staging_rows=handle.staged_rows,
                load_id=operation.attempt.request.load_id,
                target_mutation_plan=prepared.mutation_plan,
                target_projection=prepared.target_projection,
                source_lifecycle_receipt=prepared.source_lifecycle,
            )
        except MssqlGenericCommitOutcomeUnknown:
            owned.status = "outcome_unknown"
            raise
        except BaseException as error:
            owned.status = "outcome_unknown" if started is not None or not isinstance(error, Exception) else "prepared"
            raise
        owned.status = "published"
        confirmed = getattr(self._preparer, "publication_confirmed", None)
        if confirmed is not None:
            confirmed(prepared, result)
        return result

    def resume(self, load_config: Any, context: Any, admission: Any) -> StagedLoadHandle | LoadResult | None:
        """Probe publication first; no source or target stage read after commit."""
        from dpone.runtime.sinks.load_result import AtomicCommitOutcome
        from dpone.runtime.sinks.mssql_receipt_projection import load_result_from_mssql_receipt
        from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalization_evidence import (
            receipt_matches,
            require_payload_evidence,
            require_source_lifecycle,
        )
        from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState

        journal = context.journal_factory()
        state = journal.publication.state()
        if state is None or state["phase"] == "preparing":
            if journal.data is None:
                return None
            if journal.completed() is None:
                context.executor.recover(context.plan, context.lease)
                raise ValueError("mssql_native.reextract_required")
            prepared = self._preparer.recover_stage(load_config, context, admission)
            return self._register(load_config, prepared, tuple(prepared.staging.wire_schema))
        prepared = self._preparer.restore(load_config, context, admission)
        if prepared is None:
            raise ValueError("mssql_native.prepared_journal_required")
        if state["phase"] == "prepared" and admission.replay_receipt is None:
            self._preparer.reverify(prepared)
            return self._register(load_config, prepared, tuple(prepared.staging.wire_schema))
        receipt = admission.replay_receipt
        if receipt is None:
            transaction = MssqlGenericTransactionState.from_state_storage(self._sink.state_storage)
            receipt = transaction.probe_receipt_fresh(admission.operation)
        if receipt is None or not receipt_matches(
            receipt,
            payload_evidence=require_payload_evidence(prepared.staging, staging_rows=prepared.staging.row_count),
            source_lifecycle=require_source_lifecycle(prepared.source_lifecycle),
            mutation_plan=prepared.mutation_plan,
        ):
            raise MssqlGenericCommitOutcomeUnknown()
        if receipt.operation_key.hex() != state["prepared"]["operation_key"]:
            raise MssqlGenericCommitOutcomeUnknown()
        result = load_result_from_mssql_receipt(receipt, outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED)
        if state["phase"] == "prepared":
            journal.publication.publication_started(state["prepared"])
        if state["phase"] in {"prepared", "publishing"}:
            journal.publication.publication_confirmed({"receipt_id": result.commit_receipt_id})
        return result

    def cleanup_recovered(self, load_config: Any, context: Any, admission: Any) -> None:
        """Release source-free recovered stages after evidence/state succeeded."""
        state = context.journal_factory().publication.state()
        if state is None or state["phase"] != "succeeded":
            raise RuntimeError("mssql_native.recovered_completion_required")
        prepared = self._preparer.restore(load_config, context, admission)
        if prepared is None:
            raise ValueError("mssql_native.prepared_journal_required")
        self._preparer.cleanup(prepared)

    def abort(self, handle: StagedLoadHandle) -> None:
        owned = self._require_owned(handle)
        if owned.status in {"publishing", "outcome_unknown", "published"}:
            raise RuntimeError("mssql_native.publication_unresolved")
        self._preparer.cleanup(owned.prepared)
        self._owned.pop(str(handle.metadata["mssql_native_owner"]))

    def cleanup(self, handle: StagedLoadHandle) -> None:
        """Release proven published resources without rerunning publication."""

        owned = self._require_owned(handle)
        if owned.status != "published":
            raise RuntimeError("mssql_native.publication_unresolved")
        self._preparer.cleanup(owned.prepared)
        self._owned.pop(str(handle.metadata["mssql_native_owner"]))

    def load(self, load_config: Any, payload: Any) -> LoadResult:
        handle = self.stage(load_config, payload)
        try:
            result = self.finalize(load_config, handle)
        except MssqlGenericCommitOutcomeUnknown:
            raise
        except BaseException as error:
            try:
                self.abort(handle)
            except Exception as cleanup_error:
                error.add_note(f"native stage cleanup failed: {type(cleanup_error).__name__}")
            raise
        self.cleanup(handle)
        return result

    def _require_owned(self, handle: StagedLoadHandle) -> _OwnedStage:
        token = str(handle.metadata.get("mssql_native_owner", ""))
        owned = self._owned.get(token)
        if owned is None or owned.handle is not handle:
            raise ValueError("mssql_native.handle_not_owned")
        return owned

    @staticmethod
    def _handler(strategy: Any, config: Any, prepared: NativePreparedStage) -> Any:
        def publish(staging: Any) -> LoadResult:
            replaced = None
            if config.load_strategy == LoadStrategy.FULL_REFRESH:
                if strategy._table_exists(config):
                    strategy.connector.execute_query(f"TRUNCATE TABLE {strategy._target_name(config)}")
                else:
                    schema = tuple(staging.target_column_types.items())
                    strategy._ensure_target_table(config, schema, staging=staging)
            elif config.load_strategy == LoadStrategy.PARTITION_REPLACE:
                interval = prepared.interval
                if interval is None:
                    raise ValueError("mssql_native.authored_interval_required")
                column = strategy.connector.quote_identifier(interval.column)
                predicate = f"{column} >= ? AND {column} < ?"
                params = (interval.start, interval.end)
                replaced = strategy._count_target_matching_sql(config, predicate, params)
                strategy.connector.execute_query(
                    f"DELETE FROM {strategy._target_name(config)} WHERE {predicate}", params
                )
            else:
                raise ValueError("mssql_native.strategy_not_admitted")
            inserted = strategy._insert_from_staging_to_table(config, staging, config.target_table, table_lock=True)
            return LoadResult(
                inserted_rows=inserted,
                updated_rows=0,
                total_rows=strategy._count_target(config),
                replaced_rows=replaced,
            )

        return publish
