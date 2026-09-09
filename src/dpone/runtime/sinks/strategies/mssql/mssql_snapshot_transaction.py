"""Target-atomic transaction for one normalized MSSQL key snapshot."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from dpone.runtime.incremental_snapshot import (
    IncrementalSnapshotEnvelope,
    KeySnapshotReconciliationPolicy,
)
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_hash import row_hash_expression
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_options import (
    MssqlSnapshotOptionError,
    acquire_target_lock,
    lock_timeout_ms,
    target_lock_resource,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
    business_columns,
    business_schema,
    changed_lineage_assignments,
    load_identity,
    resolved_target_type,
    staging_scalar_expression,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
    load_id as projection_load_id,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import (
    SnapshotActionMetrics,
    SnapshotReconciliationError,
    SnapshotReconciliationSql,
    SoftDeletePolicy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_repair import (
    admit_snapshot_repair,
    consume_snapshot_repair,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target import (
    MssqlCommitOutcomeUnknown,
    frozen_target_config,
    require_frozen_target_coordinates,
)

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact


class MssqlSnapshotTransaction:
    """Fence, reconcile, receipt, and commit one complete-key snapshot."""

    def __init__(
        self,
        *,
        strategy: Any,
        connector: Any,
        state: Any,
        contract: Any,
        mutation: Any,
        publication_head: Any,
        detection_time: Callable[[], Any],
        insert_absent: Callable[..., int],
        probe_commit: Callable[[Any, str], Any],
    ) -> None:
        self._strategy = strategy
        self._connector = connector
        self._state = state
        self._contract = contract
        self._mutation = mutation
        self._publication_head = publication_head
        self._detection_time = detection_time
        self._insert_absent = insert_absent
        self._probe_commit = probe_commit

    def finalize(
        self,
        load_config: Any,
        payload_schema: Any,
        envelope: IncrementalSnapshotEnvelope[Any, Any],
        delta: StagingTableArtifact,
        keys: StagingTableArtifact,
        policy: KeySnapshotReconciliationPolicy,
    ) -> LoadResult:
        """Apply target DML and checkpoint under one serializable transaction."""

        load_config = frozen_target_config(load_config, envelope)
        soft_delete = SoftDeletePolicy.from_options((getattr(load_config, "options", {}) or {}).get("soft_delete"))
        renderer = SnapshotReconciliationSql(self._connector.quote_identifier, soft_delete)
        unique_key = tuple(envelope.key_receipt.key_columns)
        target = self._strategy._target_name(load_config)
        delta_name = self._strategy._staging_name(delta)
        key_name = self._strategy._staging_name(keys)
        business = business_columns(payload_schema)
        transaction_started = False
        commit_attempted = False
        result: LoadResult | None = None
        try:
            self._connector.begin()
            transaction_started = True
            self._connector.execute_query("SET XACT_ABORT ON")
            self._connector.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            try:
                timeout_ms = lock_timeout_ms(load_config)
                acquire_target_lock(
                    self._connector,
                    target_lock_resource(envelope.state_key.target_identity),
                    database=envelope.state_key.target_database,
                    timeout_ms=timeout_ms,
                )
                require_frozen_target_coordinates(load_config, envelope)
                publication_authority = self._publication_head.acquire(
                    load_config,
                    envelope,
                    timeout_ms=timeout_ms,
                )
                self._state.assert_physical_target_identity(
                    executor=self._connector,
                    key=envelope.state_key,
                )
                self._publication_head.require_locked(publication_authority, envelope)
            except MssqlSnapshotOptionError as exc:
                raise SnapshotReconciliationError(str(exc)) from exc
            effective_at = self._detection_time()
            self._contract.ensure_target(load_config, payload_schema, unique_key, soft_delete)
            self._contract.validate_target(load_config, payload_schema, unique_key, soft_delete)
            active_before = self._mutation.count(target, renderer.active_predicate("t"), alias="t")
            total_before = self._mutation.count(target)
            missing = self._mutation.missing_active_count(target, key_name, unique_key, renderer)
            repair = admit_snapshot_repair(
                state=self._state,
                executor=self._connector,
                load_config=load_config,
                envelope=envelope,
                policy=policy,
                target_rows_before=total_before,
                active_rows_before=active_before,
                missing_rows=missing,
            )
            self._state.assert_or_transfer_target_authority(
                executor=self._connector,
                key=envelope.state_key,
                authority=repair.authority,
            )
            run_id, load_id, extracted_at = load_identity(load_config)
            assignments = self._changed_assignments(
                load_config,
                payload_schema,
                delta,
                business,
            )
            business_assignments, row_hash = assignments
            changed_params = (run_id, load_id, extracted_at, effective_at)
            reactivated = self._mutation.action_count(
                renderer.reactivate(
                    target=target,
                    delta=delta_name,
                    unique_key=unique_key,
                    assignments=business_assignments,
                    row_hash_expression=row_hash,
                ),
                changed_params,
            )
            updated = self._mutation.action_count(
                renderer.update_changed(
                    target=target,
                    delta=delta_name,
                    unique_key=unique_key,
                    assignments=business_assignments,
                    row_hash_expression=row_hash,
                ),
                changed_params,
            )
            inserted = self._insert_absent(
                load_config,
                delta,
                unique_key,
                business,
                row_hash,
                renderer,
                run_id,
                load_id,
                extracted_at,
                effective_at,
            )
            delete_params = (effective_at,) if soft_delete.timestamp_is_source_of_truth else ()
            soft_deleted = self._mutation.action_count(
                renderer.soft_delete_missing(target=target, keys=key_name, unique_key=unique_key),
                delete_params,
            )
            self._mutation.assert_key_parity(target, key_name, unique_key, renderer)
            active = self._mutation.count(target, renderer.active_predicate("t"), alias="t")
            total = self._mutation.count(target)
            metrics = SnapshotActionMetrics.from_action_counts(
                inserted=inserted,
                updated=updated,
                reactivated=reactivated,
                soft_deleted=soft_deleted,
                hard_deleted=0,
                active=active,
                total=total,
                staging=delta.row_count,
                snapshot_effective_at=effective_at,
            )
            receipt = self._state.compare_and_set_with_receipt(
                executor=self._connector,
                key=envelope.state_key,
                expected=envelope.previous_checkpoint,
                candidate=envelope.candidate_checkpoint,
                load_id=load_id,
                snapshot_token=envelope.snapshot_token,
            )
            consume_snapshot_repair(
                state=self._state,
                executor=self._connector,
                admission=repair,
                envelope=envelope,
                load_id=load_id,
                receipt_id=receipt.receipt_id,
            )
            result = LoadResult(
                inserted_rows=metrics.inserted,
                updated_rows=metrics.updated,
                total_rows=metrics.total,
                staging_rows=metrics.staging,
                soft_deleted_rows=metrics.soft_deleted,
                reactivated_rows=metrics.reactivated,
                unchanged_rows=metrics.unchanged,
                hard_deleted_rows=metrics.hard_deleted,
                active_rows=metrics.active,
                commit_receipt_id=receipt.receipt_id,
                commit_outcome=AtomicCommitOutcome.COMMITTED,
                reconciliation_metrics={**metrics.to_dict(), **repair.evidence()},
            )
            # Invoking the driver COMMIT is the point after which the server
            # outcome can no longer be inferred from the local exception.
            # Keep this boundary explicit: every earlier failure is safely
            # rollback-able, including one after the receipt/checkpoint DML.
            commit_attempted = True
            self._connector.commit_transaction()
            transaction_started = False
            return result
        except Exception as exc:
            if commit_attempted:
                transaction_started = False
                closer = getattr(self._connector, "close", None)
                if callable(closer):
                    with suppress(Exception):
                        closer()
                probed = self._probe_commit(envelope, projection_load_id(load_config))
                if (
                    probed is not None
                    and probed.candidate_xmin == envelope.candidate_checkpoint.xmin_value
                    and probed.candidate_revision == receipt.candidate_revision
                ):
                    return replace(
                        result,
                        commit_receipt_id=probed.receipt_id,
                        commit_outcome=AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE,
                    )
                raise MssqlCommitOutcomeUnknown() from exc
            if transaction_started:
                self._connector.rollback()
            raise

    def _changed_assignments(
        self,
        load_config: Any,
        payload_schema: Any,
        delta: StagingTableArtifact,
        business: list[str],
    ) -> tuple[tuple[str, ...], str]:
        business_assignments = tuple(
            f"{self._connector.quote_identifier(column)} = "
            f"{staging_scalar_expression(self._strategy, delta, column, 's')}"
            for column in business
        )
        assignments = (*business_assignments, *changed_lineage_assignments(self._connector.quote_identifier))
        hash_schema = tuple(
            (column, resolved_target_type(load_config, column, source_type))
            for column, source_type in business_schema(payload_schema)
        )
        row_hash = row_hash_expression(
            hash_schema,
            lambda column: staging_scalar_expression(self._strategy, delta, column, "s"),
        )
        return assignments, row_hash


__all__ = ["MssqlSnapshotTransaction"]
