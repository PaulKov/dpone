"""Staging ownership and transaction dispatch for generic MSSQL loads."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleStateError
from dpone.runtime.sinks.mssql_transaction_requirement import require_generic_transaction_state
from dpone.runtime.sinks.strategies.mssql.mssql_initial_typed_staging import (
    is_direct_xmin_initial_staging_candidate,
    plan_direct_xmin_initial_staging,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import (
    MssqlNativeLineageProjection,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import MssqlNativeStagingNormalizer
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_finalizer import (
    MssqlGenericCommitOutcomeUnknown,
    MssqlGenericTransactionFinalizer,
)

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact
    from dpone.runtime.sinks.load_result import LoadResult

_DEFERRED_EXTRACTION_STATES = frozenset(
    {
        "extraction_lifecycle.extraction_not_started",
        "extraction_lifecycle.extraction_not_completed",
    }
)

_DEFERRED_PLANNING_STATES = frozenset({"extraction_lifecycle.extraction_not_started"})


class MssqlStagingConsumer:
    """Materialize once, then choose legacy or receipt-backed finalization."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy

    def consume(self, load_config: Any, payload: Any, handler: Any) -> LoadResult:
        admission = getattr(payload, "mssql_transaction_admission", None)
        state_storage = require_generic_transaction_state(load_config, self._strategy.state_storage)
        if not isinstance(admission, MssqlTransactionAdmission):
            raise RuntimeError("mssql_transaction.pre_source_admission_required")
        factory = self._strategy.transaction_finalizer_factory or MssqlGenericTransactionFinalizer
        finalizer = factory(self._strategy, state_storage)
        if admission.replay_receipt is not None:
            return finalizer.replay_result(admission.replay_receipt)

        raw_staging: StagingTableArtifact | None = None
        staging: StagingTableArtifact | None = None
        try:
            schema = [(str(column), str(dtype)) for column, dtype in payload.schema]
            normalizer = MssqlNativeStagingNormalizer(self._strategy)
            resolved = normalizer.resolve_schema(
                load_config,
                schema,
                relation_schema=payload.relation_schema,
                relation_metadata=payload.relation_metadata,
                relation_dialect=payload.relation_dialect,
                target_projection=payload.target_projection,
            )
            source_lifecycle = _completed_extraction_or_none(payload)
            direct_candidate = is_direct_xmin_initial_staging_candidate(
                load_config,
                payload.artifact,
                schema,
            )
            planning_lifecycle = source_lifecycle
            if planning_lifecycle is None and direct_candidate:
                planning_lifecycle = _acquired_extraction_or_none(payload)
            planning_lineage = (
                MssqlNativeLineageProjection.resolve(load_config, planning_lifecycle)
                if planning_lifecycle is not None
                else None
            )
            planned_resolved = resolved.with_lineage(planning_lineage) if planning_lineage is not None else None
            direct_plan = (
                plan_direct_xmin_initial_staging(
                    load_config,
                    payload.artifact,
                    schema,
                    planned_resolved,
                )
                if direct_candidate and planned_resolved is not None
                else None
            )
            materialization_config = direct_plan.load_config if direct_plan is not None else load_config
            staging_schema = direct_plan.staging_schema if direct_plan is not None else schema
            raw_staging = self._strategy._materialize(
                materialization_config,
                payload,
                staging_schema=staging_schema,
            )
            source_lifecycle = payload.require_completed_extraction()
            if source_lifecycle is planning_lifecycle:
                lineage = planning_lineage
                resolved_with_lineage = planned_resolved
            else:
                lineage = MssqlNativeLineageProjection.resolve(load_config, source_lifecycle)
                resolved_with_lineage = resolved.with_lineage(lineage)
            if lineage is None or resolved_with_lineage is None:  # protected by the branches above
                raise RuntimeError("mssql_transaction.completed_source_lifecycle_required")
            if direct_plan is not None:
                _require_stable_direct_plan(
                    planning_lifecycle,
                    source_lifecycle,
                    planning_lineage,
                    lineage,
                    planned_resolved,
                    resolved_with_lineage,
                    direct_plan,
                )
            staging = normalizer.normalize(
                load_config,
                raw_staging,
                schema,
                resolved_with_lineage,
                lineage=lineage,
            )
            operation = admission.operation
            if operation is None:
                raise RuntimeError("mssql_transaction.admission_operation_missing")
            result = finalizer.finalize(
                load_config,
                admission,
                handler,
                staging,
                staging_rows=raw_staging.row_count,
                load_id=operation.attempt.request.load_id,
                target_mutation_plan=getattr(payload, "mssql_target_mutation_plan", None),
                target_projection=payload.target_projection,
                source_lifecycle_receipt=source_lifecycle,
            )
            result = _with_staging_execution_evidence(result, raw_staging)
        except MssqlGenericCommitOutcomeUnknown:
            # The target handle is closed and staging is durable evidence until
            # an operator resolves the ambiguous receipt outcome.
            raise
        except BaseException:
            self._cleanup_after_terminal((staging, raw_staging), committed=False)
            raise
        self._cleanup_after_terminal((staging, raw_staging), committed=True)
        return result

    def _cleanup_after_terminal(self, artifacts: tuple[Any, ...], *, committed: bool) -> None:
        seen: set[int] = set()
        for artifact in artifacts:
            if artifact is None or id(artifact) in seen:
                continue
            seen.add(id(artifact))
            try:
                artifact.cleanup()
            except Exception:
                # Cleanup is secondary both to a pre-commit primary exception
                # and to a receipt-backed committed outcome.
                with suppress(Exception):
                    disposition = "committed_secondary" if committed else "primary_error_preserved"
                    self._strategy.logger.warning(f"event=dpone.mssql_cleanup_failed disposition={disposition}")


def _completed_extraction_or_none(payload: Any) -> Any | None:
    """Allow a lazy source to complete only while its staging artifact is consumed."""

    try:
        return payload.require_completed_extraction()
    except ExtractionLifecycleStateError as exc:
        if str(exc) not in _DEFERRED_EXTRACTION_STATES:
            raise
        return None


def _acquired_extraction_or_none(payload: Any) -> Any | None:
    """Return a stable start receipt for immutable staging planning only."""

    try:
        return payload.require_acquired_extraction()
    except ExtractionLifecycleStateError as exc:
        if str(exc) not in _DEFERRED_PLANNING_STATES:
            raise
        return None


def _require_stable_direct_plan(
    planning_lifecycle: Any,
    final_lifecycle: Any,
    planning_lineage: Any,
    final_lineage: Any,
    planned_resolved: Any,
    final_resolved: Any,
    direct_plan: Any,
) -> None:
    """Fail closed if source completion changes an admitted native plan."""

    lifecycle_fields = (
        "extraction_started_at",
        "clock_authority",
        "snapshot_acquired_at",
        "snapshot_authority",
        "source_token",
    )
    if planning_lifecycle is None or any(
        getattr(planning_lifecycle, field) != getattr(final_lifecycle, field) for field in lifecycle_fields
    ):
        raise RuntimeError("mssql_native_projection.direct_staging_lifecycle_changed_after_completion")
    if planning_lineage != final_lineage or planned_resolved != final_resolved:
        raise RuntimeError("mssql_native_projection.direct_staging_projection_changed_after_completion")
    if tuple(direct_plan.staging_schema) != tuple(final_resolved.target_schema):
        raise RuntimeError("mssql_native_projection.direct_staging_schema_changed_after_completion")


def _with_staging_execution_evidence(result: LoadResult, staging: Any) -> LoadResult:
    """Expose the actual staging route without file paths or credentials."""

    metrics = dict(result.reconciliation_metrics or {})
    metrics["mssql_staging_evidence"] = {
        "schema": "dpone.mssql.staging-execution-evidence.v1",
        "typed_file_ingestion": bool(getattr(staging, "typed_file_ingestion", False)),
        "direct_native_staging": bool(getattr(staging, "direct_native_staging", False)),
        "transport": getattr(staging, "typed_transport", None),
    }
    return replace(result, reconciliation_metrics=metrics)


__all__ = ["MssqlStagingConsumer"]
