"""Source-state loading and persistence for ETL orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.etl.strategy_policy import should_load_incremental_state
from dpone.runtime.kafka.offsets import KafkaOffsetState
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.state.xmin_storage import XMinState

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class SourceStateService:
    """Owns source-state decisions and concrete state routing."""

    def load_for_extract(self, source: Any, load_config: LoadConfig) -> Any:
        """Load previously saved source state when the strategy is stateful."""

        if should_load_incremental_state(load_config.load_strategy):
            return source.get_incremental_state(load_config)
        return None

    def persist_after_load(
        self,
        *,
        source: Any,
        sink: Any,
        original_load_config: LoadConfig,
        effective_load_config: LoadConfig,
        extract_result: Any,
        should_persist: bool,
        load_result: Any | None = None,
    ) -> None:
        """Persist extracted source state to the state owner after a successful load."""

        if not should_persist:
            return

        if getattr(extract_result, "snapshot_envelope", None) is not None:
            # The MSSQL finalizer advances this checkpoint inside the same
            # transaction as target mutation and its commit receipt.
            accepted_outcomes = {
                AtomicCommitOutcome.COMMITTED,
                AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE,
                AtomicCommitOutcome.REPLAY_SUPPRESSED,
            }
            outcome = getattr(load_result, "commit_outcome", None)
            receipt_id = getattr(load_result, "commit_receipt_id", None)
            if outcome not in accepted_outcomes or not receipt_id:
                raise RuntimeError(
                    "Snapshot-envelope load did not return a committed checkpoint receipt; "
                    "refusing to report success or persist state separately"
                )
            return

        state = getattr(extract_result, "state", None)
        if getattr(load_result, "commit_outcome", None) == AtomicCommitOutcome.REPLAY_SUPPRESSED and isinstance(
            state,
            (KafkaOffsetState, XMinState),
        ):
            raise RuntimeError("mssql_transaction.replay_source_checkpoint_recovery_required")
        if isinstance(state, dict):
            return

        if isinstance(state, KafkaOffsetState) and hasattr(source, "save_state"):
            source.save_state(effective_load_config, state)
            return

        if isinstance(state, XMinState):
            sink.save_state(original_load_config, state)


__all__ = ["SourceStateService"]
