"""Atomic MSSQL finalizer for XMin delta plus complete-key snapshots."""

from __future__ import annotations

from contextlib import suppress
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dpone.runtime.incremental_snapshot import (
    IncrementalSnapshotEnvelope,
    KeySnapshotReconciliationPolicy,
)
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.mssql.mssql_incremental_publication_head import (
    MssqlIncrementalPublicationHeadGuard,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_contract import MssqlSnapshotContract
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_mutation import MssqlSnapshotMutationExecutor
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_options import (
    MssqlSnapshotOptionError,
    require_safe_route_options,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
    business_columns as _business_columns,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
    changed_lineage_assignments as _changed_lineage_assignments,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import (
    SnapshotReconciliationError,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_staging import (
    MssqlSnapshotStagingNormalizer,
    NormalizedSnapshotStaging,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_target import (
    MssqlCommitOutcomeUnknown,
    frozen_target_config,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_transaction import MssqlSnapshotTransaction
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_typed_config import (
    typed_snapshot_staging_configs,
)

if TYPE_CHECKING:
    from dpone.runtime.artifact_models import StagingTableArtifact


class MssqlSnapshotFinalizer:
    """Stage, validate, mutate target and checkpoint, then commit exactly once."""

    def __init__(
        self,
        strategy: Any,
        state_storage: Any,
        *,
        publication_head_guard: Any | None = None,
    ) -> None:
        self._strategy = strategy
        self._connector = strategy.connector
        self._staging = strategy.staging_manager
        self._state = state_storage
        self._contract = MssqlSnapshotContract(strategy)
        self._mutation = MssqlSnapshotMutationExecutor(strategy, self._connector)
        self._normalizer = MssqlSnapshotStagingNormalizer(strategy)
        self._publication_head = publication_head_guard or MssqlIncrementalPublicationHeadGuard(
            connector=self._connector,
            state_storage=state_storage,
        )

    def load(self, load_config: Any, payload: Any, envelope: IncrementalSnapshotEnvelope[Any, Any]) -> LoadResult:
        policy = KeySnapshotReconciliationPolicy.from_runtime(
            getattr(load_config, "options", None),
            legacy_enabled=bool(getattr(load_config, "reconciliation", False)),
        )
        if not policy.key_snapshot_enabled:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.policy_missing")
        try:
            require_safe_route_options(load_config)
        except MssqlSnapshotOptionError as exc:
            raise SnapshotReconciliationError(str(exc)) from exc
        if self._state is None or getattr(self._state, "atomicity", None) != "target_atomic":
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.atomic_state_required")

        load_config = frozen_target_config(load_config, envelope)
        delta_config, key_config = typed_snapshot_staging_configs(self._connector, load_config, envelope)
        raw_delta: StagingTableArtifact | None = None
        raw_keys: StagingTableArtifact | None = None
        normalized: NormalizedSnapshotStaging | None = None
        cleanup_staging = True
        try:
            raw_delta = envelope.materialize(self._staging, delta_config, envelope.delta_schema)
            raw_keys = envelope.materialize_keys(self._staging, key_config)
            normalized = self._normalizer.normalize(load_config, envelope, raw_delta, raw_keys)
            self._contract.validate_staging(envelope, normalized.delta, normalized.keys)
            self._contract.index_keys(normalized.keys, envelope.key_receipt.key_columns)
            result = self._finalize(
                load_config,
                envelope.delta_schema,
                envelope,
                normalized.delta,
                normalized.keys,
                policy,
            )
            cleanup_staging = True
            return result
        except MssqlCommitOutcomeUnknown as error:
            cleanup_staging = False
            for failure in self._release_preserved_authority_leases(normalized, raw_keys, raw_delta):
                add_exception_note(error, failure)
            raise
        finally:
            if cleanup_staging:
                if normalized is not None:
                    with suppress(Exception):
                        normalized.cleanup()
                if raw_keys is not None and (normalized is None or normalized.keys is not raw_keys):
                    with suppress(Exception):
                        raw_keys.cleanup()
                if raw_delta is not None and (normalized is None or normalized.delta is not raw_delta):
                    with suppress(Exception):
                        raw_delta.cleanup()

    @staticmethod
    def _release_preserved_authority_leases(*artifacts: Any) -> tuple[str, ...]:
        """Release runtime sessions while retaining unknown-commit tables."""

        released: set[int] = set()
        failures: list[str] = []
        for value in artifacts:
            candidates = (value.delta, value.keys) if isinstance(value, NormalizedSnapshotStaging) else (value,)
            for artifact in candidates:
                if artifact is None or id(artifact) in released:
                    continue
                released.add(id(artifact))
                manager = getattr(artifact, "staging_manager", None)
                release = getattr(manager, "release_authority_lease", None)
                if callable(release):
                    try:
                        release(artifact)
                    except Exception as exc:
                        failures.append(
                            "staging authority lease release failed: "
                            f"artifact={getattr(artifact, 'table', '<unknown>')}; "
                            f"cause_type={type(exc).__name__}"
                        )
        return tuple(failures)

    def _finalize(
        self,
        load_config: Any,
        payload_schema: Any,
        envelope: IncrementalSnapshotEnvelope[Any, Any],
        delta: StagingTableArtifact,
        keys: StagingTableArtifact,
        policy: KeySnapshotReconciliationPolicy,
    ) -> LoadResult:
        return MssqlSnapshotTransaction(
            strategy=self._strategy,
            connector=self._connector,
            state=self._state,
            contract=self._contract,
            mutation=self._mutation,
            publication_head=self._publication_head,
            detection_time=self._detection_time,
            insert_absent=self._insert_absent,
            probe_commit=self._probe_commit,
        ).finalize(load_config, payload_schema, envelope, delta, keys, policy)

    def _insert_absent(
        self,
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
    ) -> int:
        return self._mutation.insert_absent(
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

    def _detection_time(self) -> datetime:
        rows = self._connector.get_records("SELECT SYSUTCDATETIME() AS snapshot_effective_at", as_dict=True)
        if not rows:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.clock_unavailable")
        value = rows[0]["snapshot_effective_at"]
        return (
            value.replace(tzinfo=timezone.utc)  # noqa: UP017 - mypy targets pre-3.11.
            if isinstance(value, datetime) and value.tzinfo is None
            else value
        )

    def _probe_commit(self, envelope, load_id):
        try:
            return self._state.probe_receipt(key=envelope.state_key, load_id=load_id)
        except Exception:
            return None


__all__ = [
    "MssqlCommitOutcomeUnknown",
    "MssqlSnapshotFinalizer",
    "_business_columns",
    "_changed_lineage_assignments",
]
