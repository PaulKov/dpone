"""Same-snapshot XMin delta and complete-key extraction."""

from __future__ import annotations

import time
from typing import Any

from dpone.ports.source_state_storage import SourceStateKey
from dpone.runtime.incremental_snapshot import (
    DELTA_HASH_COLUMN,
    KEY_HASH_COLUMN,
    IncrementalSnapshotEnvelope,
    schema_identity_hash,
    snapshot_scope_hash,
)
from dpone.runtime.postgres_xmin_execution import (
    checkpoint_process_identity,
    postgres_xmin_execution_policy,
)
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.postgres.postgres_baseline_snapshot_artifacts import (
    build_baseline_snapshot_files,
)
from dpone.runtime.sources.strategies.postgres.postgres_delta_snapshot import (
    PostgresDeltaSnapshotFileArtifact,
)
from dpone.runtime.sources.strategies.postgres.postgres_key_snapshot import (
    PostgresKeySnapshotFileArtifact,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    cleanup_preserving_primary,
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_key_schema import (
    project_snapshot_key_schema,
    resolve_snapshot_unique_key,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_policy import (
    mssql_snapshot_file_config,
    require_bounded_replay_amplification,
    snapshot_key_query,
    snapshot_source_predicate,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_freeze_horizon import (
    require_checkpoint_above_freeze_horizon,
)
from dpone.runtime.state.xmin_storage import XMinState
from dpone.runtime.support.mssql_snapshot_projection import (
    require_indexable_key_types,
    resolved_business_nullability,
    resolved_target_type,
    resolved_text_key_columns,
)
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect


class PostgresSnapshotEnvelopeExtractor:
    """Materialize both source artifacts before releasing one read-only snapshot."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy

    def state_key(
        self,
        load_config: Any,
        schema: list[tuple[str, str]],
        *,
        relation_schema: list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
        relation_metadata: tuple[SourceColumnProvenance, ...] = (),
    ) -> SourceStateKey:
        keys = resolve_snapshot_unique_key(load_config)
        require_indexable_key_types(schema, load_config, keys)
        source_database = str(
            getattr(load_config, "source_database", None) or getattr(self._strategy.connector, "database", "")
        )
        target_database = str(
            getattr(load_config, "target_database", None) or getattr(self._strategy.sink_connector, "database", "")
        )
        identity = (getattr(load_config, "options", {}) or {}).get("state_identity")
        identity_values = identity if isinstance(identity, dict) else {}
        environment = str(identity_values.get("environment") or "").strip()
        authored_process = str(identity_values.get("process") or "").strip()
        if not environment or not authored_process:
            raise ValueError("postgres_xmin_state_identity_runtime_context_required")
        process = checkpoint_process_identity(
            postgres_xmin_execution_policy(getattr(load_config, "options", None)),
            authored_process=authored_process,
        )
        predicate = snapshot_source_predicate(load_config)
        nullability = resolved_business_nullability(load_config, schema, keys)
        scope_hash = snapshot_scope_hash(
            source_database=source_database,
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            key_columns=keys,
            predicate=predicate,
        )
        return SourceStateKey(
            environment=environment,
            process=process,
            source_connection=str(load_config.source_conn_id),
            source_database=source_database,
            source_schema=str(load_config.source_schema),
            source_table=str(load_config.source_table),
            target_database=target_database,
            target_schema=str(load_config.target_schema),
            target_table=str(load_config.target_table),
            target_identity=self._strategy.physical_target_identity(load_config),
            unique_key=keys,
            schema_hash=schema_identity_hash(
                [
                    *[(f"source:{column}", dtype) for column, dtype in (relation_schema or schema)],
                    *[(f"source_metadata:{column.name}", column.identity_token) for column in relation_metadata],
                    *schema,
                    *[
                        (f"target:{column}", resolved_target_type(load_config, column, dtype))
                        for column, dtype in schema
                    ],
                    *[(f"target_nullable:{column}", str(nullability[column]).lower()) for column, _dtype in schema],
                ]
            ),
            scope_hash=scope_hash,
        )

    def extract(
        self,
        load_config: Any,
        previous_state: XMinState | None,
        *,
        force_full_refresh: bool,
        repair_full_baseline_previewed: bool,
        loaded_state_key: SourceStateKey | None,
    ) -> ExtractResult:
        if repair_full_baseline_previewed and not force_full_refresh:
            raise ValueError("postgres_xmin_repair_baseline_invariant_violated")
        effective = mssql_snapshot_file_config(load_config)
        delta_artifact = None
        raw_delta_artifact = None
        key_artifact = None
        envelope = None
        snapshot_completion = None
        extraction_lifecycle = self._strategy._new_extraction_lifecycle()
        try:
            snapshot_lease = self._strategy._begin_repeatable_read_snapshot(extraction_lifecycle)
            snapshot_completion = self._strategy._repeatable_read_snapshot_completion(extraction_lifecycle)
            self._strategy._verify_postgres_source_authority(
                snapshot_lease,
                effective,
            )
            projection = self._strategy.fetch_schema_projection(effective)
            schema = list(projection.projected_schema)
            relation_schema = list(projection.relation_schema)
            state_key = self.state_key(
                effective,
                schema,
                relation_schema=relation_schema,
                relation_metadata=projection.relation_metadata,
            )
            if previous_state is not None and loaded_state_key is not None and loaded_state_key != state_key:
                raise ValueError("postgres_xmin_state_identity_changed_full_baseline_required")

            snapshot_token = snapshot_lease.snapshot_token_digest
            extraction_horizon = snapshot_lease.require_visible_horizon()
            upper = self._strategy.xmin_manager.get_snapshot_xmin_anchor()
            require_bounded_replay_amplification(effective, upper, extraction_horizon)
            # A one-shot, authority-approved full repair deliberately replaces
            # the old XMin window.  Calculating safety against that stale
            # checkpoint would reject the very wrap/freeze condition the full
            # snapshot is authorized to repair.  We still derive the new
            # checkpoint from this fresh RR snapshot and keep the old state in
            # the envelope so the target-side CAS remains exact.
            repair_replaces_previous_window = repair_full_baseline_previewed and previous_state is not None
            safety_previous = None if repair_replaces_previous_window else previous_state
            safe_state = self._strategy.xmin_manager.calculate_safe_xmin(upper, safety_previous)
            if (
                previous_state is not None
                and not repair_replaces_previous_window
                and (safe_state.wraparound_detected or safe_state.is_initial)
            ):
                raise ValueError("postgres_xmin_wraparound_full_baseline_required")
            frozen_horizon = None
            if previous_state is not None and not repair_replaces_previous_window:
                frozen_horizon = self._require_checkpoint_above_freeze_horizon(effective, previous_state)
            baseline = force_full_refresh or self._strategy.xmin_manager.should_perform_full_refresh(safe_state)
            candidate = XMinState(
                xmin_value=upper,
                timestamp=safe_state.timestamp,
                is_initial=safe_state.is_initial if not repair_replaces_previous_window else False,
                wraparound_detected=(safe_state.wraparound_detected if not repair_replaces_previous_window else False),
                frozen_xid=frozen_horizon,
            )
            if baseline:
                delta_query = self._strategy.format_select_query(
                    effective.source_schema,
                    effective.source_table,
                    [column for column, _dtype in schema],
                    snapshot_source_predicate(effective),
                )
                delta_schema = schema
                delta_relation_schema = relation_schema
            else:
                delta_query = self._strategy.xmin_manager.build_incremental_query(
                    effective.source_schema,
                    effective.source_table,
                    previous_state,
                    extraction_horizon,
                    columns=None,
                    custom_predicate=snapshot_source_predicate(effective),
                )
                delta_schema = self._strategy._schema_with_meta_xmin(schema)
                delta_relation_schema = self._strategy._schema_with_meta_xmin(relation_schema)

            raw_delta_artifact = self._strategy._export_to_file_whole(
                delta_query,
                delta_schema,
                effective,
                snapshot_lease=snapshot_lease,
                relation_schema=delta_relation_schema,
                after_copy=snapshot_completion.complete if baseline else None,
            )
            source_key_schema = project_snapshot_key_schema(schema, state_key.unique_key)
            text_keys = resolved_text_key_columns(schema, effective, state_key.unique_key)
            if baseline:
                # The complete relation is now frozen in one verified local
                # artifact.  Key projection below reads only that immutable
                # file, so keeping the source RR snapshot open would add no
                # consistency guarantee and can make a hot standby cancel an
                # otherwise-complete export during recovery cleanup.
                projection_started = time.perf_counter()
                raw_receipt = raw_delta_artifact.integrity_receipt
                delta_file, key_file = build_baseline_snapshot_files(
                    raw_delta_artifact,
                    delta_schema=delta_schema,
                    key_columns=state_key.unique_key,
                    delta_hash_column=DELTA_HASH_COLUMN,
                    key_hash_column=KEY_HASH_COLUMN,
                )
                raw_delta_artifact = None
                try:
                    delta_artifact = PostgresDeltaSnapshotFileArtifact.from_hashed_artifact(
                        delta_file,
                        snapshot_token=snapshot_token,
                        scope_hash=state_key.scope_hash,
                        columns=[column for column, _dtype in delta_schema],
                    )
                    key_artifact = PostgresKeySnapshotFileArtifact.from_hashed_artifact(
                        key_file,
                        snapshot_token=snapshot_token,
                        scope_hash=state_key.scope_hash,
                        key_columns=state_key.unique_key,
                    )
                except BaseException:
                    delta_file.cleanup()
                    key_file.cleanup()
                    raise
                projection_elapsed = time.perf_counter() - projection_started
                self._strategy.logger.log_etl_progress(
                    "POSTGRES_SNAPSHOT_PROJECTION_COMPLETE",
                    {
                        "Source Bytes": raw_receipt.size_bytes if raw_receipt is not None else None,
                        "Projected Bytes": delta_artifact.size_bytes + key_artifact.size_bytes,
                        "Rows": delta_artifact.receipt.row_count,
                        "Duration": f"{projection_elapsed:.1f}s",
                    },
                )
            else:
                key_query = snapshot_key_query(effective, state_key.unique_key)
                key_file = self._strategy._export_key_snapshot_file(
                    key_query,
                    effective,
                    snapshot_lease=snapshot_lease,
                )
                key_artifact = PostgresKeySnapshotFileArtifact(
                    key_file,
                    snapshot_token=snapshot_token,
                    scope_hash=state_key.scope_hash,
                    key_columns=state_key.unique_key,
                    text_key_columns=text_keys,
                )
                delta_artifact = PostgresDeltaSnapshotFileArtifact(
                    raw_delta_artifact,
                    snapshot_token=snapshot_token,
                    scope_hash=state_key.scope_hash,
                    columns=[column for column, _dtype in delta_schema],
                )
                raw_delta_artifact = None
            envelope_delta_schema = [*delta_schema, (DELTA_HASH_COLUMN, "char(64)")]
            key_schema = [*source_key_schema, (KEY_HASH_COLUMN, "varchar(64)")]
            envelope = IncrementalSnapshotEnvelope(
                delta_artifact=delta_artifact,
                key_artifact=key_artifact,
                delta_schema=tuple(envelope_delta_schema),
                key_schema=tuple(key_schema),
                previous_checkpoint=previous_state,
                candidate_checkpoint=candidate,
                snapshot_token=snapshot_token,
                scope_hash=state_key.scope_hash,
                state_key=state_key,
                baseline=baseline,
                visible_horizon=extraction_horizon,
            )
            if snapshot_completion.active:
                snapshot_completion.complete()
        except BaseException as primary:
            if snapshot_completion is not None and snapshot_completion.active:
                rollback_preserving_primary(self._strategy.connector, primary)
            if delta_artifact is not None:
                cleanup_preserving_primary(
                    delta_artifact.cleanup,
                    primary,
                    code="postgres_snapshot.delta_cleanup_failed",
                )
            elif raw_delta_artifact is not None:
                cleanup_preserving_primary(
                    raw_delta_artifact.cleanup,
                    primary,
                    code="postgres_snapshot.raw_delta_cleanup_failed",
                )
            if key_artifact is not None:
                cleanup_preserving_primary(
                    key_artifact.cleanup,
                    primary,
                    code="postgres_snapshot.key_cleanup_failed",
                )
            raise

        if envelope is None:  # pragma: no cover - the try block either assigns or raises
            raise RuntimeError("postgres_xmin_snapshot_envelope_unavailable")
        return ExtractResult(
            artifact=envelope,
            schema=envelope_delta_schema,
            relation_schema=[*delta_relation_schema, (DELTA_HASH_COLUMN, "char(64)")],
            relation_metadata=projection.relation_metadata,
            relation_dialect=SourceRelationDialect.POSTGRES,
            target_projection=projection.target_projection,
            state=candidate,
            # A baseline is still finalized through incremental_merge so target
            # DML and the first checkpoint receipt share one MSSQL transaction.
            force_full_refresh=False,
            snapshot_envelope=envelope,
            extraction_lifecycle=extraction_lifecycle,
        )

    def _require_checkpoint_above_freeze_horizon(self, load_config: Any, previous_state: XMinState) -> int:
        try:
            return require_checkpoint_above_freeze_horizon(
                self._strategy.connector,
                load_config,
                checkpoint_xmin=previous_state.xmin_value,
            )
        except ValueError as exc:
            if str(exc) == "postgres_xmin_handoff.anchor_too_old":
                raise ValueError("postgres_xmin_frozen_checkpoint_full_baseline_required") from exc
            raise


__all__ = ["PostgresSnapshotEnvelopeExtractor"]
