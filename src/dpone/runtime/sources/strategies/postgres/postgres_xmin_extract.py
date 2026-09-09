"""Инкрементальная стратегия извлечения по xmin."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.state import XMinStateStorage


from dpone.ports.source_state_storage import SourceStateKey
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome, ExtractionClock
from dpone.runtime.file_artifacts import BatchedFileExportArtifact
from dpone.runtime.incremental_snapshot import KeySnapshotReconciliationPolicy
from dpone.runtime.internal_query_capability import InternalQueryCapabilityDecision
from dpone.runtime.postgres_xmin_execution import postgres_xmin_execution_policy
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.postgres.postgres_base import PostgresBaseStrategy
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_extract import (
    PostgresSnapshotEnvelopeExtractor,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    add_redacted_error_code_note,
    add_redacted_secondary_note,
    cleanup_preserving_primary,
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_retry import (
    PostgresSnapshotRetryPolicy,
    PostgresSnapshotRetryRunner,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_handoff_admission import (
    require_committed_xmin_handoff,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_legacy_artifact import (
    PostgresXMinLegacyArtifactExtractor,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_target_probe import PostgresXminTargetProbe
from dpone.runtime.state import XMinState
from dpone.runtime.state.mssql_route_preflight import resolve_atomic_mssql_target
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.runtime.xmin.manager import XMinStateManager
from dpone.type_system.source_sink.provenance import SourceRelationDialect


class PostgresXMinExtractStrategy(PostgresBaseStrategy):
    mssql_transaction_checkpoint_mode = "external_nonatomic"

    def __init__(
        self,
        connector,
        state_storage: XMinStateStorage,
        logger,
        sink_connector=None,
        internal_query_capability: InternalQueryCapabilityDecision | None = None,
        extraction_clock: ExtractionClock | None = None,
        snapshot_retry_policy: PostgresSnapshotRetryPolicy | None = None,
        snapshot_retry_sleeper: Callable[[float], None] | None = None,
        snapshot_retry_random: Callable[[], float] | None = None,
    ):
        super().__init__(
            connector,
            logger,
            internal_query_capability=internal_query_capability,
            extraction_clock=extraction_clock,
        )
        self.sink_connector = sink_connector or connector
        self.state_storage = state_storage
        self.xmin_manager = XMinStateManager(connector)
        self._snapshot_extractor = PostgresSnapshotEnvelopeExtractor(self)
        self._snapshot_retry_runner = PostgresSnapshotRetryRunner(
            policy=snapshot_retry_policy,
            sleeper=snapshot_retry_sleeper,
            random_unit=snapshot_retry_random,
            secondary_failure_recorder=add_redacted_secondary_note,
        )
        self._legacy_artifact_extractor = PostgresXMinLegacyArtifactExtractor(self)
        self._target_probe = PostgresXminTargetProbe(
            connector=self.sink_connector,
            source_connector=self.connector,
            logger=self.logger,
        )
        self._loaded_state_key: SourceStateKey | None = None
        self._physical_target_binding: tuple[str, str, str] | None = None
        self._physical_target_identity: bytes | None = None

    def get_state(self, load_config) -> XMinState | None:
        policy = KeySnapshotReconciliationPolicy.from_runtime(
            getattr(load_config, "options", None),
            legacy_enabled=bool(getattr(load_config, "reconciliation", False)),
        )
        if policy.key_snapshot_enabled:
            self._preflight_atomic_route(load_config)
            loader = getattr(self.state_storage, "load_state_by_key", None)
            if not callable(loader):
                raise ValueError("key_snapshot reconciliation requires collision-safe transactional state storage")
            projection = self._verified_schema_projection(load_config)
            schema = list(projection.projected_schema)
            self._loaded_state_key = self._snapshot_extractor.state_key(
                load_config,
                schema,
                relation_schema=projection.relation_schema,
                relation_metadata=projection.relation_metadata,
            )
            state = loader(self._loaded_state_key)
            return require_committed_xmin_handoff(
                state_storage=self.state_storage,
                state_key=self._loaded_state_key,
                state=state,
                policy=postgres_xmin_execution_policy(getattr(load_config, "options", None)),
            )
        return self.state_storage.load_state(
            load_config.source_schema,
            load_config.source_table,
        )

    def extract(self, load_config, last_state: XMinState | None) -> ExtractResult:
        policy = KeySnapshotReconciliationPolicy.from_runtime(
            getattr(load_config, "options", None),
            legacy_enabled=bool(getattr(load_config, "reconciliation", False)),
        )
        if policy.key_snapshot_enabled:
            self._preflight_atomic_route(load_config)
            previous_state = last_state or self.get_state(load_config)
            repair_full_baseline = self._repair_full_baseline(load_config, previous_state)
            target_exists = self._target_exists(load_config)
            if target_exists is None:
                raise ValueError("postgres_xmin_target_existence_unverified")
            if target_exists and previous_state is None:
                target_empty = self._target_is_empty(load_config)
                if target_empty is None:
                    raise ValueError("postgres_xmin_target_empty_state_unverified")
                if not target_empty and not repair_full_baseline:
                    raise ValueError("postgres_xmin_state_missing_for_existing_target_full_baseline_required")
            return self._snapshot_retry_runner.run(
                lambda: self._snapshot_extractor.extract(
                    load_config,
                    previous_state,
                    force_full_refresh=target_exists is False or repair_full_baseline,
                    repair_full_baseline_previewed=repair_full_baseline,
                    loaded_state_key=self._loaded_state_key,
                ),
                connector=self.connector,
                logger=self.logger,
            )

        previous_state = last_state or self.get_state(load_config)

        EPOCH_MOD = 2**32

        if previous_state:
            prev_epoch = previous_state.xmin_value // EPOCH_MOD
            prev_low = previous_state.xmin_value % EPOCH_MOD
        else:
            prev_epoch = 0
            prev_low = 0

        # ⚠️ ВАЖНО: Логируем состояние xmin ДО начала извлечения
        if previous_state:
            self.logger.log_xmin_state_info(
                "Загружено предыдущее состояние",
                {
                    "Epoch": prev_epoch,
                    "Low32": prev_low,
                    "Full": previous_state.xmin_value,
                    "Timestamp": str(previous_state.timestamp),
                    "Is Initial": previous_state.is_initial,
                },
            )
        else:
            self.logger.log_xmin_state_info(
                "Предыдущее состояние отсутствует — первый запуск",
                {"Source": f"{load_config.source_schema}.{load_config.source_table}"},
            )

        force_full_refresh = False
        target_exists = self._target_exists(load_config)
        if target_exists is False:
            force_full_refresh = True
            self.logger.log_xmin_state_info(
                "Целевая таблица отсутствует — выполняем полный снимок",
                {
                    "Target Schema": load_config.target_schema,
                    "Target Table": load_config.target_table,
                },
            )

        lifecycle = self._new_extraction_lifecycle()
        snapshot_lease = self._begin_repeatable_read_snapshot(lifecycle)
        try:
            snapshot_lease.require_for(self.connector)
            self._verify_postgres_source_authority(
                snapshot_lease,
                load_config,
            )
            projection = self.fetch_schema_projection(load_config)
            schema = list(projection.projected_schema)
            relation_schema = list(projection.relation_schema)
            snapshot_xmin = self.xmin_manager.get_snapshot_xmin_anchor()
            safe_state = self.xmin_manager.calculate_safe_xmin(snapshot_xmin, previous_state)
        except BaseException as primary:
            rollback_preserving_primary(self.connector, primary)
            raise

        # ВАЖНО: Логируем текущее snapshot_xmin
        curr_epoch = snapshot_xmin // EPOCH_MOD
        curr_low = snapshot_xmin % EPOCH_MOD
        self.logger.log_xmin_state_info(
            "Получено текущее snapshot_xmin",
            {
                "Epoch": curr_epoch,
                "Low32": curr_low,
                "Full": snapshot_xmin,
            },
        )

        artifact = None
        try:
            artifact, schema, relation_schema = self._legacy_artifact_extractor.extract(
                load_config=load_config,
                schema=schema,
                relation_schema=relation_schema,
                previous_state=previous_state,
                snapshot_xmin=snapshot_xmin,
                prev_low=prev_low,
                curr_low=curr_low,
                safe_state=safe_state,
                force_full_refresh=force_full_refresh,
                snapshot_lease=snapshot_lease,
            )
            if not isinstance(artifact, (StreamingRowsArtifact, BatchedFileExportArtifact)):
                if getattr(artifact, "extraction_lifecycle", None) is lifecycle:
                    lifecycle.complete()
                self.connector.commit_transaction()
        except BaseException as primary:
            if isinstance(artifact, (StreamingRowsArtifact, BatchedFileExportArtifact)):
                receipt = artifact.terminate(ArtifactTerminalOutcome.ABORT)
                if not receipt.cleanup_succeeded:
                    add_redacted_error_code_note(
                        primary,
                        "postgres_snapshot.rollback_failed",
                        receipt.cleanup_error_code,
                    )
            else:
                rollback_preserving_primary(self.connector, primary)
            if artifact is not None and not isinstance(artifact, (StreamingRowsArtifact, BatchedFileExportArtifact)):
                cleanup_preserving_primary(
                    artifact.cleanup,
                    primary,
                    code="postgres_snapshot.artifact_cleanup_failed",
                )
            raise

        checkpoint_state = XMinState(
            xmin_value=snapshot_xmin,
            timestamp=safe_state.timestamp,
            is_initial=safe_state.is_initial,
            wraparound_detected=safe_state.wraparound_detected,
            frozen_xid=safe_state.frozen_xid,
        )

        return ExtractResult(
            artifact=artifact,
            schema=schema,
            relation_schema=relation_schema,
            relation_metadata=projection.relation_metadata,
            relation_dialect=SourceRelationDialect.POSTGRES,
            target_projection=projection.target_projection,
            state=checkpoint_state,
            force_full_refresh=force_full_refresh,
            extraction_lifecycle=getattr(artifact, "extraction_lifecycle", None),
        )

    def save_state(self, load_config, state: XMinState) -> None:
        self.state_storage.save_state(
            load_config.source_schema,
            load_config.source_table,
            state,
        )

    def _repair_full_baseline(self, load_config: Any, previous_state: XMinState | None) -> bool:
        authority_ref = getattr(load_config, "repair_authority_ref", None)
        if not authority_ref:
            return False
        preview = getattr(self.state_storage, "preview_repair_authority", None)
        if not callable(preview):
            raise ValueError("repair_authority.state_service_required")
        if self._loaded_state_key is None:
            projection = self._verified_schema_projection(load_config)
            schema = list(projection.projected_schema)
            self._loaded_state_key = self._snapshot_extractor.state_key(
                load_config,
                schema,
                relation_schema=projection.relation_schema,
                relation_metadata=projection.relation_metadata,
            )
        authority = preview(
            authority_ref=authority_ref,
            key=self._loaded_state_key,
            expected_checkpoint=previous_state,
            scope_hash=self._loaded_state_key.scope_hash,
        )
        return bool(authority.allow.full_baseline)

    def _verified_schema_projection(self, load_config: Any) -> Any:
        """Read pre-state schema only after signed authority on its own RR."""

        return self._snapshot_retry_runner.run(
            lambda: self._verified_schema_projection_once(load_config),
            connector=self.connector,
            logger=self.logger,
        )

    def _verified_schema_projection_once(self, load_config: Any) -> Any:
        """Perform one source-only schema projection attempt."""

        lifecycle = self._new_extraction_lifecycle()
        snapshot_lease = self._begin_repeatable_read_snapshot(lifecycle)
        try:
            self._verify_postgres_source_authority(
                snapshot_lease,
                load_config,
            )
            projection = self.fetch_schema_projection(load_config)
        except BaseException as primary:
            rollback_preserving_primary(self.connector, primary)
            raise
        self.connector.rollback()
        return projection

    def _preflight_atomic_route(self, load_config: Any) -> None:
        binding = self._target_binding(load_config)
        if self._physical_target_binding == binding and self._physical_target_identity is not None:
            return
        resolved = resolve_atomic_mssql_target(
            self.sink_connector,
            self.state_storage,
            database=binding[0],
            schema=binding[1],
            table=binding[2],
        )
        # All downstream DDL and DML use registry spelling.  This matters for
        # an absent CI target and prevents a raw manifest alias from becoming
        # a second authority namespace.
        load_config.target_database = resolved.database_name
        load_config.target_schema = resolved.schema_name
        load_config.target_table = resolved.table_name
        self._physical_target_binding = self._target_binding(load_config)
        self._physical_target_identity = resolved.digest

    def physical_target_identity(self, load_config: Any) -> bytes:
        """Return only the identity proven for these exact runtime coordinates."""

        if self._physical_target_binding != self._target_binding(load_config):
            raise ValueError("mssql_physical_target_identity_preflight_required")
        identity = self._physical_target_identity
        if not isinstance(identity, bytes) or len(identity) != 32:
            raise ValueError("mssql_physical_target_identity_preflight_required")
        return identity

    @staticmethod
    def _target_binding(load_config: Any) -> tuple[str, str, str]:
        binding = tuple(
            str(value or "").strip()
            for value in (
                getattr(load_config, "target_database", None),
                getattr(load_config, "target_schema", None),
                getattr(load_config, "target_table", None),
            )
        )
        if any(not value for value in binding):
            raise ValueError("mssql_physical_target_coordinates_incomplete")
        return binding  # type: ignore[return-value]

    def _target_exists(self, load_config) -> bool | None:
        return self._target_probe.exists(load_config)

    def _target_is_empty(self, load_config) -> bool | None:
        return self._target_probe.is_empty(load_config)

    @staticmethod
    def _schema_with_meta_xmin(schema: list[tuple[str, str]]) -> list[tuple[str, str]]:
        if any(column == "__dpone__xmin" for column, _ in schema):
            return schema
        return [*schema, ("__dpone__xmin", "bigint")]
