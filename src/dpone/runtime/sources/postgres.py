"""PostgreSQL источник данных."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.incremental_snapshot import KeySnapshotReconciliationPolicy
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.postgres_incremental_cursor import (
    PostgresIncrementalStrategy,
    assert_postgres_column_cursor_route_supported,
    resolve_postgres_incremental_strategy,
)
from dpone.runtime.internal_query_capability import InternalQueryCapabilityDecision
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.source_protocol import AbstractSource
from dpone.runtime.sources.strategies import SourceStrategy
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
    prepared_postgres_source_boundary,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.connectors import PostgresConnector
    from dpone.runtime.state import XMinStateStorage


class PostgresSource(AbstractSource):
    """Источник данных PostgreSQL.

    **Два режима инкрементальной загрузки:**

    1. **xmin tracking** (по умолчанию для incremental Postgres):
       - Использует системную колонку xmin PostgreSQL
       - Автоматически ловит INSERT и UPDATE
       - Можно включить явно через `source.options.incremental_strategy: xmin`
       - Для обратной совместимости также выбирается при отсутствии `incremental_column`

    2. **column cursor**:
       - Использует явную колонку (created_at, updated_at)
       - Получает MAX(column) и забирает WHERE column > MAX
       - Подходит только для маршрутов, где target-derived cursor допустим
       - Для MSSQL fail-closed: single-column MAX не является атомарной
         source boundary и теряет concurrent equal/late rows
       - Включается через `incremental_column` или явно через `incremental_strategy: column`
    """

    target_max_cursor_source_type = "postgres"

    def __init__(
        self,
        connector: PostgresConnector,
        state_storage: XMinStateStorage,
        logger,
        sink_connector: PostgresConnector | None = None,
        internal_query_capability: InternalQueryCapabilityDecision | None = None,
    ):
        from dpone.runtime.sources.strategies import PostgresFullExtractStrategy

        self.connector = connector
        self.sink_connector = sink_connector or connector
        self.state_storage = state_storage
        self.logger = logger
        self.internal_query_capability = internal_query_capability or InternalQueryCapabilityDecision.not_issued(
            source_dialect="postgres"
        )
        self._postgres_source_authority_verifier: Any | None = None
        self._postgres_mssql_source_schema_runtime: Any | None = None

        self._full_extract = PostgresFullExtractStrategy(
            connector=self.connector,
            logger=logger,
            internal_query_capability=self.internal_query_capability,
        )
        self._xmin_extract_instance: SourceStrategy | None = None
        self._incremental_extract_instance: SourceStrategy | None = None

        self._base_strategy_map: dict[LoadStrategy, SourceStrategy] = {
            LoadStrategy.FULL_REFRESH: self._full_extract,
            LoadStrategy.REPLACE: self._full_extract,
            LoadStrategy.PARTITION_REPLACE: self._full_extract,
            # Missing-row semantics require one complete source snapshot;
            # target strategies add and consume their own comparison/history
            # metadata after the immutable full extract.
            LoadStrategy.SNAPSHOT_DIFF: self._full_extract,
            LoadStrategy.SCD2: self._full_extract,
            # Backfill extraction is a full scan bounded by the chunk predicate
            # (options.source_custom_predicate injected by the orchestrator).
            LoadStrategy.BACKFILL: self._full_extract,
        }

    @property
    def _xmin_extract(self) -> SourceStrategy:
        if self._xmin_extract_instance is None:
            from dpone.runtime.sources.strategies import PostgresXMinExtractStrategy

            self._xmin_extract_instance = PostgresXMinExtractStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                state_storage=self.state_storage,
                logger=self.logger,
                internal_query_capability=self.internal_query_capability,
            )
            if self._postgres_source_authority_verifier is not None:
                self._xmin_extract_instance.bind_postgres_source_authority(self._postgres_source_authority_verifier)
        return self._xmin_extract_instance

    @property
    def _incremental_extract(self) -> SourceStrategy:
        if self._incremental_extract_instance is None:
            from dpone.runtime.sources.strategies.postgres.postgres_incremental_extract import (
                PostgresIncrementalExtractStrategy,
            )

            self._incremental_extract_instance = PostgresIncrementalExtractStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                logger=self.logger,
            )
            if self._postgres_source_authority_verifier is not None:
                self._incremental_extract_instance.bind_postgres_source_authority(
                    self._postgres_source_authority_verifier
                )
        return self._incremental_extract_instance

    def bind_internal_query_capability(self, decision: InternalQueryCapabilityDecision) -> None:
        """Accept only the post-hydration internal-query authority decision."""

        self.internal_query_capability = decision
        self._full_extract.bind_internal_query_capability(decision)
        if self._xmin_extract_instance is not None:
            binder = getattr(self._xmin_extract_instance, "bind_internal_query_capability", None)
            if callable(binder):
                binder(decision)

    def bind_postgres_source_authority(self, verifier: Any) -> None:
        """Bind one descriptor-selected verifier to every extraction strategy."""

        if verifier is None or not callable(getattr(verifier, "verify_snapshot", None)):
            raise ValueError("postgres_source_authority.verifier_required")
        current = self._postgres_source_authority_verifier
        if current is not None and current is not verifier:
            raise ValueError("postgres_source_authority.verifier_already_bound")
        self._postgres_source_authority_verifier = verifier
        self._full_extract.bind_postgres_source_authority(verifier)
        for strategy in (
            self._xmin_extract_instance,
            self._incremental_extract_instance,
        ):
            if strategy is not None:
                strategy.bind_postgres_source_authority(verifier)

    def bind_postgres_mssql_source_schema_runtime(self, runtime: Any) -> None:
        """Retain the capability validated by the runtime composition root.

        Exact bundle and verifier admission belongs to bootstrap composition;
        this injection seam preserves identity and prevents conflicting rebinding.
        """

        if not callable(getattr(runtime, "prepare_boundary", None)):
            raise ValueError("postgres_mssql_source_schema_runtime.prepare_boundary_required")
        current = self._postgres_mssql_source_schema_runtime
        if current is not None and current is not runtime:
            raise ValueError("postgres_mssql_source_schema_runtime.already_bound")
        self._postgres_mssql_source_schema_runtime = runtime

    def build_xmin_initial_handoff_source(self, state_storage: Any) -> Any:
        """Build a dedicated XMin authority while backfill uses generic state."""

        from dpone.runtime.sources.strategies import PostgresXMinExtractStrategy
        from dpone.runtime.sources.strategies.postgres.postgres_xmin_handoff_source import (
            PostgresXminHandoffSource,
        )

        strategy = PostgresXMinExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            state_storage=state_storage,
            logger=self.logger,
            internal_query_capability=self.internal_query_capability,
        )
        verifier = self._postgres_source_authority_verifier
        if verifier is None:
            raise RuntimeError("mssql_transaction.postgres_source_authority_verifier_required")
        strategy.bind_postgres_source_authority(verifier)
        return PostgresXminHandoffSource(strategy)

    def get_incremental_state(self, load_config: LoadConfig) -> Any | None:
        strategy = self._resolve_strategy(load_config)
        return strategy.get_state(load_config)

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> ExtractResult:
        strategy = self._resolve_strategy(load_config)
        return strategy.extract(load_config, last_state)

    def fetch_schema_projection(self, load_config: LoadConfig) -> Any:
        """Expose catalog-only PostgreSQL projection for pre-extract gates."""

        prepared = prepared_postgres_source_boundary(load_config)
        if prepared is not None:
            prepared.require_active(self.connector)
            return prepared.schema_projection
        strategy = self._resolve_strategy(load_config)
        fetch = getattr(strategy, "fetch_schema_projection", None)
        if not callable(fetch):
            raise RuntimeError("postgres_mssql.preflight.schema_projection_unavailable")
        return fetch(load_config)

    def prepare_mssql_source_boundary(self, load_config: LoadConfig) -> Any:
        """Open the strategy-owned post-admission RR authority boundary."""

        runtime = self._postgres_mssql_source_schema_runtime
        if runtime is not None:
            return runtime.prepare_boundary(
                connector=self.connector,
                lifecycle=self._full_extract._new_extraction_lifecycle(),
                load_config=load_config,
            )
        strategy = self._resolve_strategy(load_config)
        prepare = getattr(strategy, "prepare_mssql_source_boundary", None)
        if not callable(prepare):
            raise RuntimeError("postgres_prepared_source_boundary.strategy_unsupported")
        return prepare(load_config)

    def abort_mssql_source_boundary(self, load_config: LoadConfig) -> None:
        """Release a prepared RR that did not reach artifact ownership."""

        prepared = prepared_postgres_source_boundary(load_config)
        if prepared is not None:
            prepared.abort_if_active()

    def mssql_transaction_checkpoint_mode(
        self,
        load_config: LoadConfig,
    ) -> MssqlTransactionCheckpointMode:
        """Declare whether generic MSSQL commit can own this source checkpoint."""

        incremental_strategy = self._resolved_incremental_strategy(load_config)
        if incremental_strategy is PostgresIncrementalStrategy.XMIN:
            policy = KeySnapshotReconciliationPolicy.from_runtime(
                getattr(load_config, "options", None),
                legacy_enabled=bool(getattr(load_config, "reconciliation", False)),
            )
            if policy.key_snapshot_enabled:
                return MssqlTransactionCheckpointMode.SNAPSHOT_ENVELOPE_TARGET_ATOMIC
            return MssqlTransactionCheckpointMode.EXTERNAL_NONATOMIC
        if incremental_strategy is PostgresIncrementalStrategy.COLUMN:
            return MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE
        self._resolve_strategy(load_config)
        return MssqlTransactionCheckpointMode.STATELESS

    def mssql_transaction_source_physical_identity(self, load_config: LoadConfig) -> Any:
        """Return selected signed identity without pre-admission source I/O."""

        verifier = self._postgres_source_authority_verifier
        if verifier is None:
            raise RuntimeError("mssql_transaction.postgres_source_authority_verifier_required")
        return verifier.preflight(load_config)

    def _resolve_strategy(self, load_config: LoadConfig) -> SourceStrategy:
        """
        Выбирает стратегию извлечения.

        **Логика выбора:**
        1. `incremental_strategy: xmin` → PostgresXMinExtractStrategy
        2. `incremental_strategy: column` → PostgresIncrementalExtractStrategy
        3. legacy fallback: `incremental_column` → column cursor
        4. legacy fallback: no `incremental_column` → xmin for incremental strategies

        PostgreSQL→MSSQL column mode is rejected here, before connector I/O,
        until a typed composite source boundary can be committed atomically
        with the target receipt.
        """
        options = getattr(load_config, "options", {}) or {}
        incremental_strategy = self._resolved_incremental_strategy(load_config)

        if incremental_strategy is PostgresIncrementalStrategy.XMIN:
            return self._xmin_extract

        if incremental_strategy is PostgresIncrementalStrategy.COLUMN:
            assert_postgres_column_cursor_route_supported(
                configured_sink=options.get("sink_type") or options.get("target_type"),
                sink_connector=self.sink_connector,
            )
            return self._incremental_extract

        # Стандартный маппинг
        strategy = self._base_strategy_map.get(load_config.load_strategy)
        if strategy is None:
            supported = sorted(
                {
                    strategy.value
                    for strategy in (
                        *self._base_strategy_map.keys(),
                        LoadStrategy.INCREMENTAL_MERGE,
                        LoadStrategy.INCREMENTAL_APPEND,
                    )
                }
            )
            raise ValueError(
                f"Неизвестная стратегия load_strategy: {load_config.load_strategy}. "
                f"Поддерживаемые: {', '.join(supported)}"
            )
        return strategy

    def _resolved_incremental_strategy(
        self,
        load_config: LoadConfig,
    ) -> PostgresIncrementalStrategy | None:
        """Resolve cursor selection without consulting source or target connectors."""

        options = getattr(load_config, "options", {}) or {}
        incremental_column = options.get("incremental_column")
        resolved = resolve_postgres_incremental_strategy(
            options.get("incremental_strategy"),
            incremental_column=incremental_column,
        )
        if load_config.load_strategy not in (
            LoadStrategy.INCREMENTAL_MERGE,
            LoadStrategy.INCREMENTAL_APPEND,
        ):
            return None
        if resolved is PostgresIncrementalStrategy.XMIN and incremental_column:
            raise ValueError(
                "PostgreSQL XMin incremental strategy conflicts with source.options.incremental_column. "
                "Use incremental_strategy: xmin without incremental_column, or incremental_strategy: column with incremental_column."
            )
        if resolved is PostgresIncrementalStrategy.COLUMN and not incremental_column:
            raise ValueError(
                "PostgreSQL column incremental strategy requires source.options.incremental_column. "
                "Use incremental_strategy: xmin for XMin, or set incremental_column for column cursor extraction."
            )
        return resolved
