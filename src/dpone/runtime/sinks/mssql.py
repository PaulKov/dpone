"""SQL Server sink with bcp-backed staging."""

from __future__ import annotations

from collections.abc import Sequence
from importlib import import_module
from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.manifest.mssql_native_policy import native_requested, validate_native_config
from dpone.readiness.physical_apply import DdlExecutionRequest
from dpone.readiness.schema_evolution import SchemaPlan
from dpone.runtime.mssql_spool_route import requires_mssql_character_spool_preflight
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.mssql_transaction_requirement import (
    MSSQL_GENERIC_TRANSACTION_CAPABILITY,
    require_generic_transaction_state,
)
from dpone.runtime.sinks.sink_protocol import AbstractSink
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sinks.strategies.backfill import BackfillStrategy
from dpone.runtime.sinks.strategies.base import SinkStrategy
from dpone.runtime.sinks.strategies.mssql import (
    MSSQLFullRefreshStrategy,
    MSSQLIncrementAppendStrategy,
    MSSQLIncrementMergeStrategy,
    MSSQLPartitionReplaceStrategy,
    MSSQLReplaceStrategy,
    MSSQLSCD2Strategy,
    MSSQLSnapshotDiffStrategy,
    MSSQLStrategyBase,
)
from dpone.runtime.storage_policy import RuntimeStoragePolicy, StoragePreflightService
from dpone.runtime.support.mssql_object_name import MSSQLObjectName

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
    from dpone.ports.mssql_connector import MSSQLConnectorPort


class MSSQLSink(AbstractSink):
    """SQL Server sink supporting the core dpone load strategies."""

    def __init__(
        self,
        connector: MSSQLConnectorPort,
        state_storage: Any = None,
        logger: ETLLogger | None = None,
        transaction_finalizer_factory: Any | None = None,
        staging_consumer_factory: Any | None = None,
        runtime_storage_policy: RuntimeStoragePolicy | None = None,
        storage_preflight_service: StoragePreflightService | None = None,
        native_staged_load_service: Any | None = None,
        native_staged_load_service_factory: Any | None = None,
    ):
        if native_staged_load_service is not None and native_staged_load_service_factory is not None:
            raise ValueError("mssql_native.staging_composition_ambiguous")
        self._native_staged_load = native_staged_load_service
        self.connector = connector
        self.state_storage = state_storage
        self.logger = logger or etl_logger
        self.acceptance_metric_probe = _default_acceptance_metric_probe(connector)
        self.staging_manager = MSSQLStagingManager(
            connector,
            self.logger,
            database_authority=state_storage,
            storage_policy=runtime_storage_policy,
            storage_preflight_service=storage_preflight_service,
        )
        if state_storage is not None and callable(getattr(state_storage, "bind_transaction_connector", None)):
            state_storage.bind_transaction_connector(connector)
        self._strategy_map: dict[LoadStrategy, SinkStrategy] = {
            LoadStrategy.FULL_REFRESH: MSSQLFullRefreshStrategy(
                connector,
                self.logger,
                self.staging_manager,
                state_storage=state_storage,
                transaction_finalizer_factory=transaction_finalizer_factory,
                staging_consumer_factory=staging_consumer_factory,
            ),
            LoadStrategy.INCREMENTAL_APPEND: MSSQLIncrementAppendStrategy(
                connector,
                self.logger,
                self.staging_manager,
                state_storage=state_storage,
                transaction_finalizer_factory=transaction_finalizer_factory,
                staging_consumer_factory=staging_consumer_factory,
            ),
            LoadStrategy.INCREMENTAL_MERGE: MSSQLIncrementMergeStrategy(
                connector,
                self.logger,
                self.staging_manager,
                state_storage=state_storage,
                transaction_finalizer_factory=transaction_finalizer_factory,
                staging_consumer_factory=staging_consumer_factory,
            ),
            LoadStrategy.REPLACE: MSSQLReplaceStrategy(
                connector,
                self.logger,
                self.staging_manager,
                state_storage=state_storage,
                transaction_finalizer_factory=transaction_finalizer_factory,
                staging_consumer_factory=staging_consumer_factory,
            ),
            LoadStrategy.PARTITION_REPLACE: MSSQLPartitionReplaceStrategy(
                connector,
                self.logger,
                self.staging_manager,
                state_storage=state_storage,
                transaction_finalizer_factory=transaction_finalizer_factory,
                staging_consumer_factory=staging_consumer_factory,
            ),
            LoadStrategy.SNAPSHOT_DIFF: MSSQLSnapshotDiffStrategy(
                connector,
                self.logger,
                self.staging_manager,
                state_storage=state_storage,
                transaction_finalizer_factory=transaction_finalizer_factory,
                staging_consumer_factory=staging_consumer_factory,
            ),
            LoadStrategy.SCD2: MSSQLSCD2Strategy(
                connector,
                self.logger,
                self.staging_manager,
                state_storage=state_storage,
                transaction_finalizer_factory=transaction_finalizer_factory,
                staging_consumer_factory=staging_consumer_factory,
            ),
        }
        self._strategy_map[LoadStrategy.BACKFILL] = BackfillStrategy(self._strategy_map)
        if native_staged_load_service_factory is not None:
            self._native_staged_load = native_staged_load_service_factory(self)

    def load(self, load_config: LoadConfig, payload: LoadPayload) -> LoadResult:
        if native_requested(load_config):
            return self._native_service(load_config).load(load_config, payload)
        normalize_mssql_load_strategy(load_config)
        require_generic_transaction_state(load_config, self.state_storage)
        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            raise ValueError(f"Unsupported MSSQL load strategy: {load_config.load_strategy.value}")
        return strategy.load(load_config, payload)

    def preflight_before_extract(self, *, load_config: LoadConfig, load_record: Any | None = None) -> None:
        """Validate the strategy×physical contract before source row I/O."""

        del load_record
        if native_requested(load_config):
            self._native_service(load_config)
            require_generic_transaction_state(load_config, self.state_storage)
            return
        _require_target_acceptance_database(load_config)
        normalize_mssql_load_strategy(load_config)
        require_generic_transaction_state(load_config, self.state_storage)
        if requires_mssql_character_spool_preflight(load_config):
            self.staging_manager.preflight_storage()

    def supports_staged_load_for(self, load_config: LoadConfig) -> bool:
        """Admit only an explicitly configured and composed native lifecycle."""
        if not native_requested(load_config):
            return False
        self._native_service(load_config)
        return True

    def _native_service(self, load_config: LoadConfig) -> Any:
        validate_native_config(load_config)
        if self._native_staged_load is None:
            raise ValueError("mssql_native.staging_composition_required")
        return self._native_staged_load

    def stage_payload(self, load_config: LoadConfig, payload: LoadPayload) -> Any:
        return self._native_service(load_config).stage(load_config, payload)

    def finalize_staged_load(self, load_config: LoadConfig, handle: Any) -> LoadResult:
        return self._native_service(load_config).finalize(load_config, handle)

    def abort_staged_load(self, handle: Any) -> None:
        if self._native_staged_load is None:
            raise ValueError("mssql_native.staging_composition_required")
        self._native_staged_load.abort(handle)

    def cleanup_staged_load(self, handle: Any) -> None:
        if self._native_staged_load is None:
            raise ValueError("mssql_native.staging_composition_required")
        self._native_staged_load.cleanup(handle)

    def mssql_transaction_governance_capability(self) -> str:
        """Declare mandatory receipt/fence governance to the ETL admission layer."""

        return MSSQL_GENERIC_TRANSACTION_CAPABILITY

    def target_dialect(self) -> str:
        """Expose a stable sink-family discriminator without class-name checks."""

        return "mssql"

    def project_schema_evolution_source_columns(
        self,
        load_config: LoadConfig,
        columns: Sequence[tuple[str, str, bool, str | None]],
    ) -> tuple[tuple[str, str, bool, str | None], ...]:
        """Compare drift against the value-guarded native staging shape."""

        return MSSQLStrategyBase.project_schema_evolution_source_columns(load_config, columns)

    def native_lineage_projection_capability(self) -> str:
        """Declare server-side lineage over validated typed native values."""

        return "mssql_native_lineage_v1"

    def get_target_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        database = _database(load_config.target_schema, load_config.target_database)
        if hasattr(self.connector, "table_exists") and not _table_exists(
            self.connector,
            load_config.target_schema,
            load_config.target_table,
            database=database,
        ):
            return []
        return list(
            _fetch_schema(self.connector, load_config.target_schema, load_config.target_table, database=database)
        )

    def get_target_columns(self, load_config: LoadConfig) -> list[MssqlCatalogColumn]:
        """Return catalog-complete columns for fail-closed schema evolution."""

        database = _database(load_config.target_schema, load_config.target_database)
        if hasattr(self.connector, "table_exists") and not _table_exists(
            self.connector,
            load_config.target_schema,
            load_config.target_table,
            database=database,
        ):
            return []
        fetch = getattr(self.connector, "fetch_schema_columns", None)
        if not callable(fetch):
            raise RuntimeError("MSSQL connector cannot prove target column nullability")
        try:
            return list(fetch(load_config.target_schema, load_config.target_table, database=database))
        except TypeError:
            return list(fetch(_schema_label(load_config.target_schema, database), load_config.target_table))

    def apply_schema_plan(self, load_config: LoadConfig, plan: SchemaPlan) -> None:
        database = _database(load_config.target_schema, load_config.target_database)
        qualified = _qualified_name(
            self.connector, load_config.target_schema, load_config.target_table, database=database
        )
        for statement in plan.ddl_sql("mssql", qualified):
            self.connector.execute_query(statement)

    def target_table_exists(self, load_config: LoadConfig) -> bool:
        database = _database(load_config.target_schema, load_config.target_database)
        return _table_exists(
            self.connector,
            load_config.target_schema,
            load_config.target_table,
            database=database,
        )

    def get_target_row_count(self, load_config: LoadConfig) -> int:
        """Return an exact target cardinality for schema-evolution budgets."""

        module = import_module("dpone.runtime.sinks.mssql_target_metrics")
        return int(module.MssqlTargetMetrics(self.connector).row_count(load_config))

    def inspect_physical_design(self, load_config: LoadConfig) -> Any:
        module = import_module("dpone.runtime.sinks.mssql_physical_introspection")
        return module.MssqlPhysicalIntrospector(self.connector).inspect(load_config)

    def apply_physical_ddl(self, request: DdlExecutionRequest) -> None:
        self.connector.execute_query(request.sql)

    def save_state(self, load_config: LoadConfig, state: Any) -> None:
        if self.state_storage is not None and hasattr(self.state_storage, "save_state"):
            self.state_storage.save_state(load_config.source_schema, load_config.source_table, state)


def _database(schema: str, database: str | None) -> str | None:
    return None if "." in str(schema) else database


def _schema_label(schema: str, database: str | None) -> str:
    if database and "." not in str(schema):
        return f"{database}.{schema}"
    return str(schema)


def _table_exists(connector: Any, schema: str, table: str, *, database: str | None) -> bool:
    try:
        return bool(connector.table_exists(schema, table, database=database))
    except TypeError:
        return bool(connector.table_exists(_schema_label(schema, database), table))


def _fetch_schema(connector: Any, schema: str, table: str, *, database: str | None) -> list[tuple[str, str]]:
    try:
        return list(connector.fetch_schema(schema, table, database=database))
    except TypeError:
        return list(connector.fetch_schema(_schema_label(schema, database), table))


def _qualified_name(connector: Any, schema: str, table: str, *, database: str | None) -> str:
    try:
        return str(connector.qualified_name(schema, table, database=database))
    except TypeError:
        return str(connector.qualified_name(_schema_label(schema, database), table))


def _default_acceptance_metric_probe(connector: MSSQLConnectorPort) -> Any:
    module = import_module("dpone.runtime.governance.mssql_acceptance_metrics")
    return module.MssqlAcceptanceMetricProbe(connector)


def _require_target_acceptance_database(load_config: LoadConfig) -> None:
    module = import_module("dpone.runtime.governance.acceptance_metrics")
    policy = module.AcceptanceMetricPolicy.from_load_config(load_config)
    if not policy.enabled or not policy.capture_target:
        return
    try:
        target = MSSQLObjectName.from_parts(
            database=load_config.target_database,
            schema=load_config.target_schema,
            table=load_config.target_table,
            strict=True,
        )
    except ValueError as exc:
        raise module.AcceptanceMetricConfigurationError(
            "quality.acceptance.capture.target",
            "MSSQL target relation authority is invalid",
        ) from exc
    if target.database is None:
        raise module.AcceptanceMetricConfigurationError(
            "quality.acceptance.capture.target",
            "MSSQL target database authority is required",
        )
