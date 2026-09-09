"""Runtime-oriented LoadConfig helpers.

Keeps ETLProcessor focused on orchestration by encapsulating runtime-only
mutations of LoadConfig and validation metadata derivation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.process_logging import ETLLogger


from copy import deepcopy
from dataclasses import replace
from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.readiness.profiles import ProductionProfileService


class LoadConfigRuntimeService:
    """Builds isolated runtime copies of :class:`LoadConfig`."""

    def prepare(self, load_config: LoadConfig) -> LoadConfig:
        """Returns an isolated runtime copy so processors do not mutate caller state."""

        options = deepcopy(load_config.options) if load_config.options is not None else {}
        if str(options.get("profile", "")).strip().lower() == "production_safe":
            resolved = ProductionProfileService().apply({"profile": "production_safe", "sink": {"options": options}})
            options = dict(resolved.get("sink", {}).get("options", options))
        return replace(load_config, options=options)

    def inject_extract_identity(self, load_config: LoadConfig, load_record: Any) -> LoadConfig:
        """Attach attempt identity before source I/O for deterministic GCS replacement."""

        options = dict(load_config.options or {})
        options["run_id"] = str(getattr(load_record, "run_id", "") or options.get("run_id") or "")
        options["load_id"] = str(getattr(load_record, "load_id", "") or options.get("load_id") or "")
        return replace(load_config, options=options)

    def apply_extract_result_overrides(
        self,
        load_config: LoadConfig,
        extract_result: Any,
        logger: ETLLogger,
        *,
        run_state_tracker: Any = None,
    ) -> LoadConfig:
        """Applies runtime strategy overrides inferred from extraction result."""

        if not (
            getattr(extract_result, "force_full_refresh", False)
            and load_config.load_strategy != LoadStrategy.FULL_REFRESH
        ):
            return load_config
        if getattr(extract_result, "snapshot_envelope", None) is not None:
            # A complete-key envelope carries its own baseline marker.  It must
            # still reach the incremental MSSQL finalizer so target mutation,
            # key reconciliation, checkpoint CAS and receipt share one commit.
            return load_config

        overridden = replace(load_config, load_strategy=LoadStrategy.FULL_REFRESH)
        if run_state_tracker is not None:
            run_state_tracker.update_load_strategy(LoadStrategy.FULL_REFRESH.value)
        logger.log_etl_progress(
            "LOAD_STRATEGY_OVERRIDE",
            {
                "Original": load_config.load_strategy.value,
                "Override": LoadStrategy.FULL_REFRESH.value,
                "Reason": "Target missing, bootstrap full refresh",
            },
        )
        return overridden

    def enrich_for_load(
        self,
        load_config: LoadConfig,
        extract_result: Any,
        source: Any,
        logger: ETLLogger,
    ) -> LoadConfig:
        """Enriches runtime options with extraction/runtime-specific metadata."""

        options = load_config.options if load_config.options is not None else {}
        artifact = extract_result.artifact

        partitions_to_delete: list[Any] = []
        if hasattr(artifact, "lookback_partitions") and artifact.lookback_partitions:
            partitions_to_delete.extend(artifact.lookback_partitions)
        if hasattr(artifact, "incremental_partitions") and artifact.incremental_partitions:
            partitions_to_delete.extend(artifact.incremental_partitions)

        if partitions_to_delete:
            unique_partitions = sorted(list(set(partitions_to_delete)))
            options["_lookback_partitions"] = unique_partitions

            if hasattr(artifact, "incremental_column"):
                options["incremental_column"] = artifact.incremental_column

            if hasattr(artifact, "column_timezone") and artifact.column_timezone:
                options["_column_timezone"] = artifact.column_timezone
                logger.info(f"🌐 Передача timezone из ClickHouse: {artifact.column_timezone}")
            else:
                logger.warning(
                    f"⚠️ column_timezone отсутствует в artifact! "
                    f"hasattr={hasattr(artifact, 'column_timezone')}, "
                    f"value={getattr(artifact, 'column_timezone', None)}"
                )

            logger.info(f"📌 Передача партиций для DELETE из target: {unique_partitions}")

        if hasattr(artifact, "new_partitions") and artifact.new_partitions:
            options["_new_partitions"] = artifact.new_partitions

        if options.get("date_column"):
            options["date_column"] = options.get("date_column")
        elif options.get("incremental_column") and hasattr(artifact, "new_partitions"):
            options["date_column"] = options.get("incremental_column")

        if options.get("partition_by"):
            options["partition_by"] = options.get("partition_by")
        elif hasattr(artifact, "new_partitions") and artifact.new_partitions:
            options["partition_by"] = "day"

        self._attach_source_connector_options(load_config, source, logger)
        return load_config

    def build_validation_info(
        self,
        source: Any,
        load_config: LoadConfig,
        load_result: Any,
        extract_result: Any,
    ) -> dict[str, Any] | None:
        """Builds optional validation payload for ClickHouse partition checks."""

        if not hasattr(source, "connector") or not hasattr(source.connector, "__class__"):
            return None
        connector_class = source.connector.__class__.__name__
        if connector_class != "ClickHouseConnector":
            return None

        partitions: list[Any] = []
        artifact = extract_result.artifact
        if hasattr(artifact, "new_partitions") and artifact.new_partitions:
            partitions = artifact.new_partitions
        elif hasattr(artifact, "lookback_partitions") and artifact.lookback_partitions:
            partitions = artifact.lookback_partitions

        options = load_config.options if load_config.options is not None else {}
        date_column = options.get("date_column")
        partition_by = options.get("partition_by", "day")
        if not (partitions and date_column):
            return None

        return {
            "source_connector": source.connector,
            "source_schema": load_config.source_schema,
            "source_table": load_config.source_table,
            "date_column": date_column,
            "partition_by": partition_by,
            "partitions": partitions,
            "final_count": load_result.total_rows,
            "partition_validation_results": getattr(load_result, "partition_validation_results", {}),
        }

    def should_persist_state(
        self,
        original_load_config: LoadConfig,
        effective_load_config: LoadConfig,
        extract_result: Any,
    ) -> bool:
        """Returns True when extracted state should be saved after load."""

        if getattr(extract_result, "state", None) is None:
            return False

        if effective_load_config.load_strategy in (
            LoadStrategy.INCREMENTAL_MERGE,
            LoadStrategy.INCREMENTAL_APPEND,
            LoadStrategy.SNAPSHOT_DIFF,
            LoadStrategy.SCD2,
            LoadStrategy.CDC_APPLY,
            LoadStrategy.BACKFILL,
        ):
            return True

        original_incremental = original_load_config.load_strategy in (
            LoadStrategy.INCREMENTAL_MERGE,
            LoadStrategy.INCREMENTAL_APPEND,
            LoadStrategy.SNAPSHOT_DIFF,
            LoadStrategy.SCD2,
            LoadStrategy.CDC_APPLY,
            LoadStrategy.BACKFILL,
        )
        return bool(original_incremental and getattr(extract_result, "force_full_refresh", False))

    def _attach_source_connector_options(
        self,
        load_config: LoadConfig,
        source: Any,
        logger: ETLLogger,
    ) -> None:
        if not hasattr(source, "connector"):
            return
        source_connector = getattr(source, "connector", None)
        if not source_connector:
            return

        options = load_config.options if load_config.options is not None else {}
        options["_source_connector"] = source_connector
        if load_config.reconciliation:
            options["_source_conn_id"] = load_config.source_conn_id
            options["_source_schema"] = load_config.source_schema
            options["_source_table"] = load_config.source_table
            logger.info(
                f"🔗 Передача source connector для reconciliation: "
                f"{source_connector.__class__.__name__} → {load_config.source_schema}.{load_config.source_table}"
            )
        else:
            logger.info(
                f"🔗 Передача source connector для валидации: "
                f"{source_connector.__class__.__name__} → {load_config.source_schema}.{load_config.source_table}"
            )
