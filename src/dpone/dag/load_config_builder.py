"""Build :class:`dpone.config.LoadConfig` from compiled process config dicts."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.config.mssql_strategy_contract import (
    MSSQLStrategyContractError,
    normalize_mssql_authoring_strategy,
)
from dpone.config.postgres_mssql_wire_contract import (
    PostgresMssqlWireContractError,
    normalize_postgres_mssql_wire,
)
from dpone.config.reconciliation import ReconciliationConfigError, normalize_reconciliation
from dpone.contracts.api_sources import get_api_source_defaults
from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.dag.errors import DagConfigurationError
from dpone.dag.export_format_validation import validate_export_format_for_sink
from dpone.dag.load_config_builder_support import (
    derive_api_source,
    inject_manifest_context_options,
    inject_runtime_contract_options,
    is_mssql,
    merge_load_options,
    normalize_mssql_schema_label,
    optional_text,
    record_load_fields,
    record_runtime_contract_fields,
    resolve_batch_size,
    resolve_connection_identity,
    resolve_unique_key,
)
from dpone.dag.load_config_endpoint_identity import inject_endpoint_identity_options
from dpone.dag.load_config_partition_contract import resolve_partition_contract
from dpone.dag.load_config_scope import resolve_load_scopes
from dpone.strategy_intelligence.compiler import StrategyAutoCompiler

if TYPE_CHECKING:  # pragma: no cover
    from dpone.dag.parse_trace import ParseTracer


class LoadConfigBuilder:
    """Normalizes ``source`` / ``sink`` config blocks into :class:`LoadConfig`."""

    def __init__(self, *, strategy_compiler: StrategyAutoCompiler | None = None) -> None:
        self._strategy_compiler = strategy_compiler or StrategyAutoCompiler()

    def build(
        self,
        config: dict[str, Any],
        *,
        base_path: Path | None = None,
        parse_tracer: ParseTracer | None = None,
    ) -> LoadConfig:
        try:
            source_cfg = dict(config.get("source", {}) or {})
            sink_cfg = dict(config.get("sink", {}) or {})
            runtime_cfg = dict(config.get("runtime", {}) or {})

            source_type = source_cfg.get("type", "postgres")
            canonical_source_type = canonical_endpoint_type(str(source_type))
            source_conn_id = resolve_connection_identity(source_cfg)
            if source_type == "api":
                api_defaults = get_api_source_defaults(source_cfg.get("api_type"))
                source_conn_id = source_conn_id or api_defaults.connection_id()
            source_table_cfg = source_cfg.get("table", {})
            source_database = optional_text(source_table_cfg.get("database"))
            source_options = dict(source_cfg.get("options", {}) or {})

            if parse_tracer:
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.source_conn_id",
                    value=source_conn_id,
                    sources=("source.connection_id", "source.connection_ref", "source.api_type"),
                    operation=(
                        "derive_api"
                        if source_type == "api"
                        and "connection_id" not in source_cfg
                        and "connection_ref" not in source_cfg
                        else "copy"
                    ),
                    details={"source_type": source_type} if source_type == "api" else None,
                )
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.source_type",
                    value=source_type,
                    sources=("source.type",),
                    operation="default" if "type" not in source_cfg else "copy",
                )

            if source_type == "api":
                source_schema, source_table = derive_api_source(
                    source_cfg=source_cfg,
                    source_table_cfg=source_table_cfg,
                    source_options=source_options,
                    parse_tracer=parse_tracer,
                )
            elif source_type == "kafka":
                source_schema = source_table_cfg.get("schema") or "kafka"
                source_table = source_cfg.get("topic") or source_table_cfg.get("name")
                source_options = {**source_options, "topic": source_table}
            else:
                source_schema = source_table_cfg.get("schema")
                source_table = source_table_cfg.get("name")
                source_database, source_schema = normalize_mssql_schema_label(
                    dialect=source_type,
                    database=source_database,
                    schema=source_schema,
                    table=source_table,
                )
                if parse_tracer:
                    parse_tracer.record(
                        kind="load_config.field",
                        target="load_config.source_schema",
                        value=source_schema,
                        sources=("source.table.schema",),
                        operation="copy",
                    )
                    parse_tracer.record(
                        kind="load_config.field",
                        target="load_config.source_table",
                        value=source_table,
                        sources=("source.table.name",),
                        operation="copy",
                    )

            target_conn_id = resolve_connection_identity(sink_cfg)
            sink_type = sink_cfg.get("type", "bigquery")
            canonical_sink_type = canonical_endpoint_type(str(sink_type))
            sink_table_cfg = sink_cfg.get("table", {})
            target_database = optional_text(sink_table_cfg.get("database"))
            target_schema = sink_table_cfg.get("schema")
            target_table = sink_table_cfg.get("name")
            if sink_type == "kafka":
                target_schema = target_schema or "kafka"
                target_table = sink_cfg.get("topic") or target_table
            else:
                target_database, target_schema = normalize_mssql_schema_label(
                    dialect=sink_type,
                    database=target_database,
                    schema=target_schema,
                    table=target_table,
                )
            if parse_tracer:
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.target_conn_id",
                    value=target_conn_id,
                    sources=("sink.connection_id", "sink.connection_ref"),
                    operation="copy",
                )
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.target_schema",
                    value=target_schema,
                    sources=("sink.table.schema",),
                    operation="copy",
                )
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.target_table",
                    value=target_table,
                    sources=("sink.table.name",),
                    operation="copy",
                )

            staging_cfg = sink_cfg.get("staging", {})
            staging_database = optional_text(staging_cfg.get("database"))
            if is_mssql(sink_type) and staging_database is None:
                staging_database = target_database
            staging_schema = staging_cfg.get("schema", "staging")
            staging_database, staging_schema = normalize_mssql_schema_label(
                dialect=sink_type,
                database=staging_database,
                schema=staging_schema,
                table=target_table or "staging",
            )
            if parse_tracer:
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.staging_schema",
                    value=staging_schema,
                    sources=("sink.staging.schema",),
                    operation="default" if "schema" not in staging_cfg else "copy",
                )

            sink_options = dict(sink_cfg.get("options", {}) or {})
            if sink_cfg.get("type") == "kafka" and target_table:
                sink_options = {**sink_options, "topic": target_table}
            for endpoint, endpoint_options in (("source", source_options), ("sink", sink_options)):
                if "quality" in endpoint_options:
                    raise DagConfigurationError(
                        f"{endpoint}.options.quality is misplaced; author quality at the process or manifest root"
                    )

            strategy_cfg = dict(sink_cfg.get("strategy", {}) or {})
            unique_key = resolve_unique_key(
                source_options=source_options,
                strategy_config=strategy_cfg,
                sink_options=sink_options,
            )
            load_strategy, strategy_intelligence = self._strategy_compiler.compile(
                source_type=canonical_source_type,
                sink_type=canonical_sink_type,
                strategy_cfg=strategy_cfg,
                source_options=source_options,
                sink_options=sink_options,
                source_table=f"{source_schema}.{source_table}",
                target_table=f"{target_schema}.{target_table}",
            )
            only_new_rows = strategy_cfg.get("only_new_rows", False)
            overwrite_type = strategy_cfg.get("overwrite_type")
            merge_policy = strategy_cfg.get("merge_policy", "auto")
            duplicate_policy = strategy_cfg.get("duplicate_policy", "fail")
            partition = resolve_partition_contract(
                strategy_config=strategy_cfg,
                source_options=source_options,
                load_strategy=load_strategy,
            )
            allow_non_recommended_policy = bool(strategy_cfg.get("allow_non_recommended_policy", False))
            mutations_sync = strategy_cfg.get("mutations_sync")
            if parse_tracer:
                mode_raw = strategy_cfg.get("mode", LoadStrategy.FULL_REFRESH.value)
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.load_strategy",
                    value=getattr(load_strategy, "value", load_strategy),
                    sources=("sink.strategy.mode",),
                    operation="parse_enum",
                    details={"raw": mode_raw},
                )
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.only_new_rows",
                    value=only_new_rows,
                    sources=("sink.strategy.only_new_rows",),
                    operation="default" if "only_new_rows" not in strategy_cfg else "copy",
                )
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.overwrite_type",
                    value=overwrite_type,
                    sources=("sink.strategy.overwrite_type",),
                    operation="copy" if "overwrite_type" in strategy_cfg else "default",
                )

            reconciliation, reconciliation_options = normalize_reconciliation(config.get("reconciliation", False))
            tech_schema = config.get("tech_schema", "tech")
            record_runtime_contract_fields(
                config=config,
                reconciliation=reconciliation,
                tech_schema=tech_schema,
                parse_tracer=parse_tracer,
            )

            micro_batch_commit = source_options.get("micro_batch_commit", False)
            if parse_tracer:
                parse_tracer.record(
                    kind="load_config.field",
                    target="load_config.micro_batch_commit",
                    value=micro_batch_commit,
                    sources=("source.options.micro_batch_commit",),
                    operation="default" if "micro_batch_commit" not in source_options else "copy",
                )

            self._validate_export_format(source_options=source_options, sink_cfg=sink_cfg)
            options = merge_load_options(
                source_options=source_options, sink_options=sink_options, parse_tracer=parse_tracer
            )
            options.update({"source_options": dict(source_options), "sink_options": dict(sink_options)})
            reconciliation_policy = inject_runtime_contract_options(
                config=config,
                runtime_config=runtime_cfg,
                options=options,
                reconciliation_options=reconciliation_options,
                parse_tracer=parse_tracer,
            )
            inject_endpoint_identity_options(
                options, source_type, sink_type, canonical_source_type, canonical_sink_type
            )
            options.setdefault("merge_policy", merge_policy)
            options.setdefault("duplicate_policy", duplicate_policy)
            options.setdefault("partition", partition)
            options.setdefault("allow_non_recommended_policy", allow_non_recommended_policy)
            if strategy_intelligence is not None:
                options.setdefault("strategy_intelligence", strategy_intelligence)
            if mutations_sync is not None:
                options.setdefault("mutations_sync", mutations_sync)
            inject_manifest_context_options(options=options, base_path=base_path, parse_tracer=parse_tracer)
            for strategy_option in ("diff", "scd2", "cdc", "backfill"):
                if strategy_option in strategy_cfg:
                    options.setdefault(strategy_option, strategy_cfg[strategy_option])

            portable_scope, sink_custom_predicate = resolve_load_scopes(
                strategy_config=strategy_cfg,
                source_options=source_options,
                options=options,
                parse_tracer=parse_tracer,
            )

            if parse_tracer:
                record_load_fields(
                    parse_tracer=parse_tracer,
                    source_options=source_options,
                    sink_options=sink_options,
                    strategy_cfg=strategy_cfg,
                    sink_custom_predicate=sink_custom_predicate,
                )

            dedup_target_raw = options.get("dedup_target")
            dedup_target = str(dedup_target_raw).strip() if dedup_target_raw is not None else ""

            load_config = LoadConfig(
                source_conn_id=str(source_conn_id or ""),
                target_conn_id=str(target_conn_id or ""),
                source_schema=source_schema,
                source_table=source_table,
                target_schema=target_schema,
                target_table=target_table,
                source_database=source_database,
                target_database=target_database,
                staging_database=staging_database,
                staging_schema=staging_schema,
                load_strategy=load_strategy,
                unique_key=unique_key,
                only_new_rows=only_new_rows,
                micro_batch_commit=micro_batch_commit,
                overwrite_type=overwrite_type,
                merge_policy=merge_policy,
                duplicate_policy=duplicate_policy,
                allow_non_recommended_policy=allow_non_recommended_policy,
                mutations_sync=mutations_sync,
                partition=partition,
                dedup_expression=options.get("dedup_expression"),
                dedup_target=dedup_target or "stg",
                with_dedup=options.get("with_dedup", False),
                custom_predicate=sink_custom_predicate,
                portable_scope=portable_scope,
                batch_size=resolve_batch_size(
                    source_options=source_options,
                    sink_options=sink_options,
                    strategy_intelligence=strategy_intelligence,
                ),
                log_sample_rows=sink_options.get("log_sample_rows", 5),
                export_format=source_options.get("export_format", "csv"),
                compress_export=source_options.get("compress_export", False),
                reconciliation=reconciliation,
                reconciliation_policy=reconciliation_policy,
                tech_schema=tech_schema,
                options=options,
            )
            if is_mssql(sink_type):
                try:
                    normalize_mssql_authoring_strategy(load_config)
                    if canonical_source_type == "postgres":
                        normalize_postgres_mssql_wire(load_config)
                except (MSSQLStrategyContractError, PostgresMssqlWireContractError) as exc:
                    raise DagConfigurationError(str(exc)) from exc
            return load_config
        except KeyError as exc:
            raise DagConfigurationError(f"Не хватает обязательного параметра в конфигурации: {exc.args[0]}")
        except ReconciliationConfigError as exc:
            raise DagConfigurationError(str(exc)) from exc

    def _validate_export_format(self, *, source_options: dict[str, Any], sink_cfg: dict[str, Any]) -> None:
        validate_export_format_for_sink(source_options=source_options, sink_cfg=sink_cfg)


__all__ = ["LoadConfigBuilder"]
