"""Construct ClickHouse sink collaborators without running their callbacks.

This boundary owns transport and storage wiring. The returned bundle carries the
existing collaborators directly; execution and lifecycle policy stay with them.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any

from dpone.runtime.clickhouse_file_stage_contract import (
    ClickHouseFileStageRunner,
    ClickHouseValidatedFilePolicy,
    require_transport_profile,
)
from dpone.runtime.connectors.clickhouse_file_stage_client import build_file_client_runner
from dpone.runtime.connectors.clickhouse_file_stage_http import build_file_http_runner
from dpone.runtime.sinks.clickhouse_full_refresh_router import ClickHouseFullRefreshPublicationRouter
from dpone.runtime.sinks.clickhouse_physical_types import ClickHousePhysicalColumnTypeResolver
from dpone.runtime.sinks.clickhouse_staging_decoder import ClickHouseStagingDecoder
from dpone.runtime.sinks.clickhouse_staging_finalizer import ClickHouseStagingFinalizer
from dpone.runtime.sinks.clickhouse_validated_file_ingestion import ClickHouseValidatedFileService
from dpone.runtime.sinks.clickhouse_validated_file_journal import ClickHouseFileAttemptJournal
from dpone.runtime.storage_policy import StoragePreflightService

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.clickhouse_connector import ClickHouseConnectorPort
    from dpone.runtime.sinks.load_result import LoadResult


class ClickHouseFullRefreshPublicationMixin:
    """Expose publication lifecycle hooks through the sink's narrow service."""

    _full_refresh_publication: ClickHouseFullRefreshPublicationRouter

    def prepare_runtime_admission(
        self,
        load_config: LoadConfig,
        *,
        run_context: Any,
        load_record: Any,
        dag_id: str | None,
    ) -> LoadConfig:
        """Preflight or reconcile full-refresh publication before source I/O."""

        del load_record
        identified = self._full_refresh_publication.bind_runtime_identity(
            load_config,
            scheduler_run_id=str(getattr(run_context, "run_id", "") or ""),
            process_id=str(dag_id or ""),
        )
        return self._full_refresh_publication.prepare_admission(identified)

    def replay_result(self, load_config: LoadConfig) -> LoadResult | None:
        """Return a catalog-reconciled source-free publication result."""

        return self._full_refresh_publication.replay_result(load_config)

    def _cleanup_full_refresh_publication(self, receipt: Any) -> None:
        self._full_refresh_publication.cleanup(receipt)


@dataclass(frozen=True, slots=True)
class ClickHouseStagingComponents:
    """Construction result; each component retains its existing execution API."""

    validated_file: ClickHouseValidatedFileService
    decoder: ClickHouseStagingDecoder
    finalizer: ClickHouseStagingFinalizer
    full_refresh_publication: ClickHouseFullRefreshPublicationRouter


def build_clickhouse_staging_components(
    *,
    connector: ClickHouseConnectorPort,
    connector_provider: Callable[[], ClickHouseConnectorPort],
    resolver: ClickHousePhysicalColumnTypeResolver,
    clock: Callable[[], float],
    validated_file_runner_factory: Callable[[LoadConfig, ClickHouseValidatedFilePolicy], ClickHouseFileStageRunner]
    | None,
    table_name: Callable[[LoadConfig], str],
    create_staging_table: Callable[[LoadConfig, Sequence[tuple[str, str]]], LoadConfig],
    map_type: Callable[..., str],
    drop_staging_table: Callable[[LoadConfig], None],
    plan_staging_table: Callable[[LoadConfig], LoadConfig],
    create_planned_staging_table: Callable[[LoadConfig, Sequence[tuple[str, str]]], None],
    count_rows: Callable[[LoadConfig], int],
    mutations_sync: Callable[[LoadConfig], int],
) -> ClickHouseStagingComponents:
    """Wire service, decoder and finalizer in their existing construction order.

    The default file runner reads the connector provider when invoked. Decoder
    and finalizer retain the initialization-time connector and supplied callback
    objects, including the decoder's inspectable map-type signature.
    """
    validated_file = ClickHouseValidatedFileService(
        runner_factory=validated_file_runner_factory
        or (lambda config, policy: build_file_runner(config, policy, connector=connector_provider(), clock=clock)),
        resolver=resolver,
        journal_factory=partial(ClickHouseFileAttemptJournal, storage=StoragePreflightService()),
        clock=clock,
    )
    decoder = ClickHouseStagingDecoder(
        connector=connector,
        table_name=table_name,
        create_staging_table=create_staging_table,
        map_type=map_type,
        drop_staging_table=drop_staging_table,
        plan_staging_table=plan_staging_table,
        create_planned_staging_table=create_planned_staging_table,
    )
    finalizer = ClickHouseStagingFinalizer(
        connector=connector,
        table_name=table_name,
        count_rows=count_rows,
        mutations_sync=mutations_sync,
    )
    full_refresh_publication = ClickHouseFullRefreshPublicationRouter.from_connector(connector)
    return ClickHouseStagingComponents(validated_file, decoder, finalizer, full_refresh_publication)


def build_full_refresh_publication_router(sink: Any) -> ClickHouseFullRefreshPublicationRouter:
    """Complete publication wiring after the sink composition root exists."""

    return ClickHouseFullRefreshPublicationRouter.from_sink(sink)


def build_file_runner(
    config: LoadConfig, policy: ClickHouseValidatedFilePolicy, *, connector: Any, clock: Callable[[], float]
) -> ClickHouseFileStageRunner:
    """Composition helper; explicit mode, no environment fallback or legacy runner."""
    del policy
    mode, timeout = require_transport_profile(config.options or {})
    selected = (config.options or {})["clickhouse_bulk"].get(mode, {})

    def setting(name: str, default: Any) -> Any:
        return selected[name] if name in selected else default

    common = dict(
        host=setting("host", connector.host),
        port=setting("port", connector.port if mode == "client" else 8123),
        database=setting("database", connector.database),
        user=setting("user", connector.user),
        password=setting("password", connector.password),
        secure=setting("secure", connector.secure if mode == "client" else False),
    )
    if mode == "client":
        return build_file_client_runner(
            **common, timeout=timeout, command=setting("command", "clickhouse-client"), clock=clock
        )
    return build_file_http_runner(**common, timeout=timeout, clock=clock)


build_file_runner.__module__ = "dpone.runtime.sinks.clickhouse_sink"
