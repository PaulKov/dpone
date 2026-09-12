"""Полная выборка из PostgreSQL."""

from __future__ import annotations

from typing import Any

from psycopg import sql

from dpone.config.source_scope_contract import resolve_source_scope
from dpone.contracts.portable_scope_resolution import resolve_bound_portable_scope
from dpone.runtime.extraction_lifecycle import ExtractionClock
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.internal_query_capability import InternalQueryCapabilityDecision
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.postgres.postgres_base import PostgresBaseStrategy
from dpone.runtime.sources.strategies.postgres.postgres_portable_scope import (
    render_postgres_portable_scope,
)
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
    PreparedPostgresSourceBoundary,
    prepared_postgres_source_boundary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    rollback_preserving_primary,
)
from dpone.type_system.source_sink.provenance import SourceRelationDialect


class PostgresFullExtractStrategy(PostgresBaseStrategy):
    """
    Стратегия полной выборки из PostgreSQL.

    Поддерживает опциональный custom_predicate для фильтрации:
    - Если НЕ указан → выбираем ВСЕ данные (настоящий FULL)
    - Если указан → выбираем только данные по условию (для REPLACE mode)

    Используется для:
    - FULL_REFRESH: извлечь ВСЕ данные и заменить target таблицу
    - REPLACE: извлечь ЧАСТЬ данных (по custom_predicate) и заменить эту часть в target

    Transport (runtime-issued same-database capability):
    - capability granted → InternalQueryArtifact → server-side staging INSERT
    - capability absent/denied → FileExportArtifact → COPY export/import
    The sink applies the selected load strategy after materialization.
    """

    def __init__(
        self,
        connector,
        logger,
        *,
        internal_query_capability: InternalQueryCapabilityDecision | None = None,
        extraction_clock: ExtractionClock | None = None,
    ):
        super().__init__(
            connector,
            logger,
            internal_query_capability=internal_query_capability,
            extraction_clock=extraction_clock,
        )

    def get_state(self, load_config) -> Any | None:
        return None

    def fetch_schema_projection(self, load_config: Any) -> Any:
        """Reuse the exact prepared projection without another catalog read."""

        prepared = prepared_postgres_source_boundary(load_config)
        if prepared is not None:
            prepared.require_active(self.connector)
            return prepared.schema_projection
        return super().fetch_schema_projection(load_config)

    def abort_mssql_source_boundary(self, load_config: Any) -> None:
        """Release a prepared boundary that never transferred to extraction."""

        prepared = prepared_postgres_source_boundary(load_config)
        if prepared is not None:
            prepared.abort_if_active()

    def prepare_mssql_source_boundary(
        self,
        load_config: Any,
    ) -> PreparedPostgresSourceBoundary:
        """Lock, verify, and project the source on one post-admission RR."""

        if not self._targets_mssql(load_config):
            raise RuntimeError("postgres_prepared_source_boundary.mssql_route_required")
        lifecycle = self._new_extraction_lifecycle()
        snapshot_lease = self._begin_repeatable_read_snapshot(lifecycle)
        try:
            identity = self._verify_postgres_source_authority(
                snapshot_lease,
                load_config,
            )
            projection = self.fetch_schema_projection(load_config)
            return PreparedPostgresSourceBoundary(
                connector=self.connector,
                snapshot_lease=snapshot_lease,
                source_identity=identity,
                schema_projection=projection,
            )
        except BaseException as primary:
            rollback_preserving_primary(self.connector, primary)
            raise

    def extract(self, load_config, last_state: Any | None) -> ExtractResult:
        prepared = prepared_postgres_source_boundary(load_config)
        if prepared is not None:
            try:
                prepared.require_active(self.connector)
                return self._extract_with_projection(
                    load_config,
                    snapshot_lease=prepared.snapshot_lease,
                    projection=prepared.schema_projection,
                    prepared_boundary=prepared,
                )
            except BaseException as primary:
                prepared.abort_preserving(primary)
                raise
        snapshot_lease = None
        if self._targets_mssql(load_config):
            lifecycle = self._new_extraction_lifecycle()
            snapshot_lease = self._begin_repeatable_read_snapshot(lifecycle)
            try:
                self._verify_postgres_source_authority(
                    snapshot_lease,
                    load_config,
                )
                return self._extract_with_projection(
                    load_config,
                    snapshot_lease=snapshot_lease,
                    projection=None,
                    prepared_boundary=None,
                )
            except BaseException as primary:
                rollback_preserving_primary(self.connector, primary)
                raise
        return self._extract_with_projection(
            load_config,
            snapshot_lease=None,
            projection=None,
            prepared_boundary=None,
        )

    def _extract_with_projection(
        self,
        load_config: Any,
        *,
        snapshot_lease: Any | None,
        projection: Any | None,
        prepared_boundary: PreparedPostgresSourceBoundary | None,
    ) -> ExtractResult:
        governed = prepared_boundary is not None and prepared_boundary.source_schema_authority is not None
        projection = projection or self.fetch_schema_projection(load_config)
        schema = list(projection.projected_schema)

        resolved_portable_scope = resolve_bound_portable_scope(load_config)
        predicate = resolve_source_scope(load_config).predicate
        if resolved_portable_scope is not None and predicate is not None:
            raise RuntimeError("postgres_portable_scope.ambiguous_raw_predicate")

        query = self.format_select_query(
            load_config.source_schema,
            load_config.source_table,
            [column for column, _ in schema],
            predicate if resolved_portable_scope is None else None,
            **({"only_relation": True} if governed else {}),
        )
        query_params: tuple[object, ...] = ()
        if resolved_portable_scope is not None:
            rendered = render_postgres_portable_scope(
                resolved_portable_scope.scope,
                resolved_portable_scope.binding,
            )
            query = sql.Composed([query, sql.SQL(" WHERE "), rendered.sql])
            query_params = rendered.params

        # Runtime hydration, not authoring aliases, owns this optimization.
        if self._internal_query_authorized(load_config):
            if query_params:
                raise RuntimeError("postgres_portable_scope.internal_query_parameters_unsupported")
            # Server-side staging transport; the sink retains strategy ownership.
            from dpone.runtime.internal_query_artifact import InternalQueryArtifact

            artifact = InternalQueryArtifact(
                query=query.as_string(self.connector.connection),
                extraction_lifecycle=self._new_extraction_lifecycle(),
            )
            self.logger.log_etl_progress(
                "FULL_REFRESH_SAME_CONNECTION",
                {
                    "Source": f"{load_config.source_schema}.{load_config.source_table}",
                    "Target": f"{load_config.target_schema}.{load_config.target_table}",
                    "Mode": "Internal query to staging; configured sink strategy",
                },
            )
        else:
            # FAST PATH: Binary COPY export/import (для разных БД/серверов)
            export_authority: dict[str, Any] = {"prepared_boundary": prepared_boundary} if governed else {}
            artifact = self._export_to_file(
                query,
                schema,
                load_config,
                batch_size=load_config.batch_size,
                relation_schema=projection.relation_schema,
                query_params=query_params,
                snapshot_lease=None if governed else snapshot_lease,
                **export_authority,
            )

        if governed:
            assert prepared_boundary is not None
            prepared_boundary.require_completed()

        if not governed and snapshot_lease is not None and isinstance(artifact, FileExportArtifact):
            if prepared_boundary is not None:
                prepared_boundary.complete()
            else:
                snapshot_lease.lifecycle.complete()
                self.connector.commit_transaction()

        return ExtractResult(
            artifact=artifact,
            schema=schema,
            relation_schema=projection.relation_schema,
            relation_metadata=projection.relation_metadata,
            relation_dialect=SourceRelationDialect.POSTGRES,
            target_projection=projection.target_projection,
            state=None,
        )
