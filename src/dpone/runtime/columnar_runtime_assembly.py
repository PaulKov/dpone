"""Production assembly for columnar route capability runtime."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.runtime.columnar_execution_mode import resolve_columnar_execution_policy
from dpone.runtime.columnar_route_runtime import (
    ColumnarCapabilityProbeRunner,
    ColumnarDirectPushExecutor,
    ColumnarObjectStoragePullExecutor,
    ColumnarRouteCandidateProvider,
)
from dpone.runtime.object_storage_access_models import ObjectStorageAccessEvidence, ObjectStorageAccessRequest
from dpone.runtime.object_storage_clickhouse_probe import ClickHouseObjectStorageReadinessProbe
from dpone.runtime.object_storage_connection_resolver import ObjectStorageConnectionResolver
from dpone.runtime.object_storage_fast_path_preflight import ObjectStorageAccessPreflightService
from dpone.runtime.sinks.clickhouse_capabilities import ClickHouseColumnarCapabilityProbe
from dpone.runtime.sources.strategies.mssql import mssql_columnar_range_admission
from dpone.runtime.sources.strategies.mssql.mssql_columnar_capability_probe import (
    MssqlColumnarSourceCapabilityProbe,
)
from dpone.runtime.sources.strategies.mssql.mssql_columnar_provider import MssqlColumnarSnapshotProvider
from dpone.runtime.sources.strategies.mssql.mssql_columnar_queryout_bridge import build_columnar_snapshot_request
from dpone.runtime.sources.strategies.mssql.mssql_columnar_reader import (
    authority_database,
    consistency_binding,
    resolve_partition_bounds,
    resolve_range_consistency,
    verify_write_exclusion,
)
from dpone.storage import ObjectStorageUri

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_SourceCapabilityProbe = MssqlColumnarSourceCapabilityProbe


@dataclass(frozen=True, slots=True)
class RouteRuntimeAssembly:
    candidate_provider: Any
    probe_runner: Any
    executors: Sequence[Any]
    decision_details_provider: Any | None = None


class ColumnarRuntimeAssembly:
    """Wire the first certified MSSQL -> object storage -> ClickHouse route."""

    def __init__(
        self,
        *,
        object_client: Any | None = None,
        object_storage_resolver: ObjectStorageConnectionResolver | None = None,
        parquet_writer: Any | None = None,
        clickhouse_probe_connector: Any | None = None,
        write_exclusion_verifier: Callable[..., bool] | None = None,
    ) -> None:
        self._object_client = object_client
        self._resolver = object_storage_resolver or ObjectStorageConnectionResolver()
        self._parquet_writer = parquet_writer
        self._clickhouse_probe_connector = clickhouse_probe_connector
        self._write_exclusion_verifier = write_exclusion_verifier

    def build(self, *, load_config: LoadConfig, source: Any, sink: Any) -> RouteRuntimeAssembly | None:
        if _columnar_mode(load_config) in {"off", "benchmark_only", ""}:
            return None
        request_factory = MssqlColumnarSnapshotRequestFactory(write_exclusion_verifier=self._write_exclusion_verifier)
        object_client = self._object_client or self._build_object_client(load_config)
        provider = MssqlColumnarSnapshotProvider(
            connector=getattr(source, "connector", None),
            object_client=object_client,
            parquet_writer=self._parquet_writer,
        )
        probe_runner = ColumnarCapabilityProbeRunner(
            source_capability=_SourceCapabilityProbe(request_factory, provider),
            object_storage_access=_ObjectStorageProbe(load_config, object_client, self._clickhouse_connector(sink)),
            sink_evidence=_SinkCapabilityProbe(load_config, self._clickhouse_connector(sink)),
        )
        return RouteRuntimeAssembly(
            candidate_provider=ColumnarRouteCandidateProvider(
                include_direct_push=True,
                range_admission_available=mssql_columnar_range_admission.range_byte_admission_available,
            ),
            probe_runner=probe_runner,
            executors=(
                ColumnarObjectStoragePullExecutor(
                    route_id="object_storage_pull_s3cluster",
                    request_factory=request_factory,
                    snapshot_provider=provider,
                ),
                ColumnarObjectStoragePullExecutor(
                    route_id="object_storage_pull_s3",
                    request_factory=request_factory,
                    snapshot_provider=provider,
                ),
                ColumnarDirectPushExecutor(
                    route_id="direct_push_columnar",
                    request_factory=request_factory,
                    snapshot_provider=provider,
                ),
            ),
            decision_details_provider=ColumnarRouteDecisionDetailsProvider(),
        )

    def _build_object_client(self, load_config: LoadConfig) -> Any:
        if not _object_storage_options(load_config):
            return None
        access_request = _object_storage_access_request(load_config)
        uri = ObjectStorageUri.parse(access_request.uri_prefix.format(run_id=_configured_run_id(load_config)))
        return self._resolver.build_client(ref=access_request.runtime_access.connection, uri=uri)

    def _clickhouse_connector(self, sink: Any) -> Any:
        return self._clickhouse_probe_connector or getattr(sink, "connector", None)


class MssqlColumnarSnapshotRequestFactory:
    def __init__(self, *, write_exclusion_verifier: Callable[..., bool] | None = None) -> None:
        self._write_exclusion_verifier = write_exclusion_verifier

    def __call__(self, *, load_config: LoadConfig, source: Any, sink: Any, state: Any, load_record: Any) -> Any:
        del sink, state
        connector = getattr(source, "connector", None)
        source_options = _source_options(load_config)
        partitioning, range_sessions_available, byte_admission_available = (
            mssql_columnar_range_admission.validate_range_activation(source_options, connector)
        )
        if partitioning.range_parallelism.mode == "auto" and not byte_admission_available:
            consistency, authority = "immutable", {}
        else:
            consistency, authority = resolve_range_consistency(source_options)
        if consistency == "temporal_as_of" and not callable(getattr(connector, "build_temporal_select_query", None)):
            raise RuntimeError("columnar_temporal_as_of_query_builder_missing")
        database = authority_database(
            consistency,
            authority,
            _database(load_config.source_schema, load_config.source_database),
        )
        verify_write_exclusion(
            consistency,
            authority,
            verifier=self._write_exclusion_verifier,
            connector=connector,
            load_config=load_config,
        )
        schema = _fetch_schema(connector, load_config, database=database)
        query = _select_query(
            connector,
            load_config,
            [column for column, _ in schema],
            database=database,
            consistency=consistency,
            authority=authority,
        )
        schema_types = {column.casefold(): source_type for column, source_type in schema}
        return build_columnar_snapshot_request(
            load_config=load_config,
            query=query,
            schema=schema,
            run_id=str(getattr(load_record, "run_id", "") or _configured_run_id(load_config)),
            bounds_resolver=lambda column: resolve_partition_bounds(
                connector,
                query=query,
                column=column,
                source_type=schema_types.get(column.casefold(), ""),
            ),
            consistency_binding=consistency_binding(consistency, authority, database),
            range_capability_available=range_sessions_available,
        )


class ColumnarRouteDecisionDetailsProvider:
    def __call__(self, *, load_config: LoadConfig, source: Any, sink: Any, load_record: Any) -> dict[str, object]:
        del sink, load_record
        options = _columnar_fast_path_options(load_config)
        if not options:
            return {}
        policy = resolve_columnar_execution_policy(options)
        details: dict[str, object] = {
            "execution_mode": policy.value,
            "execution_requested": policy.requested,
            "execution_deprecated_alias": policy.deprecated_alias,
            "cleanup_policy": policy.cleanup_policy,
        }
        details.update(
            mssql_columnar_range_admission.range_decision_details(
                _source_options(load_config), getattr(source, "connector", None)
            )
        )
        return details


class _ObjectStorageProbe:
    def __init__(self, load_config: LoadConfig, object_client: Any, clickhouse_connector: Any) -> None:
        self._access_request = _optional_object_storage_access_request(load_config)
        self._object_client = object_client
        self._clickhouse_connector = clickhouse_connector

    def __call__(self, **context: Any) -> ObjectStorageAccessEvidence | None:
        if self._access_request is None or self._object_client is None:
            return None
        load_record = context.get("load_record")
        run_id = str(getattr(load_record, "run_id", "") or _configured_run_id(context["load_config"]))
        return ObjectStorageAccessPreflightService(
            object_client=self._object_client,
            clickhouse_probe=ClickHouseObjectStorageReadinessProbe(connector=self._clickhouse_connector),
        ).run(self._access_request, run_id=run_id)


class _SinkCapabilityProbe:
    def __init__(self, load_config: LoadConfig, clickhouse_connector: Any) -> None:
        self._load_config = load_config
        self._access_request = _optional_object_storage_access_request(load_config)
        self._pull_options = _columnar_pull_options(load_config)
        self._probe = ClickHouseColumnarCapabilityProbe(connector=clickhouse_connector)

    def __call__(self, **context: Any) -> dict[str, Any]:
        if self._access_request is None:
            return self._probe.probe_direct_push()
        access_evidence = context.get("object_storage_access")
        if bool(getattr(access_evidence, "passed", False)):
            return self._probe.probe_columnar_pull_from_preflight(
                read_contract=self._access_request.clickhouse_read_access,
                access_evidence=access_evidence,
                cluster=_text(self._pull_options.get("cluster")),
                require_cluster_read=self._access_request.require_cluster_read,
                settings=_mapping(self._pull_options.get("settings")),
            )
        load_record = context.get("load_record")
        run_id = str(getattr(load_record, "run_id", "") or _configured_run_id(self._load_config))
        sentinel_uri = self._access_request.resolved_prefix(run_id).child("__dpone_sentinel.parquet")
        return self._probe.probe_columnar_pull(
            read_contract=self._access_request.clickhouse_read_access,
            sentinel_uri=sentinel_uri,
            use_cluster_function=str(self._pull_options.get("use_cluster_function") or "auto"),
            cluster=_text(self._pull_options.get("cluster")),
            settings=_mapping(self._pull_options.get("settings")),
        )


def _object_storage_access_request(load_config: LoadConfig) -> ObjectStorageAccessRequest:
    object_storage = _object_storage_options(load_config)
    if not object_storage:
        raise RuntimeError("columnar_object_storage_options_missing")
    return ObjectStorageAccessRequest.from_options(object_storage, columnar_pull=_columnar_pull_options(load_config))


def _optional_object_storage_access_request(load_config: LoadConfig) -> ObjectStorageAccessRequest | None:
    object_storage = _object_storage_options(load_config)
    if not object_storage:
        return None
    return ObjectStorageAccessRequest.from_options(object_storage, columnar_pull=_columnar_pull_options(load_config))


def _fetch_schema(connector: Any, load_config: LoadConfig, *, database: str | None = None) -> list[tuple[str, str]]:
    if connector is None or not hasattr(connector, "fetch_schema"):
        raise RuntimeError("mssql_source_schema_reader_missing")
    database = database or _database(load_config.source_schema, load_config.source_database)
    try:
        return list(connector.fetch_schema(load_config.source_schema, load_config.source_table, database=database))
    except TypeError:
        return list(
            connector.fetch_schema(_schema_label(load_config.source_schema, database), load_config.source_table)
        )


def _select_query(
    connector: Any,
    load_config: LoadConfig,
    columns: list[str],
    *,
    database: str | None = None,
    consistency: str = "immutable",
    authority: dict[str, Any] | None = None,
) -> str:
    if connector is None or not hasattr(connector, "build_select_query"):
        raise RuntimeError("mssql_source_query_builder_missing")
    database = database or _database(load_config.source_schema, load_config.source_database)
    if consistency == "temporal_as_of":
        builder = getattr(connector, "build_temporal_select_query", None)
        if not callable(builder):
            raise RuntimeError("columnar_temporal_as_of_query_builder_missing")
        query = str(
            builder(
                load_config.source_schema,
                load_config.source_table,
                columns,
                as_of=str((authority or {})["as_of"]),
                database=database,
            )
        )
        predicate = load_config.options.get("source_custom_predicate") or load_config.custom_predicate
        return f"{query} WHERE {predicate}" if predicate else query
    try:
        query = str(
            connector.build_select_query(
                load_config.source_schema,
                load_config.source_table,
                columns,
                database=database,
            )
        )
    except TypeError:
        query = str(
            connector.build_select_query(
                _schema_label(load_config.source_schema, database),
                load_config.source_table,
                columns,
            )
        )
    predicate = load_config.options.get("source_custom_predicate") or load_config.custom_predicate
    return f"{query} WHERE {predicate}" if predicate else query


def _columnar_mode(load_config: LoadConfig) -> str:
    options = _columnar_fast_path_options(load_config)
    return str(options.get("mode") or "auto").strip().lower() if options else ""


def _object_storage_options(load_config: LoadConfig) -> dict[str, Any]:
    return _mapping(_columnar_fast_path_options(load_config).get("object_storage"))


def _columnar_fast_path_options(load_config: LoadConfig) -> dict[str, Any]:
    source_options = _source_options(load_config)
    native_transfer = _mapping(source_options.get("native_transfer"))
    snapshot = _mapping(native_transfer.get("snapshot"))
    return _mapping(snapshot.get("columnar_fast_path") or source_options.get("columnar_fast_path"))


def _columnar_pull_options(load_config: LoadConfig) -> dict[str, Any]:
    bulk = _mapping(_sink_options(load_config).get("clickhouse_bulk"))
    return _mapping(bulk.get("columnar_pull"))


def _configured_run_id(load_config: LoadConfig) -> str:
    return str(load_config.options.get("run_id") or "capability-probe")


def _source_options(load_config: LoadConfig) -> dict[str, Any]:
    namespaced = _mapping(load_config.options.get("source_options"))
    return namespaced or load_config.options


def _sink_options(load_config: LoadConfig) -> dict[str, Any]:
    namespaced = _mapping(load_config.options.get("sink_options"))
    return namespaced or load_config.options


def _database(schema: str, database: str | None) -> str | None:
    return None if "." in str(schema) else database


def _schema_label(schema: str, database: str | None) -> str:
    if database and "." not in str(schema):
        return f"{database}.{schema}"
    return str(schema)


def _mapping(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "ColumnarRuntimeAssembly",
    "MssqlColumnarSnapshotRequestFactory",
    "RouteRuntimeAssembly",
]
