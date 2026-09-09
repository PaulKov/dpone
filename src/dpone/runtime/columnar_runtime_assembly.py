"""Production assembly for columnar route capability runtime."""

from __future__ import annotations

from collections.abc import Sequence
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
from dpone.runtime.sources.strategies.mssql.mssql_columnar_provider import MssqlColumnarSnapshotProvider
from dpone.runtime.sources.strategies.mssql.mssql_columnar_queryout_bridge import build_columnar_snapshot_request
from dpone.storage import ObjectStorageUri

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_SECRET_MARKERS = ("password=", "pwd=", "secret=", "token=", "access_key=")


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
    ) -> None:
        self._object_client = object_client
        self._resolver = object_storage_resolver or ObjectStorageConnectionResolver()
        self._parquet_writer = parquet_writer
        self._clickhouse_probe_connector = clickhouse_probe_connector

    def build(self, *, load_config: LoadConfig, source: Any, sink: Any) -> RouteRuntimeAssembly | None:
        if _columnar_mode(load_config) in {"off", "benchmark_only", ""}:
            return None
        request_factory = MssqlColumnarSnapshotRequestFactory()
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
            candidate_provider=ColumnarRouteCandidateProvider(include_direct_push=True),
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
    def __call__(self, *, load_config: LoadConfig, source: Any, sink: Any, state: Any, load_record: Any) -> Any:
        del sink, state
        connector = getattr(source, "connector", None)
        schema = _fetch_schema(connector, load_config)
        query = _select_query(connector, load_config, [column for column, _ in schema])
        return build_columnar_snapshot_request(
            load_config=load_config,
            query=query,
            schema=schema,
            run_id=str(getattr(load_record, "run_id", "") or _configured_run_id(load_config)),
        )


class ColumnarRouteDecisionDetailsProvider:
    def __call__(self, *, load_config: LoadConfig, source: Any, sink: Any, load_record: Any) -> dict[str, object]:
        del source, sink, load_record
        options = _columnar_fast_path_options(load_config)
        if not options:
            return {}
        policy = resolve_columnar_execution_policy(options)
        return {
            "execution_mode": policy.value,
            "execution_requested": policy.requested,
            "execution_deprecated_alias": policy.deprecated_alias,
            "cleanup_policy": policy.cleanup_policy,
        }


class _SourceCapabilityProbe:
    def __init__(self, request_factory: MssqlColumnarSnapshotRequestFactory, provider: Any) -> None:
        self._request_factory = request_factory
        self._provider = provider

    def __call__(self, **context: Any) -> Any:
        try:
            request = _request_from_context(self._request_factory, context)
        except Exception as exc:
            from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotCapability

            return ColumnarSnapshotCapability(
                provider_id=getattr(self._provider, "provider_id", "columnar_snapshot_provider"),
                certified=False,
                blockers=(f"source.columnar_request_failed:{type(exc).__name__}",),
                details={
                    "exception": type(exc).__name__,
                    "message": _redact_exception_message(exc),
                },
            )
        return self._provider.capabilities(request)


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


def _request_from_context(factory: MssqlColumnarSnapshotRequestFactory, context: dict[str, Any]) -> Any:
    return factory(
        load_config=context["load_config"],
        source=context["source"],
        sink=context["sink"],
        state=None,
        load_record=context.get("load_record"),
    )


def _fetch_schema(connector: Any, load_config: LoadConfig) -> list[tuple[str, str]]:
    if connector is None or not hasattr(connector, "fetch_schema"):
        raise RuntimeError("mssql_source_schema_reader_missing")
    database = _database(load_config.source_schema, load_config.source_database)
    try:
        return list(connector.fetch_schema(load_config.source_schema, load_config.source_table, database=database))
    except TypeError:
        return list(
            connector.fetch_schema(_schema_label(load_config.source_schema, database), load_config.source_table)
        )


def _select_query(connector: Any, load_config: LoadConfig, columns: list[str]) -> str:
    if connector is None or not hasattr(connector, "build_select_query"):
        raise RuntimeError("mssql_source_query_builder_missing")
    database = _database(load_config.source_schema, load_config.source_database)
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


def _redact_exception_message(exc: Exception) -> str:
    message = str(exc)
    if not message:
        return ""
    lowered = message.lower()
    if any(marker in lowered for marker in _SECRET_MARKERS):
        return "***REDACTED***"
    return message[:500]


__all__ = [
    "ColumnarRuntimeAssembly",
    "MssqlColumnarSnapshotRequestFactory",
    "RouteRuntimeAssembly",
]
