"""Governed lifecycle facade for external-replication publication."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any, Protocol

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.ports.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalArtifactSourcePort,
    ExternalPublicationError,
    ExternalPublicationRequest,
    digest_payload,
)
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.sinks.clickhouse_external_replication_context import (
    ExternalStagedContext,
    ExternalStagedValidation,
)
from dpone.runtime.sinks.clickhouse_external_replication_runtime import (
    ClickHouseExternalReplicationRuntime,
    ExternalReplicationReceipt,
    ExternalReplicationRuntimeService,
)
from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult

EXTERNAL_REPLAY_OPTION = "__dpone_clickhouse_external_replication_replay_v1"


class ExternalRuntimeFactory(Protocol):
    """Construct one coordinator around an invocation-scoped service."""

    def __call__(
        self,
        *,
        service: ExternalReplicationRuntimeService,
        artifact_source: ExternalArtifactSourcePort | None = None,
    ) -> ClickHouseExternalReplicationRuntime: ...


class ExternalServiceFactory(Protocol):
    """Build target services, optionally with payload capabilities for staging."""

    def __call__(
        self,
        cluster: str,
        database: str,
        target: str,
        *,
        load_config: Any | None = None,
        payload: Any | None = None,
    ) -> ExternalReplicationRuntimeService: ...


ExternalArtifactSourceFactory = Callable[[Any, Any], ExternalArtifactSourcePort]


class ClickHouseExternalReplicationFacade:
    """Bridge generic staged-load boundaries to the fenced external runtime.

    Factories retain connectors and artifact locations inside composition.  The
    staged context contains only the immutable request and redacted receipt.
    """

    def __init__(
        self,
        *,
        service_factory: ExternalServiceFactory,
        artifact_source_factory: ExternalArtifactSourceFactory,
        runtime_factory: ExternalRuntimeFactory = ClickHouseExternalReplicationRuntime,
    ) -> None:
        self._service_factory = service_factory
        self._artifact_source_factory = artifact_source_factory
        self._runtime_factory = runtime_factory

    @staticmethod
    def is_enabled(load_config: Any) -> bool:
        """Return whether the manifest explicitly selected external mode."""

        if getattr(load_config, "load_strategy", None) is not LoadStrategy.FULL_REFRESH:
            return False
        cluster = _cluster_options(load_config)
        return str(cluster.get("replication_mode") or "internal").strip().lower() == "external"

    def prepare_admission(self, load_config: Any) -> Any:
        """Acquire or resume Keeper authority before source extraction."""

        if not self.is_enabled(load_config):
            return load_config
        cluster, database, target = _target_identity(load_config)
        service = self._service_factory(cluster, database, target, load_config=load_config)
        runtime = self._runtime_factory(service=service)
        state = runtime.prepare(
            cluster=cluster,
            database=database,
            target=target,
            scheduler_invocation=_scheduler_identity(load_config),
            plan_sha256=_plan_digest(load_config, cluster=cluster, database=database, target=target),
        )
        if state.get("phase") != "COMPLETED":
            return load_config
        scope = str(getattr(service, "evidence_scope", "local_synthetic"))
        receipt = ExternalReplicationReceipt.from_state(dict(state), evidence_scope=scope)
        options = dict(_options(load_config))
        options[EXTERNAL_REPLAY_OPTION] = _load_result(
            receipt, staged_rows=int(state["artifact_row_count"]), replay=True
        )
        return replace(load_config, options=options)

    def stage(self, load_config: Any, payload: Any) -> ExternalStagedContext:
        """Seal and directly stage one immutable generation on every member."""

        if not self.is_enabled(load_config):
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_MODE_REQUIRED")
        _require_safe_transformations(load_config, payload)
        cluster, database, target = _target_identity(load_config)
        plan_sha256 = _plan_digest(load_config, cluster=cluster, database=database, target=target)
        self._runtime(
            cluster,
            database,
            target,
            load_config=load_config,
            payload=payload,
        ).prepare(
            cluster=cluster,
            database=database,
            target=target,
            scheduler_invocation=_scheduler_identity(load_config),
            plan_sha256=plan_sha256,
        )
        source = self._artifact_source_factory(load_config, payload)
        request = ExternalPublicationRequest(
            cluster=cluster,
            database=database,
            target=target,
            scheduler_invocation=_scheduler_identity(load_config),
            plan_sha256=plan_sha256,
            artifact=_artifact_receipt(source),
        )
        runtime = self._runtime(
            cluster,
            database,
            target,
            source=source,
            load_config=load_config,
            payload=payload,
        )
        receipt = runtime.stage(request)
        if receipt.phase != "STAGED":
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_INCOMPLETE")
        state = self._service_factory(cluster, database, target, load_config=load_config).read_authority(
            request.target_key
        )
        if state is None or state.get("operation_id") != request.operation_id:
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT")
        assert state is not None
        return ExternalStagedContext(
            request=request,
            staged_receipt=receipt,
            candidate_name=str(state["candidate_name"]),
        )

    def validate(self, context: ExternalStagedContext) -> ExternalStagedValidation:
        """Bind validation to the exact staged generation and authority version."""

        receipt = context.staged_receipt
        request = context.request
        validated = self._runtime(request.cluster, request.database, request.target).validate_staged(request, receipt)
        if validated != receipt:
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_VALIDATION_INVALID")
        return ExternalStagedValidation(
            operation_id=receipt.operation_id,
            generation_id=receipt.generation_id,
            authority_version=receipt.authority_version,
        )

    def publish(
        self,
        context: ExternalStagedContext,
        validation: ExternalStagedValidation,
    ) -> ExternalReplicationReceipt:
        """Publish only the exact validated staged authority."""

        expected = self.validate(context)
        if validation != expected:
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_VALIDATION_INVALID")
        request = context.request
        receipt = self._runtime(request.cluster, request.database, request.target).publish(request)
        if receipt.phase != "COMMITTED":
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS")
        return receipt

    def cleanup(self, context: ExternalStagedContext) -> ExternalReplicationReceipt:
        """Finish separately fenced predecessor cleanup."""

        request = context.request
        receipt = self._runtime(request.cluster, request.database, request.target).cleanup(request)
        if receipt.phase != "COMPLETED":
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_IN_PROGRESS")
        return receipt

    @staticmethod
    def publication_result(receipt: ExternalReplicationReceipt, *, staged_rows: int) -> LoadResult:
        """Project a committed external receipt into the connector-neutral result."""

        if receipt.phase != "COMMITTED":
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS")
        return _load_result(receipt, staged_rows=staged_rows)

    def abort(self, context: ExternalStagedContext) -> None:
        """Abort only an unpublished operation through exact-owned cleanup."""

        request = context.request
        self._runtime(request.cluster, request.database, request.target).abort(request)

    @staticmethod
    def replay_result(load_config: Any) -> LoadResult | None:
        """Return a previously completed same-operation receipt, if present."""

        result = _options(load_config).get(EXTERNAL_REPLAY_OPTION)
        if result is None:
            return None
        if not isinstance(result, LoadResult):
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_RECEIPT_INVALID")
        return result

    def _runtime(
        self,
        cluster: str,
        database: str,
        target: str,
        *,
        source: ExternalArtifactSourcePort | None = None,
        load_config: Any | None = None,
        payload: Any | None = None,
    ) -> ClickHouseExternalReplicationRuntime:
        service = self._service_factory(
            cluster,
            database,
            target,
            load_config=load_config,
            payload=payload,
        )
        return self._runtime_factory(service=service, artifact_source=source)


def _artifact_receipt(source: ExternalArtifactSourcePort) -> ExternalArtifactReceipt:
    identity = source.identity
    receipt = ExternalArtifactReceipt(
        artifact_id=source.binding_id,
        sha256=identity.sha256,
        byte_size=identity.byte_size,
        row_count=identity.row_count,
        schema_sha256=identity.schema_digest,
        content_sha256=identity.wire_digest,
        replayable=True,
    )
    receipt.validate()
    return receipt


def _load_result(
    receipt: ExternalReplicationReceipt,
    *,
    staged_rows: int,
    replay: bool = False,
) -> LoadResult:
    return LoadResult(
        inserted_rows=staged_rows,
        updated_rows=0,
        total_rows=staged_rows,
        staging_rows=staged_rows,
        commit_receipt_id=receipt.operation_id,
        commit_outcome=(AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE if replay else AtomicCommitOutcome.COMMITTED),
        reconciliation_metrics={"clickhouse_cluster_external_full_refresh": receipt.to_dict()},
    )


def _target_identity(load_config: Any) -> tuple[str, str, str]:
    cluster = str(_cluster_options(load_config).get("name") or "").strip()
    database = str(getattr(load_config, "target_schema", "") or "").strip()
    target = str(getattr(load_config, "target_table", "") or "").strip()
    if not all((cluster, database, target)):
        _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_REQUEST_INVALID")
    return cluster, database, target


def _scheduler_identity(load_config: Any) -> str:
    identity = str(_options(load_config).get(SCHEDULER_IDENTITY_OPTION) or "").strip()
    if not identity:
        _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_IDENTITY_REQUIRED")
    return identity


def _plan_digest(load_config: Any, *, cluster: str, database: str, target: str) -> str:
    options = _options(load_config)
    clickhouse = _clickhouse_options(load_config)
    cluster_options = _cluster_options(load_config)
    strategy = getattr(load_config, "load_strategy", None)
    return digest_payload(
        {
            "version": 1,
            "strategy": str(getattr(strategy, "value", strategy) or ""),
            "cluster": cluster,
            "database": database,
            "target": target,
            "staging_database": str(getattr(load_config, "staging_schema", "") or ""),
            "engine": str(clickhouse.get("engine") or "MergeTree"),
            "ddl_scope": str(cluster_options.get("ddl_scope") or "local"),
            "replication_mode": str(cluster_options.get("replication_mode") or "internal"),
            "max_source_bytes": options.get(SOURCE_BYTE_BUDGET_OPTION),
        }
    )


def _require_safe_transformations(load_config: Any, payload: Any) -> None:
    if LineageOptions.from_config(_options(load_config).get("lineage")).enabled:
        _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_TRANSFORMATION_UNSUPPORTED")
    artifacts = (getattr(payload, "artifact", None),)
    partitions = tuple(getattr(artifacts[0], "partitions", ()) or ())
    for artifact in (*artifacts, *partitions):
        codec = getattr(artifact, "bulk_text_codec", None)
        if codec is not None and hasattr(codec, "clickhouse_decode_expression"):
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_TRANSFORMATION_UNSUPPORTED")


def _options(load_config: Any) -> Mapping[str, Any]:
    raw = getattr(load_config, "options", None)
    return raw if isinstance(raw, Mapping) else {}


def _clickhouse_options(load_config: Any) -> Mapping[str, Any]:
    physical = _options(load_config).get("physical_design")
    physical = physical if isinstance(physical, Mapping) else {}
    storage = physical.get("storage")
    storage = storage if isinstance(storage, Mapping) else {}
    clickhouse = storage.get("clickhouse")
    return clickhouse if isinstance(clickhouse, Mapping) else {}


def _cluster_options(load_config: Any) -> Mapping[str, Any]:
    cluster = _clickhouse_options(load_config).get("cluster")
    return cluster if isinstance(cluster, Mapping) else {}


def _fail(code: str) -> None:
    raise ExternalPublicationError(code)


__all__ = [
    "EXTERNAL_REPLAY_OPTION",
    "ClickHouseExternalReplicationFacade",
    "ExternalArtifactSourceFactory",
    "ExternalRuntimeFactory",
    "ExternalServiceFactory",
]
