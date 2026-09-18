"""Governed lifecycle facade for external-replication publication."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any, Protocol

from dpone.config.load_strategy import LoadStrategy
from dpone.ports.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalArtifactSourcePort,
    ExternalPublicationRequest,
)
from dpone.runtime.sinks.clickhouse_external_artifact_source import (
    ClickHouseExternalArtifactSource,
    external_artifact_store_root,
    external_content_row_budget,
)
from dpone.runtime.sinks.clickhouse_external_replication_context import (
    ExternalStagedContext,
    ExternalStagedValidation,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    EXTERNAL_ADMISSION_OPTION,
    EXTERNAL_REPLAY_OPTION,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    artifact_receipt as _artifact_receipt,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    cluster_options as _cluster_options,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    fail as _fail,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    load_result as _load_result,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    options as _options,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    plan_digest as _plan_digest,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    read_authority as _read_authority,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    request_from_state as _request_from_state,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    require_safe_transformations as _require_safe_transformations,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    scheduler_identity as _scheduler_identity,
)
from dpone.runtime.sinks.clickhouse_external_replication_facade_support import (
    target_identity as _target_identity,
)
from dpone.runtime.sinks.clickhouse_external_replication_runtime import (
    ClickHouseExternalReplicationRuntime,
    ExternalReplicationReceipt,
    ExternalReplicationRuntimeService,
)
from dpone.runtime.sinks.load_result import LoadResult


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
        maximum_rows: int | None = None,
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
        _require_safe_transformations(load_config, None)
        cluster, database, target = _target_identity(load_config)
        service = self._service_factory(cluster, database, target, load_config=load_config)
        runtime = self._runtime_factory(service=service)
        scheduler_invocation = _scheduler_identity(load_config)
        plan_sha256 = _plan_digest(load_config, cluster=cluster, database=database, target=target)
        state = runtime.prepare(
            cluster=cluster,
            database=database,
            target=target,
            scheduler_invocation=scheduler_invocation,
            plan_sha256=plan_sha256,
        )
        options = dict(_options(load_config))
        options[EXTERNAL_ADMISSION_OPTION] = {
            "operation_id": str(state["operation_id"]),
            "target_key": str(state["target_key"]),
            "inventory_digest": str(state["inventory_digest"]),
            "phase": str(state["phase"]),
            "plan_sha256": plan_sha256,
        }
        load_config = replace(load_config, options=options)
        phase = str(state.get("phase") or "")
        if phase in {"STAGING", "STAGED", "PUBLICATION_DISPATCHING", "COMMITTED", "CLEANUP_DISPATCHING"}:
            request = _request_from_state(
                state,
                cluster=cluster,
                database=database,
                target=target,
                scheduler_invocation=scheduler_invocation,
                plan_sha256=plan_sha256,
            )
            expected = ExternalReplicationReceipt.from_state(
                dict(state), evidence_scope=str(getattr(service, "evidence_scope", "local_synthetic"))
            )
            if phase == "STAGING":
                source = ClickHouseExternalArtifactSource.reopen(
                    root=external_artifact_store_root(load_config),
                    binding_id=request.artifact.artifact_id,
                    expected=request.artifact.identity,
                )
                runtime = self._runtime(
                    cluster,
                    database,
                    target,
                    source=source,
                    load_config=load_config,
                    payload=source.open_replay(),
                )
                runtime.stage(request)
                source.release(external_artifact_store_root(load_config))
                state = _read_authority(service, request.target_key)
                phase = str(state.get("phase") or "")
                expected = ExternalReplicationReceipt.from_state(
                    dict(state), evidence_scope=str(getattr(service, "evidence_scope", "local_synthetic"))
                )
            if phase == "STAGED":
                runtime.validate_staged(request, expected)
            if phase in {"STAGED", "PUBLICATION_DISPATCHING"}:
                runtime.publish(request)
            state = _read_authority(service, request.target_key)
            if state.get("phase") in {"COMMITTED", "CLEANUP_DISPATCHING"}:
                runtime.cleanup(request)
            state = _read_authority(service, request.target_key)
        if state.get("phase") != "COMPLETED":
            return load_config
        scope = str(getattr(service, "evidence_scope", "local_synthetic"))
        receipt = ExternalReplicationReceipt.from_state(dict(state), evidence_scope=scope)
        options = dict(_options(load_config))
        options[EXTERNAL_REPLAY_OPTION] = _load_result(
            receipt, staged_rows=int(state["artifact_row_count"]), replay=True
        )
        return replace(load_config, options=options)

    def require_preflight(self, load_config: Any) -> None:
        """Fail closed unless this exact external plan completed admission."""

        cluster, database, target = _target_identity(load_config)
        plan_sha256 = _plan_digest(load_config, cluster=cluster, database=database, target=target)
        admission = _options(load_config).get(EXTERNAL_ADMISSION_OPTION)
        if not isinstance(admission, Mapping):
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ADMISSION_REQUIRED")
        expected_target = ExternalPublicationRequest(
            cluster=cluster,
            database=database,
            target=target,
            scheduler_invocation=_scheduler_identity(load_config),
            plan_sha256=plan_sha256,
            artifact=ExternalArtifactReceipt(
                artifact_id="preflight",
                sha256="0" * 64,
                byte_size=0,
                row_count=0,
                schema_sha256="0" * 64,
                content_sha256="0" * 64,
                replayable=True,
            ),
        )
        if (
            admission.get("operation_id") != expected_target.operation_id
            or admission.get("target_key") != expected_target.target_key
            or admission.get("plan_sha256") != plan_sha256
            or admission.get("phase") not in {"LOCKED", "STAGING", "STAGED", "COMPLETED"}
        ):
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ADMISSION_REQUIRED")

    def stage(self, load_config: Any, payload: Any) -> ExternalStagedContext:
        """Seal and directly stage one immutable generation on every member."""

        if not self.is_enabled(load_config):
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_MODE_REQUIRED")
        cluster, database, target = _target_identity(load_config)
        plan_sha256 = _plan_digest(load_config, cluster=cluster, database=database, target=target)
        prepared_runtime = self._runtime(
            cluster,
            database,
            target,
            load_config=load_config,
            payload=payload,
        )
        prepared_runtime.prepare(
            cluster=cluster,
            database=database,
            target=target,
            scheduler_invocation=_scheduler_identity(load_config),
            plan_sha256=plan_sha256,
        )
        try:
            _require_safe_transformations(load_config, payload)
            source = self._artifact_source_factory(load_config, payload)
            if isinstance(source, ClickHouseExternalArtifactSource):
                source.persist(external_artifact_store_root(load_config))
        except Exception:
            prepared_runtime.abort_prepared(
                cluster=cluster,
                database=database,
                target=target,
                scheduler_invocation=_scheduler_identity(load_config),
                plan_sha256=plan_sha256,
            )
            raise
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
        if isinstance(source, ClickHouseExternalArtifactSource):
            source.release(external_artifact_store_root(load_config))
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
            content_row_budget=external_content_row_budget(load_config),
        )

    def validate(self, context: ExternalStagedContext) -> ExternalStagedValidation:
        """Bind validation to the exact staged generation and authority version."""

        receipt = context.staged_receipt
        request = context.request
        validated = self._runtime(
            request.cluster,
            request.database,
            request.target,
            maximum_rows=context.content_row_budget,
        ).validate_staged(request, receipt)
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
        receipt = self._runtime(
            request.cluster,
            request.database,
            request.target,
            maximum_rows=context.content_row_budget,
        ).publish(request)
        if receipt.phase != "COMMITTED":
            _fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_IN_PROGRESS")
        return receipt

    def cleanup(self, context: ExternalStagedContext) -> ExternalReplicationReceipt:
        """Finish separately fenced predecessor cleanup."""

        request = context.request
        receipt = self._runtime(
            request.cluster,
            request.database,
            request.target,
            maximum_rows=context.content_row_budget,
        ).cleanup(request)
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

    def abort_prepared_admission(self, load_config: Any) -> None:
        """Release this invocation's lock when extraction never reached staging."""

        if not self.is_enabled(load_config):
            return
        cluster, database, target = _target_identity(load_config)
        self._runtime(cluster, database, target, load_config=load_config).abort_prepared(
            cluster=cluster,
            database=database,
            target=target,
            scheduler_invocation=_scheduler_identity(load_config),
            plan_sha256=_plan_digest(load_config, cluster=cluster, database=database, target=target),
        )

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
        maximum_rows: int | None = None,
    ) -> ClickHouseExternalReplicationRuntime:
        kwargs = {"load_config": load_config, "payload": payload}
        if maximum_rows is not None:
            kwargs["maximum_rows"] = maximum_rows
        service = self._service_factory(cluster, database, target, **kwargs)
        return self._runtime_factory(service=service, artifact_source=source)


__all__ = [
    "EXTERNAL_REPLAY_OPTION",
    "EXTERNAL_ADMISSION_OPTION",
    "ClickHouseExternalReplicationFacade",
    "ExternalArtifactSourceFactory",
    "ExternalRuntimeFactory",
    "ExternalServiceFactory",
]
