"""Pure identity, result, and validation helpers for the external facade."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, NoReturn, Protocol

from dpone.ports.clickhouse_external_replication import (
    ExternalArtifactReceipt,
    ExternalArtifactSourcePort,
    ExternalPublicationError,
    ExternalPublicationRequest,
)
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.sinks.clickhouse_external_replication_context import derive_semantic_plan_digest
from dpone.runtime.sinks.clickhouse_external_replication_receipt import ExternalReplicationReceipt
from dpone.runtime.sinks.clickhouse_external_replication_runtime import (
    ClickHouseExternalReplicationRuntime,
    ExternalReplicationRuntimeService,
)
from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.support.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec

EXTERNAL_REPLAY_OPTION = "__dpone_clickhouse_external_replication_replay_v1"
EXTERNAL_ADMISSION_OPTION = "__dpone_clickhouse_external_replication_admission_v1"


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


def artifact_receipt(source: ExternalArtifactSourcePort) -> ExternalArtifactReceipt:
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


def load_result(
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


def target_identity(load_config: Any) -> tuple[str, str, str]:
    cluster = str(cluster_options(load_config).get("name") or "").strip()
    database = str(getattr(load_config, "target_schema", "") or "").strip()
    target = str(getattr(load_config, "target_table", "") or "").strip()
    if not all((cluster, database, target)):
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_REQUEST_INVALID")
    return cluster, database, target


def scheduler_identity(load_config: Any) -> str:
    identity = str(options(load_config).get(SCHEDULER_IDENTITY_OPTION) or "").strip()
    if not identity:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_IDENTITY_REQUIRED")
    return identity


def plan_digest(load_config: Any, *, cluster: str, database: str, target: str) -> str:
    return derive_semantic_plan_digest(
        load_config,
        cluster=cluster,
        database=database,
        target=target,
        runtime_option_keys=frozenset({SCHEDULER_IDENTITY_OPTION, EXTERNAL_REPLAY_OPTION, EXTERNAL_ADMISSION_OPTION}),
    )


def request_from_state(
    state: Mapping[str, Any],
    *,
    cluster: str,
    database: str,
    target: str,
    scheduler_invocation: str,
    plan_sha256: str,
) -> ExternalPublicationRequest:
    return ExternalPublicationRequest(
        cluster=cluster,
        database=database,
        target=target,
        scheduler_invocation=scheduler_invocation,
        plan_sha256=plan_sha256,
        artifact=ExternalArtifactReceipt(
            artifact_id=str(state["artifact_binding_id"]),
            sha256=str(state["artifact_sha256"]),
            byte_size=int(state["artifact_byte_size"]),
            row_count=int(state["artifact_row_count"]),
            schema_sha256=str(state["artifact_schema_sha256"]),
            content_sha256=str(state["artifact_content_sha256"]),
            replayable=True,
        ),
    )


def read_authority(service: ExternalReplicationRuntimeService, target_key: str) -> dict[str, Any]:
    state = service.read_authority(target_key)
    if state is None:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_AUTHORITY_CONFLICT")
    return dict(state)


def require_safe_transformations(load_config: Any, payload: Any) -> None:
    if LineageOptions.from_config(options(load_config).get("lineage")).enabled:
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_TRANSFORMATION_UNSUPPORTED")
    artifacts = (getattr(payload, "artifact", None),)
    partitions = tuple(getattr(artifacts[0], "partitions", ()) or ())
    for artifact in (*artifacts, *partitions):
        codec = getattr(artifact, "bulk_text_codec", None)
        if codec is not None and not isinstance(codec, ClickHouseTabSeparatedCodec):
            fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_TRANSFORMATION_UNSUPPORTED")


def require_preflight(load_config: Any) -> None:
    """Require the exact admission receipt for this external operation."""

    cluster, database, target = target_identity(load_config)
    plan_sha256 = plan_digest(load_config, cluster=cluster, database=database, target=target)
    admission = options(load_config).get(EXTERNAL_ADMISSION_OPTION)
    if not isinstance(admission, Mapping):
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ADMISSION_REQUIRED")
    expected = ExternalPublicationRequest(
        cluster=cluster,
        database=database,
        target=target,
        scheduler_invocation=scheduler_identity(load_config),
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
        admission.get("operation_id") != expected.operation_id
        or admission.get("target_key") != expected.target_key
        or admission.get("plan_sha256") != plan_sha256
        or admission.get("phase") not in {"LOCKED", "STAGING", "STAGED", "COMPLETED"}
    ):
        fail("DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_ADMISSION_REQUIRED")


def options(load_config: Any) -> Mapping[str, Any]:
    raw = getattr(load_config, "options", None)
    return raw if isinstance(raw, Mapping) else {}


def cluster_options(load_config: Any) -> Mapping[str, Any]:
    physical = options(load_config).get("physical_design")
    physical = physical if isinstance(physical, Mapping) else {}
    storage = physical.get("storage")
    storage = storage if isinstance(storage, Mapping) else {}
    clickhouse = storage.get("clickhouse")
    clickhouse = clickhouse if isinstance(clickhouse, Mapping) else {}
    cluster = clickhouse.get("cluster")
    return cluster if isinstance(cluster, Mapping) else {}


def fail(code: str) -> NoReturn:
    raise ExternalPublicationError(code)


__all__ = [
    "EXTERNAL_ADMISSION_OPTION",
    "EXTERNAL_REPLAY_OPTION",
    "artifact_receipt",
    "cluster_options",
    "fail",
    "load_result",
    "options",
    "plan_digest",
    "read_authority",
    "request_from_state",
    "require_preflight",
    "require_safe_transformations",
    "scheduler_identity",
    "target_identity",
]
