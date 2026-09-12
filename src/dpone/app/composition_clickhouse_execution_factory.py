"""Compose the MSSQL→ClickHouse worker from verified parent authority."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from dpone.adapters.composition_clickhouse_admin import ClickHousePrincipalHttpClient
from dpone.adapters.composition_clickhouse_dispatch_store import FileSnapshotPublicationStore
from dpone.adapters.composition_clickhouse_gate import ClickHouseSupervisorObserver, MssqlClickHouseGate
from dpone.adapters.composition_clickhouse_principal import ClickHousePrincipalAdmin
from dpone.adapters.composition_clickhouse_supervisor import DockerClickHouseLocalSupervisor
from dpone.adapters.composition_clickhouse_supervisor_docker import LocalDockerSupervisorClient
from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    read_service_enrollment,
    require_attempt_enrollment_original,
)
from dpone.adapters.composition_clickhouse_supervisor_linux import LinuxSupervisorProbe
from dpone.adapters.composition_clickhouse_transport import ClickHouseDispatchTransport
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore, composition_control_transaction
from dpone.app.composition_clickhouse_catalog import ClickHouseHttpSnapshotCatalog
from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionDependencies,
    CompositionClickHouseExecutionRequest,
    CompositionClickHouseExecutionRoot,
)
from dpone.app.composition_clickhouse_outcome import (
    CompositionClickHouseOutcomeObserver,
    publication_state,
)
from dpone.app.composition_clickhouse_publication import (
    ClickHouseDispatchBudgetPolicy,
    ClickHouseHttpSnapshotExecutor,
    ObservingSnapshotPublicationAuthority,
    clickhouse_http_endpoint,
    clickhouse_plan_write,
    snapshot_limits_from_manifest,
    snapshot_target_for_write,
)
from dpone.app.composition_clickhouse_source import MssqlClickHouseSourceReader
from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.app.composition_pack_cache import cache_root_from_environment
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
from dpone.contracts.composition_snapshot import SnapshotGeneration
from dpone.contracts.strict_json import strict_json_object
from dpone.runtime.composition_snapshot import ClickHouseAtomicSnapshotPublisher
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.deployment_cache_common import require_path_without_symlinks

SUPERVISOR_ROOT_ENV = "DPONE_COMPOSITION_SUPERVISOR_ROOT"
_DEFAULT_SUPERVISOR_ROOT = "/var/lib/dpone/composition"


def build_composition_clickhouse_execution_dependencies(
    *,
    control: CompositionDbtControlAuthority,
    read_active: Any,
    gate: Any,
    publisher_gate: Any,
    publisher: Any,
    bind_transport: Any,
    read_source: Any,
    require_enrollment: Any,
    outcome_observer: Any,
    target: Any,
    attach_publisher_transport: Any | None = None,
    can_classify_publication: bool = False,
) -> CompositionClickHouseExecutionDependencies:
    """Return protected collaborators of one supervised ClickHouse cell."""

    control.__post_init__()
    if not callable(bind_transport) or not callable(read_source) or not callable(require_enrollment):
        raise CompositionAdmissionError("clickhouse_transport_binding")
    if any(item is None for item in (gate, publisher_gate, publisher, outcome_observer, target)):
        raise CompositionAdmissionError("clickhouse_publisher")
    return CompositionClickHouseExecutionDependencies(
        read_active=read_active,
        attempts=MssqlCompositionAttemptStore(
            control.connection_factory,
            expected_service_id=control.expected_service_id,
            control_schema=control.control_schema,
        ),
        gate=gate,
        publisher_gate=publisher_gate,
        publisher=publisher,
        bind_transport=bind_transport,
        read_source=read_source,
        require_enrollment=require_enrollment,
        outcome_observer=outcome_observer,
        target=target,
        expected_service_id=control.expected_service_id,
        attach_publisher_transport=attach_publisher_transport,
        can_classify_publication=can_classify_publication,
    )


def build_composition_clickhouse_execution_root(
    **kwargs: Any,
) -> CompositionClickHouseExecutionRoot:
    """Construct the ClickHouse root from verified parent authority."""

    dependencies = kwargs.get("dependencies")
    if not isinstance(dependencies, CompositionClickHouseExecutionDependencies):
        dependencies = build_composition_clickhouse_execution_dependencies(**kwargs)
    return CompositionClickHouseExecutionRoot(dependencies)


def load_sealed_clickhouse_snapshot(cache_root: Path, release_id: str, workload_id: str) -> dict[str, Any]:
    """Reopen a producer-sealed generation original; inventing columns is rejected."""

    if not is_canonical_sha256_digest(release_id) or not workload_id or "/" in workload_id or "\\" in workload_id:
        raise CompositionAdmissionError("clickhouse_source_payload")
    root = Path(cache_root) / "releases" / release_id.replace(":", "-") / "composition-snapshots"
    path = root / f"{workload_id}.json"
    try:
        require_path_without_symlinks(path, root=Path(cache_root), error_path=path)
        body = strict_json_object(path.read_bytes())
        ref = body.get("generation_ref")
        uuid = body.get("generation_uuid")
        raw_columns = body.get("columns")
        if (
            not is_canonical_sha256_digest(ref)
            or str(UUID(str(uuid))) != uuid
            or not isinstance(raw_columns, list)
            or not raw_columns
        ):
            raise CompositionAdmissionError("clickhouse_source_payload")
        columns = tuple(ClickHouseDispatchColumn(str(row["name"]), str(row["type_name"])) for row in raw_columns)
        generation = body.get("generation")
        if generation is not None:
            generation = SnapshotGeneration(**generation)
            generation.__post_init__()
            if generation.record_sha256 != ref or generation.new_generation_uuid != uuid:
                raise CompositionAdmissionError("clickhouse_source_payload")
    except CompositionAdmissionError:
        raise
    except Exception:
        raise CompositionAdmissionError("clickhouse_source_payload") from None
    return {"generation_ref": ref, "generation_uuid": uuid, "columns": columns, "generation": generation}


def clickhouse_execution_request(
    request: Any,
    manifest: Mapping[str, Any],
    *,
    plan_sha256: str,
    cache_root: Path,
) -> CompositionClickHouseExecutionRequest:
    """Bind scheduler identity to a sealed ClickHouse generation original."""

    from dpone.contracts.dbt_runtime import (
        AIRFLOW_RUN_IDENTITY_ENV,
        airflow_attempt_from_environment,
        parse_airflow_run_identity_json,
    )

    if not is_canonical_sha256_digest(plan_sha256):
        raise CompositionAdmissionError("clickhouse_source_payload")
    identity = parse_airflow_run_identity_json(str(request.env.get(AIRFLOW_RUN_IDENTITY_ENV) or ""))
    attempt = airflow_attempt_from_environment(request.env, identity)
    snapshot = load_sealed_clickhouse_snapshot(cache_root, identity.release_id, str(manifest.get("name") or ""))
    return CompositionClickHouseExecutionRequest(
        manifest=manifest,
        plan_sha256=plan_sha256,
        run_identity=identity,
        airflow_attempt=attempt,
        generation_ref=snapshot["generation_ref"],
        generation_uuid=snapshot["generation_uuid"],
        columns=snapshot["columns"],
    )


def compose_clickhouse_pack_dependencies(
    *,
    parent: Mapping[str, Any],
    manifest: Mapping[str, Any],
    environment: Mapping[str, str],
    plan: Any,
) -> CompositionClickHouseExecutionDependencies | None:
    """Compose protected ClickHouse collaborators, or omit the root fail-closed."""

    try:
        return _compose_clickhouse_pack_dependencies(parent, manifest, environment, plan)
    except (CompositionAdmissionError, OSError, TypeError, ValueError, KeyError, AttributeError):
        return None


def snapshot_store_root(environment: Mapping[str, str]) -> Path:
    """Return the supervisor PVC snapshot CAS directory."""

    raw = environment.get(SUPERVISOR_ROOT_ENV) or _DEFAULT_SUPERVISOR_ROOT
    return Path(raw) / "snapshots"


def _compose_clickhouse_pack_dependencies(
    parent: Mapping[str, Any],
    manifest: Mapping[str, Any],
    environment: Mapping[str, str],
    plan: Any,
) -> CompositionClickHouseExecutionDependencies:
    if parent.get("context") is None or parent.get("resolver") is None:
        raise CompositionAdmissionError("clickhouse_enrollment")
    control = parent["control"]
    write = clickhouse_plan_write(plan, manifest)
    source = manifest.get("source")
    if not isinstance(source, Mapping) or not isinstance(source.get("connection_ref"), str):
        raise CompositionAdmissionError("clickhouse_source_payload")
    with composition_control_transaction(
        control.connection_factory, control.control_schema, control.expected_service_id
    ) as ledger:
        service_id = parent["target"].descriptor.properties.get("composition_service_id")
        enrollment = read_service_enrollment(ledger, str(service_id))
    target = snapshot_target_for_write(enrollment, write)
    limits = snapshot_limits_from_manifest(manifest)
    endpoint, admin_credentials, ca_file = clickhouse_http_endpoint(parent["target"])
    cache = cache_root_from_environment(environment)
    release_id = parent["context"].release_id
    snapshot = load_sealed_clickhouse_snapshot(cache, release_id, str(manifest.get("name") or write.resource_id))
    store = FileSnapshotPublicationStore(snapshot_store_root(environment))
    admin = ClickHousePrincipalAdmin(
        ClickHousePrincipalHttpClient(
            endpoint=endpoint, credentials=admin_credentials, timeout_seconds=30.0, ca_file=ca_file
        )
    )
    supervisor = cast(
        ClickHouseSupervisorObserver,
        DockerClickHouseLocalSupervisor(
            enrollment_sha256=enrollment.enrollment_sha256,
            docker=LocalDockerSupervisorClient(),
            linux=LinuxSupervisorProbe(),
        ),
    )
    policy = ClickHouseDispatchBudgetPolicy(limits)
    gate = MssqlClickHouseGate(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        target=target,
        purpose="ingest",
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
        enrollment_sha256=enrollment.enrollment_sha256,
        control_schema=control.control_schema,
    )
    publisher_gate = MssqlClickHouseGate(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        target=target,
        purpose="publisher",
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
        enrollment_sha256=enrollment.enrollment_sha256,
        control_schema=control.control_schema,
    )
    executor = ClickHouseHttpSnapshotExecutor()

    def load_generation(attempt: Any, generation_ref: str) -> Any:
        del attempt
        generation = snapshot["generation"]
        if generation is None or generation.record_sha256 != generation_ref:
            raise CompositionAdmissionError("snapshot_prepared_subject")
        return generation

    authority = ObservingSnapshotPublicationAuthority(
        read_active=parent["read_active"],
        connection_factory=control.connection_factory,
        control_schema=control.control_schema,
        target=target,
        limits=limits,
        enrollment_sha256=enrollment.enrollment_sha256,
        load_generation=load_generation,
        publisher_gate=publisher_gate,
    )
    catalog = ClickHouseHttpSnapshotCatalog(
        endpoint=endpoint,
        credentials=admin_credentials,
        timeout_seconds=30.0,
        max_response_bytes=1024 * 1024,
        ca_file=ca_file,
    )
    publisher = ClickHouseAtomicSnapshotPublisher(
        authority=authority,
        store=store,
        catalog=catalog,
        executor=executor,
    )

    def bind_transport(credentials: Any, journal: Any) -> ClickHouseDispatchTransport:
        return ClickHouseDispatchTransport(
            endpoint=endpoint, credentials=credentials, journal=journal, timeout_seconds=60.0, ca_file=ca_file
        )

    def require_enrollment(attempt: Any, phase: str) -> bytes:
        if phase not in {"dispatch", "publication"}:
            raise CompositionAdmissionError("clickhouse_enrollment")
        with composition_control_transaction(
            control.connection_factory, control.control_schema, control.expected_service_id
        ) as ledger:
            original = require_attempt_enrollment_original(ledger, enrollment.enrollment_sha256, attempt, target)
        return original.document

    def read_publication_state(attempt: Any) -> str:
        rows = [row for row in store.records() if row.intent.attempt.attempt_sha256 == attempt.attempt_sha256]
        return publication_state(rows[-1]) if rows else "COMMIT_UNKNOWN"

    def read_authorities(attempt: Any) -> Any:
        return (authority.ingest_authority(attempt),)

    return build_composition_clickhouse_execution_dependencies(
        control=control,
        read_active=parent["read_active"],
        gate=gate,
        publisher_gate=publisher_gate,
        publisher=publisher,
        bind_transport=bind_transport,
        read_source=MssqlClickHouseSourceReader(
            open_connection=lambda: (
                ResolvedConnectorFactory.create(
                    parent["resolver"].resolve(str(source["connection_ref"])), autocommit=True
                ).connection
            ),
            table=source["table"] if isinstance(source.get("table"), Mapping) else {},
            columns=snapshot["columns"],
        ),
        require_enrollment=require_enrollment,
        outcome_observer=CompositionClickHouseOutcomeObserver(
            transaction=lambda: composition_control_transaction(
                control.connection_factory, control.control_schema, control.expected_service_id
            ),
            service_id=target.service_id,
            read_publication_state=read_publication_state,
            read_authorities=read_authorities,
        ),
        target=target,
        attach_publisher_transport=executor.attach,
        can_classify_publication=catalog.can_classify_publication(),
    )


__all__ = [
    "SUPERVISOR_ROOT_ENV",
    "build_composition_clickhouse_execution_dependencies",
    "build_composition_clickhouse_execution_root",
    "clickhouse_execution_request",
    "compose_clickhouse_pack_dependencies",
    "load_sealed_clickhouse_snapshot",
    "snapshot_store_root",
]
