"""Compose the MSSQL→ClickHouse worker from verified parent authority."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from dpone.adapters.composition_clickhouse_gate import ClickHouseSupervisorObserver
from dpone.adapters.composition_clickhouse_supervisor import DockerClickHouseLocalSupervisor
from dpone.adapters.composition_clickhouse_supervisor_docker import LocalDockerSupervisorClient
from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    read_service_enrollment,
)
from dpone.adapters.composition_clickhouse_supervisor_linux import LinuxSupervisorProbe
from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.composition_snapshot_capture_store import ProtectedSnapshotFiles
from dpone.app.composition_clickhouse_cell_factory import (
    build_composition_clickhouse_execution_dependencies as build_composition_clickhouse_execution_dependencies,
)
from dpone.app.composition_clickhouse_cell_factory import (
    build_composition_clickhouse_execution_root as build_composition_clickhouse_execution_root,
)
from dpone.app.composition_clickhouse_cell_factory import (
    build_protected_clickhouse_cell,
)
from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionDependencies,
    CompositionClickHouseExecutionRequest,
    build_composition_clickhouse_attempt,
)
from dpone.app.composition_clickhouse_publication import (
    clickhouse_plan_write,
)
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
from dpone.contracts.composition_snapshot import SnapshotGeneration
from dpone.contracts.strict_json import strict_json_object
from dpone.runtime.deployment_cache_common import require_path_without_symlinks

SUPERVISOR_ROOT_ENV = "DPONE_COMPOSITION_SUPERVISOR_ROOT"
_DEFAULT_SUPERVISOR_ROOT = "/var/lib/dpone/composition"


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
    """Bind scheduler identity; source originals are produced by runtime capture."""

    from dpone.contracts.dbt_runtime import (
        AIRFLOW_RUN_IDENTITY_ENV,
        airflow_attempt_from_environment,
        parse_airflow_run_identity_json,
    )

    if not is_canonical_sha256_digest(plan_sha256):
        raise CompositionAdmissionError("clickhouse_source_payload")
    identity = parse_airflow_run_identity_json(str(request.env.get(AIRFLOW_RUN_IDENTITY_ENV) or ""))
    attempt = airflow_attempt_from_environment(request.env, identity)
    del cache_root  # Retained call signature; release-side snapshot files grant no authority.
    return CompositionClickHouseExecutionRequest(
        manifest=manifest,
        plan_sha256=plan_sha256,
        run_identity=identity,
        airflow_attempt=attempt,
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
    attempt = _pack_attempt(parent, manifest, environment, plan)
    supervisor = cast(
        ClickHouseSupervisorObserver,
        DockerClickHouseLocalSupervisor(
            enrollment_sha256=enrollment.enrollment_sha256,
            docker=LocalDockerSupervisorClient(),
            linux=LinuxSupervisorProbe(),
        ),
    )
    root = snapshot_store_root(environment)
    return build_protected_clickhouse_cell(
        parent=parent,
        manifest=manifest,
        plan=plan,
        attempt=attempt,
        write=write,
        enrollment=enrollment,
        supervisor=supervisor,
        files=ProtectedSnapshotFiles(root),
        root=root,
    ).dependencies


def _pack_attempt(parent: Any, manifest: Any, environment: Any, plan: Any) -> Any:
    from dpone.contracts.dbt_runtime import (
        AIRFLOW_RUN_IDENTITY_ENV,
        airflow_attempt_from_environment,
        parse_airflow_run_identity_json,
    )

    identity = parse_airflow_run_identity_json(str(environment.get(AIRFLOW_RUN_IDENTITY_ENV) or ""))
    return build_composition_clickhouse_attempt(
        parent["read_active"](),
        manifest=manifest,
        plan_sha256=plan.sources.subject_sha256,
        run_identity=identity,
        airflow_attempt=airflow_attempt_from_environment(environment, identity),
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
