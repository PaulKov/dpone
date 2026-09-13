"""Route one admitted pack-exec command to the installed v3 cell factories.

Native-v2 dispatch stays in ``composition_native_dbt_dispatch``. This cell
selects the v3 factory after the verified command and manifest are classified.
Ordinary argv remains ``dpone run``; the concrete cell is a manifest decision.

PostgreSQL→MSSQL pack-exec reopens the producer-verified parent plan from
``DPONE_CACHE_ROOT`` (or ``DPONE_SCHEDULER_CACHE_ROOT``) before composing a
root. Missing cache, a drifted source subject, or a missing factory fail closed
before login issuance. ClickHouse pack-exec composes
``CompositionClickHouseExecutionRoot`` only when that plan, protected runtime snapshot
capture, and enrolled supervisor/HTTP collaborators all exist.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import Any

from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionRoot,
    CompositionClickHouseResult,
)
from dpone.app.composition_execution_cells import (
    MSSQL_CLICKHOUSE_FULL_REFRESH_V1,
    POSTGRES_MSSQL_FULL_REFRESH_V1,
    InstalledCompositionExecutionCapabilities,
)
from dpone.app.composition_pack_cache import (
    CACHE_ROOT_ENV,
    SCHEDULER_CACHE_ROOT_ENV,
    cache_root_from_environment,
)
from dpone.app.composition_transfer_execution import (
    CompositionTransferExecutionRequest,
    CompositionTransferExecutionRoot,
    CompositionTransferResult,
)
from dpone.app.composition_transfer_execution_factory import (
    build_composition_transfer_execution_dependencies,
)
from dpone.app.composition_transfer_execution_factory import (
    transfer_payload_root as _transfer_payload_root,
)
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_control import CompositionExecutionPlan
from dpone.contracts.composition_execution import (
    composition_generated_transfer_cell,
    composition_transfer_cell,
)
from dpone.contracts.dbt_runtime import (
    AIRFLOW_RUN_IDENTITY_ENV,
    airflow_attempt_from_environment,
    parse_airflow_run_identity_json,
)
from dpone.contracts.strict_json import strict_json_object
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.manifest.composition_execution_plan import plan_composition_execution
from dpone.runtime.composition_native_dbt_dispatch import (
    EVIDENCE_DISAGREEMENT,
    EVIDENCE_WRITE_FAILED,
    ORDINARY_WORKER_UNAVAILABLE,
    CompositionNativeDbtDispatcher,
)
from dpone.runtime.composition_verified_dispatch import (
    CompositionDispatchRejection,
    CompositionDispatchRequest,
    CompositionRunVolume,
)
from dpone.runtime.deployment_cache_common import require_path_without_symlinks

_MAX_MANIFEST_BYTES = 8 * 1024 * 1024


class CompositionPackExecutionDispatcher:
    """Dispatch native dbt or ordinary transfer through the installed factories."""

    def __init__(
        self,
        *,
        supervisor: Any,
        capabilities: InstalledCompositionExecutionCapabilities,
        native_executor: Any | None,
        ordinary_root: Callable[[CompositionDispatchRequest, str, Mapping[str, Any]], Any | None],
    ) -> None:
        self._native = CompositionNativeDbtDispatcher(native_executor, supervisor=supervisor)
        self._capabilities = capabilities
        self._ordinary_root = ordinary_root

    def run(self, request: CompositionDispatchRequest) -> int:
        if request.kind == "native_dbt":
            return self._native.run(request)
        if request.kind != "ordinary_transfer":
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        return self._run_ordinary(request)

    def _run_ordinary(self, request: CompositionDispatchRequest) -> int:
        try:
            manifest = ordinary_manifest(request)
            cell = ordinary_cell(manifest)
            factory = self._capabilities.factory(cell)
        except (CompositionAdmissionError, CompositionDispatchRejection, OSError, TypeError, ValueError):
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE) from None
        if not callable(factory):
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        root = self._ordinary_root(request, cell, manifest)
        if root is None:
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        prove = getattr(root, "can_execute_attempt", None)
        if callable(prove) and not prove():
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        try:
            typed = _typed_ordinary_request(request, cell, manifest)
        except (CompositionAdmissionError, OSError, TypeError, ValueError):
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE) from None
        try:
            result = root.execute(typed)
        except CompositionAdmissionError:
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True) from None
        return publish_pack_exec_evidence(request.run_volume, cell, result)


def ordinary_manifest(request: CompositionDispatchRequest) -> Mapping[str, Any]:
    """Read the launcher-verified ordinary manifest from the worktree."""

    path = request.working_directory / request.verified_input
    payload = path.read_bytes()
    if len(payload) > _MAX_MANIFEST_BYTES:
        raise CompositionAdmissionError("transfer_manifest_capability")
    return strict_json_object(payload)


def ordinary_cell(manifest: Mapping[str, Any]) -> str:
    """Classify PostgreSQL→MSSQL vs MSSQL→ClickHouse from original bytes."""

    try:
        return composition_transfer_cell(manifest)
    except CompositionAdmissionError:
        return composition_generated_transfer_cell(manifest)


def reopen_composition_plan(cache_root: Path, release_id: str) -> CompositionExecutionPlan:
    """Recapture the producer-verified parent and plan every admitted cell."""

    if not is_canonical_sha256_digest(release_id):
        raise CompositionAdmissionError("transfer_source_plan")
    from dpone.app.release_composition import build_composition_source_reader

    root = Path(cache_root) / "releases" / release_id.replace(":", "-")
    try:
        require_path_without_symlinks(root, root=Path(cache_root), error_path=root)
        sources = build_composition_source_reader().read_sources(root, expected_release_id=release_id)
        plan = plan_composition_execution(sources)
    except CompositionAdmissionError:
        raise
    except Exception:
        raise CompositionAdmissionError("transfer_source_plan") from None
    plan.__post_init__()
    return plan


def verify_transfer_pack_operation(
    attempt: Any,
    operation: Any,
    write: Any,
    mutation_plan_sha256: bytes,
    plan: Any,
    *,
    verified_manifest: Mapping[str, Any] | None = None,
    runtime_environment: str | None = None,
) -> None:
    """Reject a generic operation that is not the sealed plan write."""

    from dpone.contracts.composition_persistence import CompositionAttemptIdentity

    if type(attempt) is not CompositionAttemptIdentity:
        raise CompositionAdmissionError("transfer_operation")
    attempt.__post_init__()
    writes = getattr(plan, "writes", ())
    request = getattr(getattr(operation, "attempt", None), "request", None)
    if (
        write not in writes
        or request is None
        or getattr(request, "target_schema", None) != getattr(write, "schema", None)
        or getattr(request, "target_table", None) != getattr(write, "relation", None)
        or getattr(request, "strategy", None) != "full_refresh"
        or type(mutation_plan_sha256) is not bytes
        or len(mutation_plan_sha256) != 32
    ):
        raise CompositionAdmissionError("transfer_operation")
    from dpone.app.composition_transfer_invocation import expected_transfer_invocation

    invocation = getattr(request, "invocation", None)
    expected = expected_transfer_invocation(
        attempt,
        write,
        verified_manifest=verified_manifest,
        runtime_environment=runtime_environment,
    )
    if invocation != expected or getattr(getattr(plan, "sources", None), "subject_sha256", None) != attempt.plan_sha256:
        raise CompositionAdmissionError("transfer_operation")
    declared = getattr(write, "database", None)
    if declared is not None and getattr(request, "target_database", None) != declared:
        raise CompositionAdmissionError("transfer_operation")


def transfer_execution_request(
    request: CompositionDispatchRequest,
    manifest: Mapping[str, Any],
    *,
    plan_sha256: str,
) -> CompositionTransferExecutionRequest:
    """Bind scheduler identity and the compiled transfer load to one request."""

    if not is_canonical_sha256_digest(plan_sha256):
        raise CompositionAdmissionError("transfer_source_plan")
    try:
        identity = parse_airflow_run_identity_json(str(request.env.get(AIRFLOW_RUN_IDENTITY_ENV) or ""))
        attempt = airflow_attempt_from_environment(request.env, identity)
        load_config = LoadConfigBuilder().build(dict(manifest), base_path=request.working_directory)
        from dpone.services.airflow_mapping_context import AirflowMappingContextService
        from dpone.services.interval_context import IntervalContextService
        from dpone.services.run_invocation_context import RunInvocationContextService

        invocation = RunInvocationContextService(
            mapping_context_service=AirflowMappingContextService(),
            interval_context_factory=IntervalContextService,
        ).resolve(environ=request.env, dag_id=attempt.dag_id)
        load_config = invocation.load_config_mutator(load_config)
    except (CompositionAdmissionError, OSError, TypeError, ValueError):
        raise
    except Exception:
        raise CompositionAdmissionError("transfer_source_plan") from None
    selector = request.process_selector or str(manifest.get("name") or "")
    return CompositionTransferExecutionRequest(
        manifest=manifest,
        plan_sha256=plan_sha256,
        run_identity=identity,
        airflow_attempt=attempt,
        raw_config=manifest,
        load_config=load_config,
        selector=selector or None,
        execution_date=invocation.execution_date,
    )


def compose_pack_execution_root(
    *,
    request: CompositionDispatchRequest,
    cell: str,
    manifest: Mapping[str, Any],
    capabilities: InstalledCompositionExecutionCapabilities,
    parent: Mapping[str, Any],
) -> Any | None:
    """Compose a typed ordinary root after the sealed plan can be reopened."""

    if parent.get("context") is None:
        return None
    factory = capabilities.factory(cell)
    if not callable(factory):
        return None
    environment = dict(request.env)
    cache = cache_root_from_environment(environment)
    plan = reopen_composition_plan(cache, parent["context"].release_id)
    if cell == POSTGRES_MSSQL_FULL_REFRESH_V1:
        root = factory(dependencies=_transfer_dependencies(parent, manifest, cache, plan, environment=environment))
        if type(root) is not CompositionTransferExecutionRoot:
            raise CompositionAdmissionError("execution_capability")
        return root
    if cell != MSSQL_CLICKHOUSE_FULL_REFRESH_V1:
        return None
    from dpone.app.composition_clickhouse_execution_factory import compose_clickhouse_pack_dependencies

    dependencies = compose_clickhouse_pack_dependencies(
        parent=parent, manifest=manifest, environment=environment, plan=plan
    )
    if dependencies is None:
        return None
    root = factory(dependencies=dependencies)
    if type(root) is not CompositionClickHouseExecutionRoot:
        raise CompositionAdmissionError("execution_capability")
    return root


def publish_pack_exec_evidence(run_volume: CompositionRunVolume, cell: str, result: Any) -> int:
    """Retain worker metrics only after the parent root already sealed OUTCOME."""

    if type(result) is CompositionTransferResult:
        rows = result.rows_written
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
        extra: dict[str, Any] = {"rows_written": rows}
    elif type(result) is CompositionClickHouseResult:
        if result.publication_state != "PUBLISHED":
            raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
        extra = {"publication_state": result.publication_state, "rows": len(result.rows)}
    else:
        raise CompositionDispatchRejection(EVIDENCE_DISAGREEMENT, dispatch_started=True)
    payload = {
        "kind": "dpone.composition.pack-exec-evidence.v1",
        "status": "passed",
        "cell": cell,
        **extra,
    }
    try:
        document = json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2)
        run_volume.evidence_path.write_text(document, encoding="utf-8")
    except (OSError, TypeError, ValueError):
        raise CompositionDispatchRejection(EVIDENCE_WRITE_FAILED, dispatch_started=True) from None
    return 0


def _typed_ordinary_request(
    request: CompositionDispatchRequest,
    cell: str,
    manifest: Mapping[str, Any],
) -> Any:
    identity = parse_airflow_run_identity_json(str(request.env.get(AIRFLOW_RUN_IDENTITY_ENV) or ""))
    cache = cache_root_from_environment(request.env)
    plan = reopen_composition_plan(cache, identity.release_id)
    if cell == POSTGRES_MSSQL_FULL_REFRESH_V1:
        return transfer_execution_request(request, manifest, plan_sha256=plan.sources.subject_sha256)
    if cell == MSSQL_CLICKHOUSE_FULL_REFRESH_V1:
        from dpone.app.composition_clickhouse_execution_factory import clickhouse_execution_request

        return clickhouse_execution_request(
            request, manifest, plan_sha256=plan.sources.subject_sha256, cache_root=cache
        )
    raise CompositionAdmissionError("execution_capability")


def _transfer_dependencies(
    parent: Mapping[str, Any],
    manifest: Mapping[str, Any],
    cache_root: Path,
    plan: CompositionExecutionPlan,
    *,
    environment: Mapping[str, str],
) -> Any:
    state = manifest.get("state")
    if not isinstance(state, Mapping) or not isinstance(state.get("connection_ref"), str):
        raise CompositionAdmissionError("external_target_atomic_state_required")
    expected = plan.sources.subject_sha256

    def read_plan(attempt: Any) -> CompositionExecutionPlan:
        del attempt
        occurrence = parent["read_active"]()
        occurrence.require_state("ACTIVE")
        fresh = reopen_composition_plan(cache_root, occurrence.request.release_id)
        if (
            fresh.sources.subject_sha256 != occurrence.request.source_subject_sha256
            or fresh.workloads != occurrence.request.workloads
            or fresh.sources.subject_sha256 != expected
        ):
            raise CompositionAdmissionError("transfer_source_plan")
        return fresh

    return build_composition_transfer_execution_dependencies(
        control=parent["control"],
        read_active=parent["read_active"],
        sink_target=parent["target"],
        state_target=parent["resolver"].resolve(str(state["connection_ref"])),
        read_plan=read_plan,
        verify_operation=partial(
            verify_transfer_pack_operation,
            verified_manifest=manifest,
            runtime_environment=parent["context"].environment,
        ),
        state_config=state,
        payload_root=_transfer_payload_root(environment),
        verified_manifest=manifest,
        source_target=parent["resolver"].resolve(str(manifest["source"]["connection_ref"])),
        parent_context=parent["context"],
    )


__all__ = [
    "CACHE_ROOT_ENV",
    "SCHEDULER_CACHE_ROOT_ENV",
    "CompositionPackExecutionDispatcher",
    "cache_root_from_environment",
    "compose_pack_execution_root",
    "ordinary_cell",
    "ordinary_manifest",
    "publish_pack_exec_evidence",
    "reopen_composition_plan",
    "transfer_execution_request",
    "verify_transfer_pack_operation",
]
