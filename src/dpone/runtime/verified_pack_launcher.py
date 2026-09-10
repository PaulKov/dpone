"""Prepare one shell-free command from a fetched, verified workload pack."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan


import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from dpone.contracts.dbt_runtime import (
    DbtPublishingError,
    dbt_runtime_plan_payload_order,
    validate_dbt_runtime_release_identity,
    validate_dbt_runtime_source_projection,
)
from dpone.runtime.dbt_project_bundle import verify_dbt_project_bundle_tree
from dpone.runtime.deployment_cache_common import DeploymentCacheError, open_regular_file
from dpone.runtime.init_fetch_contract import InitFetchError, cache_relative_path
from dpone.runtime.runtime_init_fetch_ready import (
    RUNTIME_FETCH_READY_NAME,
    parse_ready_manifest,
)
from dpone.runtime.runtime_init_fetch_receipts import validate_runtime_receipts
from dpone.runtime.runtime_init_fetch_storage import read_bounded_regular_file
from dpone.runtime.runtime_payload_archive import (
    runtime_payload_archive,
    verify_runtime_payload_tree,
)
from dpone.runtime.verified_pack_command_selection import (
    selected_verified_command as _selected_command,
)

_MAX_READY_BYTES = 64 * 1024
_MAX_DBT_EXECUTION_PACK_BYTES = 1024 * 1024
_READ_BYTES = 64 * 1024
RUNTIME_CONNECTION_CONTEXT_ENV = "DPONE_RUNTIME_CONNECTION_CONTEXT"


@dataclass(frozen=True, slots=True)
class VerifiedPackCommand:
    """Exact child command plus non-secret environment additions."""

    argv: tuple[str, ...]
    env: Mapping[str, str]
    working_directory: Path
    exit_code_policy: Literal["xcom_gate", "child"] = "xcom_gate"
    publish_xcom: bool = True


class VerifiedPackLauncher:
    """Re-verify ready state and select no command supplied by the scheduler."""

    def __init__(self, *, artifact_root: Path, worktree_root: Path) -> None:
        self._artifact_root = artifact_root.absolute()
        self._worktree_root = worktree_root.absolute()

    def prepare(
        self,
        plan: RuntimeInitFetchPlan,
        *,
        plan_sha256: str,
        attestation_required: bool | None = None,
        deployment_attestation_required: bool | None = None,
    ) -> VerifiedPackCommand:
        ready_payload = read_bounded_regular_file(
            self._artifact_root / RUNTIME_FETCH_READY_NAME,
            root=self._artifact_root,
            max_bytes=_MAX_READY_BYTES,
            missing_code="DPONE_RUNTIME_FETCH_READY_INVALID",
            invalid_code="DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
            label="runtime ready manifest",
        )
        ready = parse_ready_manifest(
            ready_payload,
            plan=plan,
            plan_sha256=plan_sha256,
            attestation_required=attestation_required,
            deployment_attestation_required=deployment_attestation_required,
        )
        ready_artifacts = {
            plan.release.artifact_ref: ready.release,
            plan.deployment.artifact_ref: ready.deployment,
            plan.workload_pack.artifact_ref: ready.workload_pack,
        }
        payloads = {
            descriptor.artifact_ref: _read_artifact(
                self._artifact_root
                / (
                    ready_artifacts[descriptor.artifact_ref].locator
                    if descriptor.artifact_ref in ready_artifacts
                    else f"payload/{cache_relative_path(descriptor.artifact_ref).as_posix()}"
                ),
                root=self._artifact_root,
                expected_bytes=descriptor.bytes,
                expected_sha256=descriptor.sha256,
            )
            for descriptor in plan.artifacts
        }
        pack, _verified_pack_fingerprint = validate_runtime_receipts(plan, payloads)
        archive = runtime_payload_archive(pack)
        if archive.sha256 != ready.runtime_payload_sha256:
            raise _launcher_error("runtime payload differs from the ready manifest")
        dbt_project_bundle = _dbt_project_bundle(plan, payloads)
        verify_runtime_payload_tree(
            archive,
            self._worktree_root,
            external_roots=(("dbt-project",) if dbt_project_bundle is not None else ()),
        )
        if dbt_project_bundle is not None:
            try:
                verify_dbt_project_bundle_tree(
                    dbt_project_bundle,
                    self._worktree_root / "dbt-project",
                )
            except Exception as exc:
                raise _launcher_error("dbt project bundle worktree verification failed") from exc
        argv, environment = _selected_command(pack, plan)
        # Separate hooks hydrate the same canonical connections as the runtime.
        # Both receive only the context whose artifacts were verified above.
        environment = {
            **environment,
            RUNTIME_CONNECTION_CONTEXT_ENV: _runtime_connection_context_path(
                plan,
                artifact_root=self._artifact_root,
            ).as_posix(),
        }
        _validate_worktree_command(
            argv,
            root=self._worktree_root,
            manifest_index=(_runtime_input_index(argv) if plan.execution.kind == "runtime" else 3),
        )
        if argv[:3] == ("dpone", "dbt", "execute-pack"):
            _validate_dbt_runtime_identity(
                airflow_pack=pack,
                execution_pack_path=argv[3],
                payloads=payloads,
                plan=plan,
                worktree_root=self._worktree_root,
            )
        return VerifiedPackCommand(
            argv=argv,
            env=environment,
            working_directory=self._worktree_root,
            publish_xcom=plan.execution.kind != "pre_hook",
            exit_code_policy=(
                "child"
                if plan.execution.kind == "pre_hook" or argv[:3] == ("dpone", "dbt", "execute-pack")
                else "xcom_gate"
            ),
        )


def _runtime_connection_context_path(
    plan: RuntimeInitFetchPlan,
    *,
    artifact_root: Path,
) -> Path:
    parents = {
        cache_relative_path(descriptor.artifact_ref).parent
        for descriptor in (
            plan.binding_set,
            plan.connection_registry,
            plan.credential_runtime,
        )
    }
    if len(parents) != 1:
        raise _launcher_error("runtime connection artifacts do not share one verified context")
    return artifact_root / "payload" / parents.pop()


def _dbt_project_bundle(
    plan: RuntimeInitFetchPlan,
    payloads: Mapping[str, bytes],
) -> bytes | None:
    descriptors = [item for item in plan.runtime_payloads if item.kind == "dbt_project_bundle"]
    if not descriptors:
        return None
    if len(descriptors) != 1:
        raise _launcher_error("verified runtime plan contains multiple dbt project bundles")
    payload = payloads.get(descriptors[0].artifact_ref)
    if payload is None:
        raise _launcher_error("verified dbt project bundle is missing")
    return payload


def _runtime_input_index(argv: tuple[str, ...]) -> int:
    if argv[:3] == ("dpone", "dbt", "execute-pack"):
        return 3
    return 2


def _validate_worktree_command(
    argv: tuple[str, ...],
    *,
    root: Path,
    manifest_index: int,
) -> None:
    if len(argv) <= manifest_index:
        raise _launcher_error("verified workload command is incomplete")
    raw_manifest = argv[manifest_index]
    manifest = PurePosixPath(raw_manifest)
    if (
        not raw_manifest
        or "\\" in raw_manifest
        or manifest.is_absolute()
        or any(part in {"", ".", ".."} for part in manifest.parts)
    ):
        raise _launcher_error("verified workload manifest path is unsafe")
    try:
        descriptor = open_regular_file(
            root.joinpath(*manifest.parts),
            missing_code="DPONE_RUNTIME_WORKTREE_INCOMPLETE",
            invalid_code="DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
            label="runtime manifest",
            root=root,
        )
    except DeploymentCacheError as exc:
        raise InitFetchError(exc.code, str(exc)) from exc
    os.close(descriptor)


def _read_artifact(
    path: Path,
    *,
    root: Path,
    expected_bytes: int,
    expected_sha256: str,
) -> bytes:
    payload = read_bounded_regular_file(
        path,
        root=root,
        max_bytes=expected_bytes,
        missing_code="DPONE_RUNTIME_FETCH_READY_INVALID",
        invalid_code="DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
        label="fetched runtime artifact",
    )
    if len(payload) != expected_bytes:
        raise _launcher_error("fetched runtime artifact size changed after ready publication")
    if "sha256:" + hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise _launcher_error("fetched runtime artifact checksum changed after ready publication")
    return payload


def _validate_dbt_runtime_identity(
    *,
    airflow_pack: Mapping[str, Any],
    execution_pack_path: str,
    payloads: Mapping[str, bytes],
    plan: RuntimeInitFetchPlan,
    worktree_root: Path,
) -> None:
    by_kind = {item.kind: item for item in plan.runtime_payloads}
    required_kinds = {"dbt_project_bundle", "dbt_manifest", "dbt_selection_lock"}
    if len(plan.runtime_payloads) != 3 or set(by_kind) != required_kinds:
        raise InitFetchError(
            "DPONE_DBT_SELECTION_DRIFT",
            "verified dbt runtime identity is inconsistent",
        )
    execution_pack_payload = read_bounded_regular_file(
        worktree_root.joinpath(*PurePosixPath(execution_pack_path).parts),
        root=worktree_root,
        max_bytes=_MAX_DBT_EXECUTION_PACK_BYTES,
        missing_code="DPONE_DBT_SELECTION_DRIFT",
        invalid_code="DPONE_DBT_SELECTION_DRIFT",
        label="dbt execution pack",
    )
    project = by_kind["dbt_project_bundle"]
    manifest = by_kind["dbt_manifest"]
    selection_descriptor = by_kind["dbt_selection_lock"]
    try:
        wire_contract = validate_dbt_runtime_source_projection(
            release_payload=payloads[plan.release.artifact_ref],
            release_id=plan.release_id,
            workload_id=plan.workload_pack.id,
            payload_refs=tuple((item.id, item.artifact_ref) for item in plan.runtime_payloads),
            artifact_bytes=payloads,
        )
    except (ValueError, RecursionError) as exc:
        raise InitFetchError("DPONE_DBT_SELECTION_DRIFT", "verified dbt release wire is invalid") from exc
    pack_payload_ids = airflow_pack.get("runtime_payload_ids")
    plan_payload_ids = dbt_runtime_plan_payload_order(
        airflow_runtime_payload_ids=pack_payload_ids,
        runtime_payload_ids=tuple(item.id for item in plan.runtime_payloads),
        wire_contract=wire_contract,
    )
    try:
        validate_dbt_runtime_release_identity(
            execution_pack_payload=execution_pack_payload,
            selection_lock_payload=payloads[selection_descriptor.artifact_ref],
            project_bundle_sha256=project.sha256,
            manifest_sha256=manifest.sha256,
            workload_id=plan.workload_pack.id,
            airflow_runtime_payload_ids=pack_payload_ids,
            runtime_payload_ids=plan_payload_ids,
            wire_contract=wire_contract,
        )
    except DbtPublishingError as exc:
        raise InitFetchError(exc.code, str(exc)) from None


def _launcher_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


__all__ = [
    "RUNTIME_CONNECTION_CONTEXT_ENV",
    "VerifiedPackCommand",
    "VerifiedPackLauncher",
]
