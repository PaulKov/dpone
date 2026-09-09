"""Build one portable environment-neutral dbt release-set v2."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_project_artifacts import (
    DbtReleaseInputs as DbtReleaseInputs,
)
from dpone.contracts.dbt_project_artifacts import (
    DbtWorkflowReleasePlan,
    project_runtime_payloads,
    workflow_logical_target,
)
from dpone.contracts.dbt_project_artifacts import (
    release_toolchain_sha256 as _toolchain_sha256,
)
from dpone.contracts.dbt_project_artifacts import (
    require_dbt_workflow_graph_ownership as _require_dbt_workflow_graph_ownership,
)
from dpone.contracts.dbt_publishing import DbtExecutionPack, DbtPublishingError, DbtSelectionLock, sha256_bytes
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from dpone.services.dbt_release_runtime_options import (
    dbt_warning_policy,
    positive_int,
)
from dpone.services.dbt_release_runtime_options import (
    profiles_dir as _profiles_dir,
)
from dpone.services.dbt_release_set_assembly import build_release_set as build_release_set

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import CompiledDbtWorkflow, DbtCompileReport
    from dpone.ports.dbt_project_bundle import (
        DbtProjectBundleBuilder,
        DbtProjectBundleExtractor,
        DbtProjectBundleVerifier,
    )
    from dpone.ports.dbt_publishing import DbtProjectPolicyValidator
    from dpone.ports.dbt_selection import DbtSelectionResolver

DBT_PROJECT_MEDIA_TYPE = "application/vnd.dpone.dbt-project-bundle+gzip"
DBT_MANIFEST_MEDIA_TYPE = "application/vnd.dbt.manifest+json"
DBT_SELECTION_MEDIA_TYPE = "application/vnd.dpone.dbt-selection-lock+json"

# Retain the original private compatibility access points without owning policy.
_workflow_logical_target = workflow_logical_target
_dbt_warning_policy = dbt_warning_policy
_positive_int = positive_int


def build_release_inputs(
    report: DbtCompileReport,
    *,
    project_root: Path,
    selection_resolver: DbtSelectionResolver,
    bundle_builder: DbtProjectBundleBuilder,
    bundle_extractor: DbtProjectBundleExtractor,
    bundle_verifier: DbtProjectBundleVerifier,
    project_policy: DbtProjectPolicyValidator,
    profiles_dir: Path | None = None,
    manifest_payload: bytes | None = None,
    wire_contract: str = DBT_RUNTIME_WIRE_V1,
) -> DbtReleaseInputs:
    """Snapshot source once and freeze workflow selections against the manifest."""

    if wire_contract not in (DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2):
        raise ValueError("unsupported dbt release wire contract")
    manifest_bytes = Path(report.manifest_path).read_bytes() if manifest_payload is None else manifest_payload
    manifest_sha256 = sha256_bytes(manifest_bytes)
    if report.manifest_sha256 is None or report.manifest_sha256 != manifest_sha256:
        raise ValueError("dbt manifest changed after canonical validation")
    bundle = bundle_builder.build(project_root)
    toolchain_sha256 = _toolchain_sha256(report)
    locks: dict[str, DbtSelectionLock] = {}
    packs: dict[str, DbtExecutionPack] = {}
    authorities: set[str] = set()
    resolved_profiles_dir = _profiles_dir(profiles_dir, project_root)
    with TemporaryDirectory(prefix="dpone-dbt-source-snapshot-") as temporary:
        snapshot_root = Path(temporary) / "dbt-project"
        bundle_extractor.extract(bundle.archive, snapshot_root)
        bundle_verifier.verify(bundle.archive, snapshot_root)
        project_issues = project_policy.validate_root(snapshot_root)
        if project_issues:
            issue = project_issues[0]
            raise DbtPublishingError(
                issue.code,
                issue.message,
                path=issue.path,
                remediation=issue.remediation,
            )
        for workflow in report.workflows:
            plan = DbtWorkflowReleasePlan.prepare(workflow)
            selection = selection_resolver.resolve(
                project_root=snapshot_root,
                manifest_bytes=manifest_bytes,
                profiles_dir=resolved_profiles_dir,
                **plan.selection_arguments(),
            )
            authorities.add(selection.authority)
            lock, pack = plan.complete(
                selection,
                manifest_sha256=manifest_sha256,
                toolchain_sha256=toolchain_sha256,
                project_sha256=bundle.bundle.archive_sha256,
                wire_contract=wire_contract,
            )
            locks[workflow.workflow] = lock
            packs[workflow.workflow] = pack
    _require_dbt_workflow_graph_ownership(
        publish_model_ids_by_workflow={workflow: lock.publish_model_unique_ids for workflow, lock in locks.items()},
        selected_graph_ids_by_workflow={workflow: lock.selected_graph_unique_ids for workflow, lock in locks.items()},
        model_paths={
            item.model.unique_id: item.model.original_file_path
            for workflow in report.workflows
            for item in workflow.models
        },
    )
    if len(authorities) != 1:
        raise ValueError("dbt workflows resolved through different selection authorities")
    return DbtReleaseInputs(
        project_root=project_root,
        manifest_bytes=manifest_bytes,
        project_archive=bundle.archive,
        project_sha256=bundle.bundle.archive_sha256,
        toolchain_sha256=toolchain_sha256,
        selection_locks=locks,
        execution_packs=packs,
        selection_authority=authorities.pop(),
    )


def runtime_payload_files(inputs: DbtReleaseInputs) -> dict[str, bytes]:
    """Compatibility adapter over the shared versioned payload projection."""

    return dict(project_runtime_payloads(inputs, wire_contract=DBT_RUNTIME_WIRE_V1).files)


def runtime_payload_descriptors(inputs: DbtReleaseInputs) -> list[dict[str, Any]]:
    """Retain legacy descriptor ordering through the canonical projector."""

    return [dict(row) for row in project_runtime_payloads(inputs, wire_contract=DBT_RUNTIME_WIRE_V1).descriptors]


def workflow_runtime_payload_ids(workflow: CompiledDbtWorkflow) -> tuple[str, ...]:
    return ("dbt_project", "dbt_manifest", f"dbt_selection_{workflow.workflow}")


def source_snapshot_sha256(project_archive_sha256: str, manifest_sha256: str) -> str:
    return _fingerprint({"project": project_archive_sha256, "manifest": manifest_sha256})


def _fingerprint(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "DbtReleaseInputs",
    "build_release_inputs",
    "build_release_set",
    "runtime_payload_descriptors",
    "runtime_payload_files",
    "source_snapshot_sha256",
    "workflow_runtime_payload_ids",
]
