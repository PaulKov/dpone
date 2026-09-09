"""Preserved singleton release assembly over the reusable project projection."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_contract_validation import DbtPublishingError, artifact_json_bytes
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1
from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder
from dpone.readiness.dbt_airflow_pack_adapter import AirflowPackBuilderPort
from dpone.readiness.dbt_publish_atomic_publisher import (
    DbtArtifactOutputConflict,
    DbtArtifactPublicationError,
    DbtArtifactTreePublisher,
)
from dpone.services.dbt_project_artifacts import DbtProjectArtifactProjector
from dpone.services.dbt_publish_artifact_report import blocked_report as _blocked_report
from dpone.services.dbt_release_assets import (
    build_source_snapshot,
    canonical_schema_descriptors,
    canonical_schema_files,
    source_snapshot_bytes,
)
from dpone.services.dbt_release_set_assembly import build_release_set
from dpone.version import installed_version

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import DbtCompileReport, DbtPublishIssue
    from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPreReleaseProofBundle
    from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations
    from dpone.ports.dbt_publishing import DbtProjectPolicyValidator
    from dpone.ports.dbt_selection import DbtSelectionResolver

DbtExecutionPackBuilder = DbtAirflowExecutionPackBuilder


class DbtArtifactWriter:
    """Write the existing singleton wire without changing its output semantics."""

    def __init__(
        self,
        *,
        selection_resolver: DbtSelectionResolver,
        bundle_operations: DbtProjectBundleOperations,
        project_policy: DbtProjectPolicyValidator,
        pack_builder: AirflowPackBuilderPort | None = None,
        dbt_pack_builder: DbtAirflowExecutionPackBuilder | None = None,
        publisher: DbtArtifactTreePublisher | None = None,
        dbt_profiles_dir: Path | None = None,
        producer_version: str = installed_version(),
    ) -> None:
        self._projector = DbtProjectArtifactProjector(
            selection_resolver=selection_resolver,
            bundle_operations=bundle_operations,
            project_policy=project_policy,
            pack_builder=pack_builder,
            dbt_pack_builder=dbt_pack_builder,
        )
        self._publisher = publisher if publisher is not None else DbtArtifactTreePublisher()
        self._project_policy = project_policy
        self._dbt_profiles_dir = dbt_profiles_dir
        self._producer_version = producer_version

    def write(
        self,
        report: DbtCompileReport,
        output_dir: str | Path,
        *,
        project_root: str | Path | None = None,
        environment: str = "dev",
        semantic_refresh_pre_release_bundles: Mapping[str, SemanticRefreshPreReleaseProofBundle] | None = None,
    ) -> DbtCompileReport:
        if not report.passed:
            raise ValueError("Cannot write artifacts for a blocked dbt publish compile")
        if not report.models:
            return report
        root = Path(output_dir).absolute()
        resolved_project_root = _project_root(report, project_root)
        project_issues = self._project_policy.validate_root(resolved_project_root)
        if project_issues:
            return replace(report, blockers=tuple(_portable_issue(item) for item in project_issues))
        _ = environment  # Physical binding remains a deployment concern.
        try:
            files, artifacts = self._build_tree(
                report,
                project_root=resolved_project_root,
                semantic_refresh_pre_release_bundles=semantic_refresh_pre_release_bundles,
            )
        except DbtPublishingError as exc:
            return _blocked_report(
                report,
                code=exc.code,
                message=str(exc),
                path=exc.path or report.manifest_path,
                remediation=exc.remediation,
            )
        except Exception:
            return _blocked_report(
                report,
                code="DPONE_DBT_COMPILE_FAILED",
                message="The dbt artifact tree could not be compiled safely",
                path=report.manifest_path,
            )
        try:
            self._publisher.publish(root, files)
        except DbtArtifactOutputConflict:
            return _blocked_report(
                report,
                code="DPONE_DBT_PUBLISH_OUTPUT_CONFLICT",
                message="The output directory already contains different content",
                path=root.as_posix(),
            )
        except DbtArtifactPublicationError:
            return _blocked_report(
                report,
                code="DPONE_DBT_OUTPUT_WRITE_FAILED",
                message="The dbt artifact tree could not be published atomically",
                path=root.as_posix(),
            )
        release = json.loads(files["release-set.json"])
        return replace(report, artifacts=artifacts, release_id=str(release["release_id"]))

    def _build_tree(
        self,
        report: DbtCompileReport,
        *,
        project_root: Path,
        semantic_refresh_pre_release_bundles: Mapping[str, SemanticRefreshPreReleaseProofBundle] | None = None,
    ) -> tuple[dict[str, bytes], dict[str, str]]:
        project = self._projector.project(
            report,
            project_root=project_root,
            wire_contract=DBT_RUNTIME_WIRE_V1,
            profiles_dir=self._dbt_profiles_dir,
            semantic_refresh_pre_release_bundles=semantic_refresh_pre_release_bundles,
        )
        inputs = project.inputs
        files, artifacts = dict(project.files), dict(project.artifacts)
        schema_files = canonical_schema_files()
        files.update(schema_files)
        source_snapshot = build_source_snapshot(
            project_bundle_sha256=inputs.project_sha256,
            manifest_sha256=inputs.execution_packs[report.workflows[0].workflow].selection_lock.manifest_sha256,
        )
        source_snapshot_path = "_dbt/dbt-source-snapshot.json"
        files[source_snapshot_path] = source_snapshot_bytes(source_snapshot)
        artifacts["release:source_snapshot"] = source_snapshot_path
        release = build_release_set(
            dag_files=project.dag_files,
            pack_files=project.pack_files,
            pack_runtime_payload_ids=project.runtime_payload_ids,
            runtime_files=project.runtime_files,
            runtime_descriptors=[dict(row) for row in project.payloads.descriptors],
            canonical_schema_files=schema_files,
            canonical_schema_descriptors=canonical_schema_descriptors(schema_files),
            source_snapshot_sha256=source_snapshot["snapshot_sha256"],
            selection_authority=inputs.selection_authority,
            route_certifications=project.route_certifications(report),
            selection_fingerprints=[lock.selection_sha256 for _, lock in sorted(inputs.selection_locks.items())],
            producer_version=self._producer_version,
        )
        # External pack verification must not invalidate the captured receipts.
        project.route_certifications(report)
        files["release-set.json"] = artifact_json_bytes(release)
        artifacts["release"] = "release-set.json"
        evidence_path = "_dbt/dbt-publish-evidence.json"
        stable_report = replace(
            report,
            manifest_path="manifest.json",
            warnings=tuple(_portable_issue(item) for item in report.warnings),
            blockers=tuple(_portable_issue(item) for item in report.blockers),
            artifacts=artifacts,
        )
        files[evidence_path] = artifact_json_bytes(stable_report.to_jsonable())
        artifacts["evidence"] = evidence_path
        return files, artifacts


def _project_root(report: DbtCompileReport, configured: str | Path | None) -> Path:
    if configured is not None:
        root = Path(configured).absolute()
        if not (root / "dbt_project.yml").is_file():
            raise ValueError("project_root must contain dbt_project.yml")
        return root
    current = Path(report.manifest_path).absolute().parent
    for candidate in (current, *current.parents):
        if (candidate / "dbt_project.yml").is_file():
            return candidate
    raise ValueError("dbt project root could not be discovered")


def _portable_issue(issue: DbtPublishIssue) -> DbtPublishIssue:
    path = Path(issue.path)
    return replace(issue, path=path.name if path.is_absolute() else issue.path)


__all__ = ["DbtArtifactWriter", "DbtExecutionPackBuilder"]
