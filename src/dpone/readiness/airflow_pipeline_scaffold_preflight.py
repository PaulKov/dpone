"""Fail-before-write checks for domain-first pipeline scaffolding."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path

from dpone.manifest.confined_files import ConfinedFileError, sha256_confined_file
from dpone.manifest.project_config import ProjectLayout
from dpone.manifest.project_discovery import ProjectDiscoveryIssue, ProjectDiscoveryService
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.error_contract import error_docs_url, manual_fix


@dataclass(frozen=True, slots=True)
class DomainFirstScaffoldGuard:
    """Revalidate the exact preflight authority before and after scaffold writes."""

    root: Path
    layout: ProjectLayout
    domain: str
    pipeline_id: str
    expected_path: str
    project_fingerprint: str
    workload_fingerprints: Mapping[str, str]
    consumed_files: Mapping[str, str | None]

    def with_consumed_files(self, consumed_files: Mapping[str, str]) -> DomainFirstScaffoldGuard:
        """Bind additional inputs that were read after the initial discovery preflight."""

        merged = dict(self.consumed_files)
        merged.update(consumed_files)
        return replace(self, consumed_files=dict(sorted(merged.items())))

    def before_apply(self) -> bool:
        if not self._inputs_unchanged():
            return False
        snapshot = ProjectDiscoveryService(self.root).discover(layout=self.layout)
        return snapshot.ok and snapshot.project_fingerprint == self.project_fingerprint

    def after_apply(self) -> bool:
        if not self._inputs_unchanged():
            return False
        snapshot = ProjectDiscoveryService(self.root).discover(layout=self.layout)
        if not snapshot.ok:
            return False
        workloads = {workload.pipeline_id: workload for workload in snapshot.workloads}
        expected_ids = {*self.workload_fingerprints, self.pipeline_id}
        target = workloads.get(self.pipeline_id)
        if (
            set(workloads) != expected_ids
            or target is None
            or target.domain != self.domain
            or target.checked_source.source_label != self.expected_path
        ):
            return False
        return all(
            workloads[pipeline_id].workload_fingerprint == fingerprint
            for pipeline_id, fingerprint in self.workload_fingerprints.items()
            if pipeline_id != self.pipeline_id
        )

    def _inputs_unchanged(self) -> bool:
        return all(_current_digest(self.root, path) == digest for path, digest in self.consumed_files.items())


@dataclass(frozen=True, slots=True)
class DomainFirstScaffoldPreflight:
    failure: SelfServiceResult | None = None
    guard: DomainFirstScaffoldGuard | None = None


def preflight_domain_first_scaffold(
    root: Path,
    *,
    layout: ProjectLayout,
    domain: str | None,
    pipeline_id: str,
    project_config_sha256: str | None,
) -> DomainFirstScaffoldPreflight:
    """Validate domain ownership and project identity before any file write."""

    if not layout.is_domain_first:
        return DomainFirstScaffoldPreflight()
    if domain is None:
        return DomainFirstScaffoldPreflight(
            failure=_authoring_failed(
                "DPONE_DOMAIN_REQUIRED",
                "Domain-first projects require --domain.",
                command="dpone init pipeline --help",
                exit_code=2,
            )
        )
    discovery = ProjectDiscoveryService(root)
    ownership_issue = discovery.ownership_issue(domain, layout=layout)
    if ownership_issue is not None:
        return DomainFirstScaffoldPreflight(failure=_discovery_failed(ownership_issue))
    snapshot = discovery.discover(layout=layout)
    if snapshot.issues:
        return DomainFirstScaffoldPreflight(failure=_discovery_failed(snapshot.issues[0]))
    existing = next((item for item in snapshot.workloads if item.pipeline_id == pipeline_id), None)
    expected_path = f"{layout.root}/{domain}/pipelines/{pipeline_id}/pipeline.yaml"
    if existing is not None and existing.checked_source.source_label != expected_path:
        return DomainFirstScaffoldPreflight(
            failure=_authoring_failed(
                "DPONE_PIPELINE_ID_DUPLICATE",
                "Pipeline id must be unique across the project.",
                entity={"kind": "pipeline", "id": pipeline_id},
                command="dpone check .",
            )
        )
    consumed: dict[str, str | None] = dict(snapshot.consumed_files)
    consumed["dpone.yaml"] = project_config_sha256
    return DomainFirstScaffoldPreflight(
        guard=DomainFirstScaffoldGuard(
            root=root,
            layout=layout,
            domain=domain,
            pipeline_id=pipeline_id,
            expected_path=expected_path,
            project_fingerprint=snapshot.project_fingerprint,
            workload_fingerprints={
                workload.pipeline_id: workload.workload_fingerprint for workload in snapshot.workloads
            },
            consumed_files=dict(sorted(consumed.items())),
        )
    )


def _current_digest(root: Path, path: str) -> str | None:
    try:
        return sha256_confined_file(root, path)
    except (ConfinedFileError, OSError):
        return None


def _discovery_failed(issue: ProjectDiscoveryIssue) -> SelfServiceResult:
    entity = None
    if issue.pipeline_id is not None:
        entity = {"kind": "pipeline", "id": issue.pipeline_id}
    elif issue.domain is not None:
        entity = {"kind": "domain", "id": issue.domain}
    command = (
        (
            f"dpone init domain {issue.domain} "
            "--owner-team <team> --owner-contact <contact> --approver-team <github-team>"
        )
        if issue.code == "DPONE_DOMAIN_OWNERSHIP_MISSING" and issue.domain is not None
        else "dpone check ."
    )
    exit_code = 4 if issue.code == "DPONE_DISCOVERY_PATH_INVALID" else 1
    return _authoring_failed(
        issue.code,
        issue.message,
        entity=entity,
        command=command,
        exit_code=exit_code,
    )


def _authoring_failed(
    code: str,
    message: str,
    *,
    command: str,
    entity: dict[str, str] | None = None,
    exit_code: int = 1,
) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage="init_pipeline",
                entity=entity,
                fixes=[manual_fix("repair_authoring_contract", command=command)],
                docs_url=error_docs_url(code),
            ),
        ),
        exit_code=exit_code,
    )


__all__ = [
    "DomainFirstScaffoldGuard",
    "DomainFirstScaffoldPreflight",
    "preflight_domain_first_scaffold",
]
