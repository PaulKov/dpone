"""Plan the complete publishing inventory once, without publishing partial output."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_artifact_publication import DbtArtifactOutputConflict, DbtArtifactPublicationError
from dpone.contracts.dbt_contract_validation import DbtPublishingError, sha256_bytes
from dpone.contracts.dbt_publish_models import DbtCompileReport, DbtPublishIssue
from dpone.contracts.dbt_workspace import (
    DbtWorkspaceCheckReport,
    DbtWorkspaceCompileReport,
    DbtWorkspaceDiscoveryReport,
    DbtWorkspaceProjectCheck,
)
from dpone.services.dbt_release_integrity import DBT_RELEASE_SUBJECTS_FILENAME

if TYPE_CHECKING:
    from dpone.ports.dbt_workspace import (
        DbtWorkspaceCompilerFactory,
        DbtWorkspaceDiscoveryPort,
        DbtWorkspaceWriterFactory,
    )


class DbtWorkspaceService:
    """Shared authoring service; only explicit compile invokes dbt selection."""

    def __init__(
        self,
        *,
        discovery: DbtWorkspaceDiscoveryPort,
        compiler_factory: DbtWorkspaceCompilerFactory,
        writer_factory: DbtWorkspaceWriterFactory | None = None,
    ) -> None:
        self._discovery = discovery
        self._compiler_factory = compiler_factory
        self._writer_factory = writer_factory

    def discover(self, root: Path) -> DbtWorkspaceDiscoveryReport:
        return self._discovery.discover(root)

    def check(self, root: Path) -> DbtWorkspaceCheckReport:
        root = root.absolute()
        discovery = self.discover(root)
        if not discovery.passed:
            return DbtWorkspaceCheckReport(discovery)
        projects: list[DbtWorkspaceProjectCheck] = []
        for project in discovery.projects:
            if not project.publishing:
                continue
            project_root = root / project.project_path
            manifest = project_root / str(project.manifest_path)
            try:
                compiler = self._compiler_factory(root, project)
                report = compiler.build(
                    manifest,
                    profiles_path=project_root / str(project.profiles_path),
                    require_contracts=True,
                    allow_empty=False,
                )
            except (OSError, ValueError, RecursionError):
                report = DbtCompileReport(
                    manifest_path=manifest.as_posix(),
                    manifest_schema_version=None,
                    blockers=(
                        DbtPublishIssue(
                            code="DPONE_DBT_COMPILE_FAILED",
                            path=project.project_path,
                            message="Project inputs could not be checked safely",
                            remediation="Restore regular project-local inputs and regenerate the canonical manifest with the certified toolchain.",
                        ),
                    ),
                )
            projects.append(DbtWorkspaceProjectCheck(project, report))
        return DbtWorkspaceCheckReport(discovery, tuple(projects), _global_identities(tuple(projects)))

    def compile(self, root: Path, *, output_dir: Path) -> DbtWorkspaceCompileReport:
        """Check once and publish all projects, retaining diagnostic rows on failure."""

        root, output_dir = root.absolute(), output_dir.absolute()
        check = self.check(root)
        if not check.passed:
            return DbtWorkspaceCompileReport(check, output_dir.as_posix())
        if not check.projects:
            return _compile_failure(
                check,
                output_dir,
                DbtPublishIssue(
                    code="DPONE_DBT_NO_PUBLISH_MODELS",
                    message="The workspace has no configured publishing projects",
                    path=root.as_posix(),
                    remediation="Use a complete workspace root with project-local publishing policy. Empty compile is not a workload retirement operation.",
                ),
            )
        try:
            if self._writer_factory is None:
                raise RuntimeError("workspace publication capability is not configured")
            tree = self._writer_factory().write(check, root=root, output_dir=output_dir)
            return DbtWorkspaceCompileReport(
                check,
                output_dir.as_posix(),
                tree.release_id,
                tree.inventory.snapshot_sha256,
                sha256_bytes(tree.files[DBT_RELEASE_SUBJECTS_FILENAME]),
            )
        except DbtPublishingError as exc:
            issue = DbtPublishIssue(
                code=exc.code,
                message=str(exc),
                path=exc.path or root.as_posix(),
                remediation=exc.remediation,
            )
        except DbtArtifactOutputConflict:
            issue = DbtPublishIssue(
                code="DPONE_DBT_PUBLISH_OUTPUT_CONFLICT",
                message="The output directory already contains different content",
                path=output_dir.as_posix(),
                remediation="Keep the existing output; choose a new immutable output directory or retry its identical source inputs.",
            )
        except DbtArtifactPublicationError:
            issue = DbtPublishIssue(
                code="DPONE_DBT_OUTPUT_WRITE_FAILED",
                message="Output may already be visible, but durable publication could not be proven",
                path=output_dir.as_posix(),
                remediation="Check filesystem durability and verify the destination or retry identical inputs. Do not delete the output to force success.",
            )
        except (OSError, ValueError, RecursionError):
            issue = DbtPublishIssue(
                code="DPONE_DBT_COMPILE_FAILED",
                message="The complete workspace release could not be compiled safely",
                path=root.as_posix(),
                remediation="Inspect every project check, restore canonical manifests/dependencies and retry. Do not publish a subset.",
            )
        except Exception:
            issue = DbtPublishIssue(
                code="DPONE_DBT_INTERNAL",
                message="An unexpected workspace publication failure was redacted",
                path=output_dir.as_posix(),
                remediation="Preserve any existing output and provide this non-secret report and dpone version to the platform owner.",
            )
        return _compile_failure(check, output_dir, issue)


def _compile_failure(
    check: DbtWorkspaceCheckReport, output_dir: Path, issue: DbtPublishIssue
) -> DbtWorkspaceCompileReport:
    return DbtWorkspaceCompileReport(check, output_dir.as_posix(), blockers=(issue,))


def _global_identities(projects: tuple[DbtWorkspaceProjectCheck, ...]) -> tuple[DbtPublishIssue, ...]:
    """Node IDs are project-scoped; generated workflow/DAG/workload IDs are not."""

    owners: dict[tuple[str, str], str] = {}
    issues: list[DbtPublishIssue] = []
    for project in projects:
        identities = [("workload", model.workload_id) for model in project.report.models]
        for workflow in project.report.workflows:
            identities.extend(
                (("workflow", workflow.workflow), ("dag", workflow.dag_id), ("workload", f"dbt__{workflow.workflow}"))
            )
        for identity in identities:
            previous = owners.get(identity)
            if previous is not None:
                issues.append(
                    DbtPublishIssue(
                        code="DPONE_DBT_WORKSPACE_IDENTITY_COLLISION",
                        path=project.project.project_path,
                        message=f"Global {identity[0]} {identity[1]} is owned by both {previous} and {project.project.project_path}",
                        remediation="Resolve the conflicting declarations before publishing; existing deployed DAG IDs must not be automatically renamed.",
                    )
                )
            else:
                owners[identity] = project.project.project_path
    return tuple(issues)
