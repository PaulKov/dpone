"""Capture every checked project, verify one owned stage, publish exactly once."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from dpone.contracts.dbt_contract_validation import DbtPublishingError, sha256_bytes
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_read_limit
from dpone.contracts.dbt_workspace_paths import validate_dbt_project_layout
from dpone.contracts.dbt_workspace_release import DbtWorkspaceProjectArtifacts, DbtWorkspaceReleaseTree
from dpone.manifest.dbt_workspace_profiles import read_workspace_parse_profile, require_parse_profile_targets
from dpone.services.dbt_release_integrity import DBT_RELEASE_SUBJECTS_FILENAME, DbtReleaseIntegrityService
from dpone.services.dbt_workspace_release_assembly import assemble_workspace_release

if TYPE_CHECKING:
    from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport
    from dpone.ports.dbt_publishing import DbtProfileStore
    from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
    from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactTreePublisher
    from dpone.services.dbt_project_artifacts import DbtProjectArtifactProjector
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader


class DbtWorkspaceArtifactWriter:
    """No active-output side effects until every project and source has passed.

    All infrastructure is injected. This writer neither installs dependencies nor
    changes images, signs releases, resolves physical targets or executes SQL.
    It returns the exact published file map, including its integrity subject.
    """

    def __init__(
        self,
        *,
        projector: DbtProjectArtifactProjector,
        source_reader: DbtReleaseSourceReader,
        integrity: DbtReleaseIntegrityService,
        publisher: DbtArtifactTreePublisher,
        producer_version: str,
        profile_store: DbtProfileStore,
        read_file: ConfinedReleaseFileReader,
        dbt_profiles_dir: Path | None = None,
    ) -> None:
        self._projector = projector
        self._sources = source_reader
        self._integrity = integrity
        self._publisher = publisher
        self._version = producer_version
        self._profile_store = profile_store
        self._read_file = read_file
        self._profiles_dir = dbt_profiles_dir.absolute() if dbt_profiles_dir is not None else None

    def write(self, check: DbtWorkspaceCheckReport, *, root: Path, output_dir: Path) -> DbtWorkspaceReleaseTree:
        """Reacquire bounded manifest snapshots; never reread unchecked manifest paths."""

        _require_complete_check(check)
        root = root.absolute()
        with ExitStack() as profiles:
            profile_dirs = self._capture_profiles(check, root=root, stack=profiles)
            projects = self._project_all(check, root=root, profile_dirs=profile_dirs)
        # Profile cleanup must succeed before publication. Otherwise a cleanup
        # error could report validation failure after making output active.
        tree = assemble_workspace_release(check, projects, producer_version=self._version)
        files = dict(tree.files)
        with TemporaryDirectory(prefix="dpone-dbt-workspace-release-") as temporary:
            stage = Path(temporary)
            for path, body in files.items():
                destination = stage / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
            # Source verification includes independent exact file/directory
            # closure, all descriptor bytes, selections, packs and DAGs.
            self._sources.read(stage, expected_release_id=tree.release_id)
            self._integrity.write(stage)
            self._integrity.verify(stage)
            files[DBT_RELEASE_SUBJECTS_FILENAME] = self._read_file(
                stage, DBT_RELEASE_SUBJECTS_FILENAME, max_bytes=8 * 1024 * 1024
            )
        self._publisher.publish(output_dir, files)
        return replace(tree, files=files)

    def _capture_profiles(self, check: DbtWorkspaceCheckReport, *, root: Path, stack: ExitStack) -> dict[str, Path]:
        """Freeze shared bytes once, or separate local bytes before projection."""

        captured: dict[Path, tuple[bytes, Path]] = {}
        directories: dict[str, Path] = {}
        for row in check.projects:
            project = row.project.project_path
            source = self._profiles_dir if self._profiles_dir is not None else root / project
            if source not in captured:
                content = read_workspace_parse_profile(root, project, profiles_dir=self._profiles_dir)
                snapshot = stack.enter_context(self._profile_store.materialize(content))
                captured[source] = content, snapshot.parent
            content, directory = captured[source]
            names = tuple(
                (
                    str(workflow.models[0].profile.runtime.get("dbt_profile") or "dpone_runtime"),
                    str(workflow.models[0].profile.runtime.get("dbt_target") or "runtime"),
                )
                for workflow in row.report.workflows
            )
            require_parse_profile_targets(content, names, project_path=project)
            directories[project] = directory
        return directories

    def _project_all(
        self, check: DbtWorkspaceCheckReport, *, root: Path, profile_dirs: dict[str, Path]
    ) -> list[DbtWorkspaceProjectArtifacts]:
        projects = []
        for row in sorted(check.projects, key=lambda item: item.project.project_path):
            project = row.project
            project_root = root / project.project_path
            if project.manifest_path is None:
                raise ValueError("checked project manifest path is missing")
            relative = (Path(project.project_path) / project.manifest_path).as_posix()
            if Path(row.report.manifest_path).absolute() != root / relative:
                raise ValueError("checked manifest path differs from project discovery")
            manifest = self._read_file(root, relative, max_bytes=dbt_runtime_payload_read_limit("dbt_manifest"))
            if sha256_bytes(manifest) != row.report.manifest_sha256:
                raise ValueError("project manifest changed after workspace check")
            try:
                artifacts = self._projector.project(
                    row.report,
                    project_root=project_root,
                    wire_contract=DBT_RUNTIME_WIRE_V2,
                    manifest_payload=manifest,
                    profiles_dir=profile_dirs[project.project_path],
                )
            except DbtPublishingError as exc:
                if exc.code != "DPONE_DBT_PROFILE_INVALID":
                    raise
                raise DbtPublishingError(
                    "DPONE_DBT_WORKSPACE_PARSE_PROFILE_INVALID",
                    "Canonical dbt parse profile target could not be resolved safely",
                    path=project.project_path,
                    remediation="Check the selected profile/output using non-secret parse credentials and the certified toolchain.",
                ) from None
            projects.append(DbtWorkspaceProjectArtifacts(row, artifacts))
        return projects


def _require_complete_check(check: DbtWorkspaceCheckReport) -> None:
    if not check.passed or not check.projects:
        raise ValueError("workspace publication requires a complete nonempty successful check")
    discovered = {row.project_path: row for row in check.discovery.projects if row.publishing}
    checked = {row.project.project_path: row.project for row in check.projects}
    if (
        checked != discovered
        or len(checked) != len(check.projects)
        or len(discovered) != sum(row.publishing for row in check.discovery.projects)
    ):
        raise ValueError("workspace check does not cover the complete publishing inventory")
    if any(row.project_name is None for row in checked.values()):
        raise ValueError("checked workspace project name is missing")
    validate_dbt_project_layout(tuple((path, str(row.project_name)) for path, row in checked.items()))


__all__ = ["DbtWorkspaceArtifactWriter"]
