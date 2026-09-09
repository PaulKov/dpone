"""Project-local, bounded inputs for a workspace's canonical dbt compiler."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.contracts.dbt_runtime_payloads import dbt_runtime_payload_read_limit
from dpone.contracts.strict_json import strict_json_object
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry

if TYPE_CHECKING:
    from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
    from dpone.contracts.dbt_publish_models import DbtManifestArtifact
    from dpone.contracts.dbt_workspace import DbtWorkspaceProject


class ConfinedDbtWorkspaceInputs:
    """Implement artifact/profile acquisition for one explicitly selected project.

    Policy values and raw parser exceptions are never placed in diagnostics.
    The reusable canonical decoders own all schema and intent interpretation.
    There is no global profile fallback or implicit dependency installation.
    """

    def __init__(self, *, root: Path, project: DbtWorkspaceProject, reader: DbtArtifactReader) -> None:
        self._root = root.absolute()
        self._project = project
        self._reader = reader
        self._manifest_bytes: bytes | None = None

    def read(self, path: str | Path) -> tuple[DbtManifestArtifact | None, tuple[DbtPublishIssue, ...]]:
        self._manifest_bytes = None
        try:
            relative = self._relative(self._project.manifest_path)
            self._require_path(path, relative)
            payload = read_confined_file(self._root, relative, max_bytes=dbt_runtime_payload_read_limit("dbt_manifest"))
            artifact, issues = self._reader.read_payload(payload, path=self._root / relative)
            if artifact is not None and artifact.project_name != self._project.project_name:
                raise ValueError("manifest project differs from discovered project")
            if artifact is not None and not any(issue.severity == "error" for issue in issues):
                self._manifest_bytes = payload
            return artifact, issues
        except ConfinedFileError as exc:
            code = "DPONE_DBT_MANIFEST_MISSING" if exc.code == "file_not_found" else "DPONE_DBT_MANIFEST_INVALID"
        except (OSError, ValueError, TypeError, RecursionError):
            code = "DPONE_DBT_MANIFEST_INVALID"
        return None, (
            self._issue(
                code,
                "Canonical manifest is missing, unsafe or belongs to another project",
                "Run the certified dbt parse in this project; keep the resulting manifest in its configured target directory.",
            ),
        )

    def load(
        self, manifest_path: str | Path, explicit_path: str | Path | None = None
    ) -> tuple[DbtPublishProfileRegistry | None, tuple[DbtPublishIssue, ...]]:
        try:
            self._require_path(manifest_path, self._relative(self._project.manifest_path))
            relative = self._relative(self._project.profiles_path)
            if explicit_path is not None:
                self._require_path(explicit_path, relative)
            content = read_confined_file(self._root, relative, max_bytes=1024 * 1024)
            registry, issues = DbtPublishProfileRegistry.from_mapping(load_bounded_yaml(content), source_path=relative)
            if registry is not None and not issues:
                return registry, ()
        except (OSError, ValueError, TypeError, RecursionError):
            pass
        return None, (
            self._issue(
                "DPONE_DBT_PROFILES_INVALID",
                "Project-local publishing policy is missing, unsafe or invalid",
                "Correct the selected standard policy file using the published schema; global or foreign-project profiles are not accepted.",
            ),
        )

    def _relative(self, path: str | None) -> str:
        if path is None:
            raise ValueError("project input is not configured")
        return (Path(self._project.project_path) / path).as_posix()

    def graph_manifest(self, path: Path) -> dict[str, Any]:
        """Use the validated snapshot and reject changes between compilation phases."""

        relative = self._relative(self._project.manifest_path)
        self._require_path(path, relative)
        if self._manifest_bytes is None:
            raise ValueError("graph validation requires a successfully acquired manifest")
        current = read_confined_file(self._root, relative, max_bytes=dbt_runtime_payload_read_limit("dbt_manifest"))
        if current != self._manifest_bytes:
            raise ValueError("manifest changed between artifact and graph validation")
        return strict_json_object(self._manifest_bytes)

    def _require_path(self, path: str | Path, relative: str) -> None:
        if Path(path).absolute() != self._root / relative:
            raise ValueError("input does not belong to the selected project")

    def _issue(self, code: str, message: str, remediation: str) -> DbtPublishIssue:
        return DbtPublishIssue(code=code, message=message, path=self._project.project_path, remediation=remediation)
