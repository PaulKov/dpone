from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from dpone.gitops.airflow_dag_spec_build_models import GitOpsAirflowDagSpecBuildReport
from dpone.gitops.paths import confined_repo_file_path, confined_repo_path


class AirflowDagSpecArtifactWriter:
    """Persist one validated DAG-spec report below a confined repository path."""

    def __init__(self, *, repo_root: Path) -> None:
        self._repo_root = repo_root.resolve(strict=False)

    def write(
        self,
        report: GitOpsAirflowDagSpecBuildReport,
        *,
        artifact_dir: str | Path,
    ) -> GitOpsAirflowDagSpecBuildReport:
        if not report.passed:
            return report
        relative_dir, spec_dir = self.validate_destination(report, artifact_dir=artifact_dir)
        for spec in report.specs:
            output = spec_dir / f"{spec.dag_id}.dag-spec.json"
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(spec.to_json(), encoding="utf-8")
        self._prune_stale(report, spec_dir=spec_dir)
        return replace(report, artifact_dir=relative_dir.as_posix())

    def validate_destination(
        self,
        report: GitOpsAirflowDagSpecBuildReport,
        *,
        artifact_dir: str | Path,
    ) -> tuple[Path, Path]:
        """Validate the directory and every future artifact before any write."""

        relative_dir, spec_dir = confined_repo_path(
            self._repo_root,
            artifact_dir,
            source="--output-dir",
        )
        for spec in report.specs:
            confined_repo_file_path(
                self._repo_root,
                relative_dir / f"{spec.dag_id}.dag-spec.json",
                source="--output-dir",
            )
        return relative_dir, spec_dir

    def planned_paths(
        self,
        report: GitOpsAirflowDagSpecBuildReport,
        *,
        artifact_dir: str | Path,
    ) -> tuple[str, ...]:
        """Return validated repository-relative paths without writing artifacts."""

        relative_dir, _spec_dir = self.validate_destination(report, artifact_dir=artifact_dir)
        return tuple((relative_dir / f"{spec.dag_id}.dag-spec.json").as_posix() for spec in report.specs)

    @staticmethod
    def _prune_stale(report: GitOpsAirflowDagSpecBuildReport, *, spec_dir: Path) -> None:
        if not spec_dir.is_dir():
            return
        current = {spec.dag_id for spec in report.specs}
        for path in spec_dir.glob("*.dag-spec.json"):
            dag_id = path.name[: -len(".dag-spec.json")]
            if dag_id not in current:
                path.unlink(missing_ok=True)


__all__ = ["AirflowDagSpecArtifactWriter"]
