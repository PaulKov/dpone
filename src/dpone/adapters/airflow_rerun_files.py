"""No-follow local filesystem adapter for Airflow rerun planning."""

from __future__ import annotations

from itertools import islice
from pathlib import Path
from typing import Any

from dpone.adapters.deployment_cache_files import (
    DeploymentCacheError,
    atomic_write_json,
    read_regular_json_object,
    resolve_relative_current_symlink,
)
from dpone.ports.airflow_rerun import AirflowRerunPlanInputError, AirflowRerunPlanInputs


class LocalAirflowRerunPlanInputAdapter:
    """Read bounded local evidence and deployment projections without network I/O."""

    def load(
        self,
        *,
        evidence_path: str | Path,
        current_index_path: str | Path,
        cache_root: str | Path | None,
    ) -> AirflowRerunPlanInputs:
        evidence_file = Path(evidence_path).absolute()
        inferred_root = _cache_root(Path(current_index_path), explicit=cache_root)
        index_file = _resolved_index_path(Path(current_index_path), cache_root=inferred_root)
        return AirflowRerunPlanInputs(
            evidence_path=evidence_file,
            evidence=_read_input(
                evidence_file,
                root=evidence_file.parent.resolve(strict=False),
                missing_code="DPONE_AIRFLOW_EVIDENCE_NOT_FOUND",
                invalid_code="DPONE_AIRFLOW_EVIDENCE_INVALID",
                label="Airflow evidence bundle",
            ),
            index_path=index_file,
            current_index=_read_input(
                index_file,
                root=inferred_root,
                missing_code="DPONE_AIRFLOW_INDEX_NOT_FOUND",
                invalid_code="DPONE_AIRFLOW_INDEX_INVALID",
                label="Airflow deployment index",
            ),
            cache_root=inferred_root,
        )

    def original_artifacts_availability(
        self,
        cache_root: Path,
        *,
        release_id: str,
        deployment_id: str,
    ) -> tuple[bool, str]:
        release_dir = cache_root / "releases" / release_id.replace(":", "-")
        if release_dir.is_symlink() or not release_dir.is_dir():
            return False, "DPONE_RELEASE_EXPIRED"
        release_set = release_dir / "release-set.json"
        if release_set.is_symlink() or not release_set.is_file():
            return False, "DPONE_RELEASE_NOT_FOUND"
        deployment_dir_name = deployment_id.replace(":", "-")
        deployment_seen = False
        for section in ("deployments", "activations"):
            section_root = cache_root / section
            if not section_root.is_dir():
                continue
            for environment in islice(section_root.iterdir(), 256):
                if environment.is_symlink() or not environment.is_dir():
                    continue
                deployment = environment / deployment_dir_name
                if deployment.is_symlink() or not deployment.is_dir():
                    continue
                deployment_seen = True
                candidate = deployment / "airflow-index.json"
                success = deployment / "_SUCCESS"
                if success.is_symlink() or candidate.is_symlink() or not success.is_file() or not candidate.is_file():
                    continue
                if _deployment_index_matches(candidate, cache_root, release_id, deployment_id):
                    return True, ""
        if deployment_seen:
            return False, "DPONE_DEPLOYMENT_INCOMPLETE"
        return False, "DPONE_DEPLOYMENT_EXPIRED"


class LocalAirflowRerunPlanOutputAdapter:
    """Atomically persist a complete rerun plan on the local filesystem."""

    def write(self, path: str | Path, payload: dict[str, Any]) -> None:
        atomic_write_json(Path(path), payload)


def _read_input(
    path: Path,
    *,
    root: Path,
    missing_code: str,
    invalid_code: str,
    label: str,
) -> dict[str, Any]:
    try:
        return read_regular_json_object(
            path,
            missing_code=missing_code,
            invalid_code=invalid_code,
            label=label,
            root=root,
        )
    except DeploymentCacheError as exc:
        raise AirflowRerunPlanInputError(exc.code, str(exc), path=exc.path or path.as_posix()) from exc


def _cache_root(index_path: Path, *, explicit: str | Path | None) -> Path:
    if explicit is not None:
        return Path(explicit).resolve(strict=False)
    absolute = index_path.absolute()
    for parent in absolute.parents:
        if parent.name == ".dpone-cache":
            return parent.resolve(strict=False)
    if absolute.parent.name == "current":
        return absolute.parent.parent.resolve(strict=False)
    return absolute.parent.resolve(strict=False)


def _resolved_index_path(index_path: Path, *, cache_root: Path) -> Path:
    absolute = index_path.absolute()
    if absolute.parent.name != "current":
        return absolute.resolve(strict=False)
    try:
        active = resolve_relative_current_symlink(cache_root)
    except DeploymentCacheError as exc:
        raise AirflowRerunPlanInputError(exc.code, str(exc), path=exc.path or absolute.as_posix()) from exc
    return active / absolute.name


def _deployment_index_matches(path: Path, cache_root: Path, release_id: str, deployment_id: str) -> bool:
    try:
        payload = read_regular_json_object(
            path,
            missing_code="DPONE_DEPLOYMENT_NOT_FOUND",
            invalid_code="DPONE_DEPLOYMENT_INCOMPLETE",
            label="retained Airflow deployment index",
            root=cache_root,
        )
    except DeploymentCacheError:
        return False
    return payload.get("release_id") == release_id and payload.get("deployment_id") == deployment_id


__all__ = ["LocalAirflowRerunPlanInputAdapter", "LocalAirflowRerunPlanOutputAdapter"]
