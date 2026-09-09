"""Resolve dag-spec artifact paths from pack cache or gitops tree."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dpone_airflow_pack.cache_activation_contract import cache_read_lease
from dpone_airflow_pack.cache_artifact_contract import read_confined_cache_file
from dpone_airflow_pack.cache_authority import (
    read_current_generation,
    read_current_symlink_generation,
    read_legacy_cache_authority,
)
from dpone_airflow_pack.cache_generation_files import file_sha256
from dpone_airflow_pack.cache_layout import (
    LAYOUT_MARKER_NAME,
    LEGACY_PACK_INDEX_LAYOUT,
    CacheLayoutError,
    read_cache_layout,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.pack_index import AirflowPackIndexEntry, dag_spec_index_entries, safe_pack_relative_path
from dpone_airflow_pack.pack_index_security import (
    normalize_index_checksum,
    require_entry_sha256,
    validate_index_generation,
)
from dpone_airflow_pack.pack_storage_consumer import (
    DagSpecCacheMissingError,
    allows_git_fallback_on_cache_miss,
    pack_storage_mode_from_environ,
)

DAG_SPEC_DIRNAME = "_dags"
_MAX_PACK_INDEX_BYTES = 25 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class PinnedDagSpec:
    """One DAG spec pinned to the index identity authorized by the cache receipt."""

    path: Path
    expected_sha256: str | None = None
    expected_bytes: int | None = None
    expected_dag_id: str | None = None
    confined_root: Path | None = None


def discover_dag_spec_paths(repo_root: Path) -> tuple[Path, ...]:
    """Discover dag-spec files from scheduler cache (remote) or gitops tree.

    In ``remote`` storage mode, a missing/invalid generation or an empty ``_dags``
    directory fails closed with ``dag_spec_cache_missing`` — never silent gitops
    fallthrough (empty parse would look like a successful zero-DAG deployment).
    """

    with pinned_dag_spec_paths(repo_root) as paths:
        return paths


@contextmanager
def pinned_dag_spec_paths(repo_root: Path) -> Iterator[tuple[Path, ...]]:
    """Pin one receipt-authorized generation for the complete DAG-spec read."""

    cache_dir = os.environ.get("DPONE_AIRFLOW_PACK_CACHE_DIR", "").strip()
    mode = pack_storage_mode_from_environ()
    allow_git = allows_git_fallback_on_cache_miss()

    if cache_dir:
        cache_root = Path(cache_dir)
        try:
            managed = (cache_root / LAYOUT_MARKER_NAME).exists() or (cache_root / ".promotion.lock").exists()
            if not managed:
                generation_dir = _authorized_generation_dir(cache_root)
                specs = _dag_spec_paths_in_generation(generation_dir) if generation_dir is not None else ()
                if specs:
                    yield specs
                    return
                if mode == "remote" or not allow_git:
                    message = (
                        "cache generation has no dag-spec artifacts under _dags"
                        if generation_dir is not None
                        else "no valid cache generation under DPONE_AIRFLOW_PACK_CACHE_DIR"
                    )
                    raise DagSpecCacheMissingError(f"dag_spec_cache_missing: {message}")
            else:
                with cache_read_lease(cache_root) as lease:
                    if not lease.root_available:
                        raise DagSpecCacheMissingError("dag_spec_cache_missing: cache root is absent")
                    generation_dir = _authorized_generation_dir(cache_root)
                    specs = _dag_spec_paths_in_generation(generation_dir) if generation_dir is not None else ()
                    if specs:
                        yield specs
                        return
                    if mode == "remote" or not allow_git:
                        raise DagSpecCacheMissingError(
                            "dag_spec_cache_missing: cache generation has no dag-spec artifacts under _dags"
                        )
        except (CacheLayoutError, OSError, ValueError) as exc:
            if mode == "remote" or not allow_git:
                raise DagSpecCacheMissingError(
                    f"dag_spec_cache_missing: durable commit receipt is invalid: {exc}"
                ) from exc
    elif mode == "remote":
        raise DagSpecCacheMissingError(
            "dag_spec_cache_missing: DPONE_AIRFLOW_PACK_CACHE_DIR is required in remote storage mode"
        )

    root = repo_root / ".dpone" / "gitops" / "airflow" / DAG_SPEC_DIRNAME
    yield tuple(sorted(root.glob("*.dag-spec.json"))) if root.exists() else ()


@contextmanager
def pinned_dag_specs(repo_root: Path) -> Iterator[tuple[PinnedDagSpec, ...]]:
    """Pin DAG specs and bind managed cache files to their index digests."""

    with pinned_dag_spec_paths(repo_root) as paths:
        if not paths:
            yield ()
            return
        cache_dir = os.environ.get("DPONE_AIRFLOW_PACK_CACHE_DIR", "").strip()
        cache_root = Path(cache_dir) if cache_dir else None
        managed = cache_root is not None and (cache_root / LAYOUT_MARKER_NAME).exists()
        if not managed:
            yield tuple(PinnedDagSpec(path=path) for path in paths)
            return
        assert cache_root is not None
        generation_dir = _common_generation_dir(paths)
        entries = _indexed_dag_specs(cache_root, generation_dir)
        by_path: dict[str, AirflowPackIndexEntry] = {}
        for entry in entries:
            if entry.path in by_path:
                raise DagSpecCacheMissingError(
                    "dag_spec_cache_missing: duplicate DAG-spec destination in the authorized pack index"
                )
            by_path[entry.path] = entry
        actual = {path.relative_to(generation_dir).as_posix() for path in paths}
        if actual != set(by_path):
            raise DagSpecCacheMissingError(
                "dag_spec_cache_missing: cached DAG specs differ from the authorized pack index"
            )
        yield tuple(
            PinnedDagSpec(
                path=generation_dir / relative,
                expected_sha256="sha256:"
                + normalize_index_checksum(require_entry_sha256(by_path[relative], artifact_kind="dag_spec")),
                expected_bytes=by_path[relative].bytes,
                expected_dag_id=by_path[relative].workload_id,
                confined_root=cache_root,
            )
            for relative in sorted(actual)
        )


def _common_generation_dir(paths: tuple[Path, ...]) -> Path:
    for path in paths:
        for parent in path.parents:
            if parent.parent.name == "generations":
                if all(candidate.is_relative_to(parent) for candidate in paths):
                    return parent
                break
    raise DagSpecCacheMissingError("dag_spec_cache_missing: DAG specs are not confined to one generation")


def _indexed_dag_specs(cache_root: Path, generation_dir: Path) -> tuple[AirflowPackIndexEntry, ...]:
    try:
        raw = read_confined_cache_file(
            generation_dir / "pack-index.json",
            cache_root=cache_root,
            max_bytes=_MAX_PACK_INDEX_BYTES,
        )
        payload = json.loads(raw)
    except (AirflowDeploymentIndexError, OSError, TypeError, ValueError) as exc:
        raise DagSpecCacheMissingError("dag_spec_cache_missing: authorized pack index is unreadable") from exc
    if not isinstance(payload, dict):
        raise DagSpecCacheMissingError("dag_spec_cache_missing: authorized pack index must be an object")
    entries = dag_spec_index_entries(payload)
    destinations: set[str] = set()
    for entry in entries:
        relative = safe_pack_relative_path(entry)
        if relative is None or relative != entry.path:
            raise DagSpecCacheMissingError("dag_spec_cache_missing: pack index DAG-spec path is unsafe")
        if relative in destinations:
            raise DagSpecCacheMissingError("dag_spec_cache_missing: duplicate DAG-spec destination in pack index")
        destinations.add(relative)
    return entries


def _authorized_generation_dir(cache_root: Path) -> Path | None:
    layout = read_cache_layout(cache_root)
    if layout not in {None, LEGACY_PACK_INDEX_LAYOUT}:
        raise ValueError(f"cache layout {layout!r} is not a legacy pack cache")
    generation = read_current_generation(cache_root)
    if (cache_root / LAYOUT_MARKER_NAME).exists():
        authority = read_legacy_cache_authority(cache_root)
        receipt = authority.require_consistent_durable()
        generation_dir = _validated_generation_dir(cache_root, receipt.generation)
        actual_index = file_sha256(generation_dir / "pack-index.json")
        if receipt.index_sha256 != actual_index:
            raise ValueError("durable commit receipt differs from active pack index")
        return generation_dir
    if generation:
        return _validated_generation_dir(cache_root, generation)
    link_generation = read_current_symlink_generation(cache_root)
    if link_generation:
        return _validated_generation_dir(cache_root, link_generation)
    return None


def _validated_generation_dir(cache_root: Path, generation: str) -> Path:
    validate_index_generation(generation)
    path = cache_root / "generations" / generation
    if not path.is_dir() or path.is_symlink():
        raise ValueError("committed generation directory is missing or unsafe")
    return path


def _dag_spec_paths_in_generation(generation_dir: Path) -> tuple[Path, ...]:
    """Locate synced dag-spec files under a cache generation directory.

    Publisher indexes use ``airflow/_dags/*.dag-spec.json``; older indexes and
    local gitops trees use ``_dags/*.dag-spec.json`` at the generation root.
    """

    for remote_root in (
        generation_dir / DAG_SPEC_DIRNAME,
        generation_dir / "airflow" / DAG_SPEC_DIRNAME,
    ):
        if remote_root.is_dir():
            specs = tuple(sorted(remote_root.glob("*.dag-spec.json")))
            if specs:
                return specs
    for pattern in (
        f"{DAG_SPEC_DIRNAME}/*.dag-spec.json",
        f"airflow/{DAG_SPEC_DIRNAME}/*.dag-spec.json",
    ):
        specs = tuple(sorted(generation_dir.glob(pattern)))
        if specs:
            return specs
    return ()


__all__ = [
    "DAG_SPEC_DIRNAME",
    "PinnedDagSpec",
    "discover_dag_spec_paths",
    "pinned_dag_spec_paths",
    "pinned_dag_specs",
]
