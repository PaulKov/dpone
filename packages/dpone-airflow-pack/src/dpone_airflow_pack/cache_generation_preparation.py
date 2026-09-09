"""Bounded staging and verification of one legacy cache generation."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from dpone_airflow_pack.artifact_store import ArtifactReadPort
from dpone_airflow_pack.cache_generation_files import pack_file_sha256, remove_tree
from dpone_airflow_pack.cache_generation_lease import (
    exclusive_stage_detach_lease,
    generation_stage_lease,
)
from dpone_airflow_pack.cache_generation_store import (
    STAGE_MARKER_NAME,
    GenerationCandidate,
    create_generation_stage,
    finalize_generation_stage,
    heartbeat_generation_stage,
)
from dpone_airflow_pack.cache_generation_writer import write_generation_file_exclusive
from dpone_airflow_pack.pack_index import dag_spec_index_entries, index_entries, safe_pack_relative_path
from dpone_airflow_pack.pack_index_security import (
    assert_path_under_cache_dir,
    normalize_index_checksum,
    require_entry_sha256,
    resolve_entry_uri,
)


class GenerationPreparationOptions(Protocol):
    @property
    def index_uri(self) -> str: ...

    @property
    def cache_dir(self) -> Path: ...

    @property
    def max_pack_bytes(self) -> int | None: ...


@dataclass(slots=True)
class PreparedGeneration:
    candidate: GenerationCandidate
    downloaded_packs: int
    downloaded_dag_specs: int
    warnings: list[str] = field(default_factory=list)


@contextmanager
def prepared_generation(
    *,
    options: GenerationPreparationOptions,
    reader: ArtifactReadPort,
    index_bytes: bytes,
    index: Mapping[str, Any],
    generation: str,
) -> Iterator[PreparedGeneration]:
    """Keep stage ownership through promotion and remove the stage on every exit."""

    _validate_destination_inventory(index)
    stage = create_generation_stage(options.cache_dir, generation=generation)
    assert_path_under_cache_dir(stage, cache_dir=options.cache_dir)
    preparation: PreparedGeneration | None = None
    try:
        with generation_stage_lease(stage, marker_name=STAGE_MARKER_NAME):
            write_generation_file_exclusive(stage, "pack-index.json", index_bytes)
            downloaded_packs = _download_entries(
                reader=reader,
                options=options,
                index=index,
                generation=generation,
                stage=stage,
                entries=index_entries(index),
                artifact_kind="pack",
            )
            downloaded_specs = _download_entries(
                reader=reader,
                options=options,
                index=index,
                generation=generation,
                stage=stage,
                entries=dag_spec_index_entries(index),
                artifact_kind="dag_spec",
            )
            heartbeat_generation_stage(stage)
            preparation = PreparedGeneration(
                candidate=finalize_generation_stage(stage, generation=generation),
                downloaded_packs=downloaded_packs,
                downloaded_dag_specs=downloaded_specs,
            )
            if downloaded_packs == 0:
                if dag_spec_index_entries(index):
                    preparation.warnings.append("dag_specs_only_index_no_workload_packs")
                elif not index_entries(index):
                    preparation.warnings.append("empty_workload_pack_index")
            yield preparation
    finally:
        if stage.exists():
            try:
                with exclusive_stage_detach_lease(stage, marker_name=STAGE_MARKER_NAME) as lease:
                    if not lease.acquired:
                        raise OSError("airflow_pack_cache_stage_cleanup_lease_busy")
                    remove_tree(stage)
                    lease.mark_detached()
            except (OSError, TypeError, ValueError):
                if preparation is not None:
                    preparation.warnings.append("cache_stage_cleanup_failed")


def _download_entries(
    *,
    reader: ArtifactReadPort,
    options: GenerationPreparationOptions,
    index: Mapping[str, Any],
    generation: str,
    stage: Path,
    entries: tuple[Any, ...],
    artifact_kind: str,
) -> int:
    downloaded = 0
    for entry in entries:
        relative_path = safe_pack_relative_path(entry)
        if relative_path is None:
            raise ValueError(f"Unsafe {artifact_kind} path in index: {entry.path}")
        expected_sha256 = require_entry_sha256(entry, artifact_kind=artifact_kind)
        raw = reader.read_bytes(
            resolve_entry_uri(index_uri=options.index_uri, generation=generation, entry=entry),
            max_bytes=options.max_pack_bytes,
        )
        _check_size(raw, options.max_pack_bytes, f"airflow_{artifact_kind}_too_large")
        _check_declared_size(raw, entry.bytes, f"airflow_{artifact_kind}_size_mismatch")
        path = write_generation_file_exclusive(stage, relative_path, raw)
        if pack_file_sha256(path) != normalize_index_checksum(expected_sha256):
            raise ValueError(f"{artifact_kind} checksum mismatch for {entry.workload_id}")
        downloaded += 1
        heartbeat_generation_stage(stage)
    return downloaded


def _check_size(payload: bytes, max_bytes: int | None, code: str) -> None:
    if max_bytes is not None and max_bytes > 0 and len(payload) > max_bytes:
        raise ValueError(f"{code}: {len(payload)} > {max_bytes}")


def _check_declared_size(payload: bytes, declared_bytes: int | None, code: str) -> None:
    if declared_bytes is not None and len(payload) != declared_bytes:
        raise ValueError(f"{code}: {len(payload)} != {declared_bytes}")


def _validate_destination_inventory(index: Mapping[str, Any]) -> None:
    destinations: dict[str, str] = {"pack-index.json": "control"}
    for artifact_kind, entries in (
        ("pack", index_entries(index)),
        ("dag_spec", dag_spec_index_entries(index)),
    ):
        for entry in entries:
            relative_path = safe_pack_relative_path(entry)
            if relative_path is None or relative_path != entry.path:
                raise ValueError(f"airflow_{artifact_kind}_path_unsafe: {entry.path}")
            if any(part.startswith(".dpone-") for part in Path(relative_path).parts):
                raise ValueError(f"airflow_{artifact_kind}_path_reserved: {relative_path}")
            existing = destinations.get(relative_path)
            if existing is not None:
                raise ValueError(
                    f"airflow_pack_generation_path_collision: {relative_path} is used by {existing} and {artifact_kind}"
                )
            destinations[relative_path] = artifact_kind


__all__ = ["PreparedGeneration", "prepared_generation"]
