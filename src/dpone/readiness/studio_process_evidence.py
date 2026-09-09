"""Project-confined evidence references for resolved Studio processes."""

from __future__ import annotations

import copy
import os
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.models import ProcessSpec
from dpone.readiness.studio_errors import StudioError
from dpone.readiness.studio_manifest_validation import MAX_STUDIO_MANIFEST_BYTES


class StudioProcessEvidencePolicy:
    """Validate and optionally snapshot process evidence under one project root."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve(strict=True)

    def validate_processes(
        self,
        processes: Sequence[ProcessSpec],
        *,
        manifest_dir: Path,
    ) -> None:
        for process in processes:
            self._confined_artifact(process, manifest_dir=manifest_dir)

    def snapshot(
        self,
        process: ProcessSpec,
        *,
        manifest_dir: Path,
        snapshot_root: Path,
    ) -> ProcessSpec:
        content = self._confined_artifact(process, manifest_dir=manifest_dir)
        if content is None:
            return process
        raw = copy.deepcopy(dict(process.raw_config))
        certification = _native_transfer_certification(raw)
        assert certification is not None
        snapshot_path = snapshot_root / "certification.json"
        snapshot_path.write_bytes(content)
        certification["artifact"] = str(snapshot_path)
        return replace(process, raw_config=raw)

    def _confined_artifact(
        self,
        process: ProcessSpec,
        *,
        manifest_dir: Path,
    ) -> bytes | None:
        certification = _native_transfer_certification(process.raw_config)
        if certification is None or "artifact" not in certification:
            return None
        artifact = certification.get("artifact")
        if not isinstance(artifact, str) or not artifact.strip():
            raise _unsafe_artifact()
        artifact_path = Path(artifact)
        if artifact_path.is_absolute():
            raise _unsafe_artifact()
        candidate = Path(os.path.normpath(manifest_dir / artifact_path))
        try:
            relative = candidate.relative_to(self._root).as_posix()
            return read_confined_file(
                self._root,
                relative,
                max_bytes=MAX_STUDIO_MANIFEST_BYTES,
            )
        except (ConfinedFileError, ValueError) as exc:
            raise _unsafe_artifact() from exc


def _native_transfer_certification(
    process: Mapping[str, Any],
) -> dict[str, Any] | None:
    source = process.get("source")
    options = source.get("options") if isinstance(source, Mapping) else None
    native_transfer = options.get("native_transfer") if isinstance(options, Mapping) else None
    execution = native_transfer.get("execution") if isinstance(native_transfer, Mapping) else None
    certification = execution.get("certification") if isinstance(execution, Mapping) else None
    return certification if isinstance(certification, dict) else None


def _unsafe_artifact() -> StudioError:
    return StudioError(
        "DPONE_STUDIO_PLAN_ARTIFACT_UNSAFE",
        "Certification artifact references must be bounded regular files inside the project root.",
    )


__all__ = ["StudioProcessEvidencePolicy"]
