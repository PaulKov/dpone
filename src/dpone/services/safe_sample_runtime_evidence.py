"""Atomic evidence writer for safe sample runtime results."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from dpone.services.safe_sample_redaction import redact_safe_sample_value

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutionResult

_DEFAULT_FILENAME = "safe-sample-runtime-execution.json"
_OUTPUT_ROOT = "$OUTPUT_ROOT"


@dataclass(frozen=True, slots=True)
class SafeSampleRuntimeEvidenceWriteReport:
    path: str
    sha256: str
    bytes: int
    release_id: str | None
    deployment_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.safe-sample-runtime-evidence-write.v1",
            "path": self.path,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
        }


class SafeSampleRuntimeEvidenceWriter:
    """Persist safe sample runtime evidence without leaking secret-like fields."""

    def write(
        self,
        result: SafeSampleRuntimeExecutionResult | Mapping[str, Any],
        output_dir: str | Path,
        *,
        filename: str = _DEFAULT_FILENAME,
    ) -> SafeSampleRuntimeEvidenceWriteReport:
        payload = redact_safe_sample_value(_payload(result))
        relative_path = _safe_relative_output_path(filename)
        output_path = Path(output_dir).joinpath(*relative_path.parts)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data = _encode(payload)
        _write_create_only_atomic(output_path, data)
        return SafeSampleRuntimeEvidenceWriteReport(
            path=_public_output_locator(output_path, relative_path=relative_path),
            sha256="sha256:" + hashlib.sha256(data).hexdigest(),
            bytes=len(data),
            release_id=_optional_string(payload.get("release_id")),
            deployment_id=_optional_string(payload.get("deployment_id")),
        )


def _payload(result: SafeSampleRuntimeExecutionResult | Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(result, "to_dict"):
        value = result.to_dict()
    else:
        value = dict(result)
    return value if isinstance(value, dict) else {}


def _encode(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _optional_string(value: Any) -> str | None:
    text = str(value or "")
    return text or None


def _safe_relative_output_path(value: str) -> PurePosixPath:
    raw = str(value or "")
    text = raw.strip()
    candidate = PurePosixPath(text)
    parts = text.split("/")
    if (
        not text
        or text != raw
        or "\\" in text
        or candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in parts)
        or any(":" in part or any(char.isspace() or ord(char) < 32 for char in part) for part in parts)
    ):
        raise ValueError("evidence filename must be a safe relative path")
    return candidate


def _public_output_locator(output_path: Path, *, relative_path: PurePosixPath) -> str:
    try:
        return output_path.resolve(strict=False).relative_to(Path.cwd().resolve(strict=False)).as_posix()
    except ValueError:
        return f"{_OUTPUT_ROOT}/{relative_path.as_posix()}"


def _write_create_only_atomic(output_path: Path, data: bytes) -> None:
    """Publish complete evidence exactly once without replacing another run."""

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary_path, output_path)
        _fsync_directory(output_path.parent)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


__all__ = [
    "SafeSampleRuntimeEvidenceWriteReport",
    "SafeSampleRuntimeEvidenceWriter",
]
