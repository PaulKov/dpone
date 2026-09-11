"""Retained diagnostic bytes, portable bundles and atomic comparison output.

This I/O service never evaluates correctness, benchmark identity or acceptance.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


class BenchmarkInputError(ValueError):
    """Stable sanitized input failure; never include dataset or exception text."""


def content_sha256(content: bytes) -> str:
    """Hash retained bytes exactly, without reparsing or reserializing."""
    return hashlib.sha256(content).hexdigest()


def canonical_json(payload: Any) -> bytes:
    """Canonical UTF-8 JSON with finite numbers; usable by independent producers."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BenchmarkInputError("duplicate_json_key")
        result[key] = value
    return result


class BenchmarkArtifacts:
    """Retained bytes and paths for hashing, identity checks, and output protection."""

    def __init__(self) -> None:
        self.bytes: dict[Path, bytes] = {}
        self.aliases: dict[Path, Path] = {}
        self.references: list[tuple[dict[str, Any], Path]] = []

    def read(self, path: Path) -> bytes:
        resolved = path.resolve(strict=True)
        logical = path.absolute()
        if logical in self.aliases and self.aliases[logical] != resolved:
            raise BenchmarkInputError("artifact_changed")
        self.aliases[logical] = resolved
        content = resolved.read_bytes()
        if resolved in self.bytes and self.bytes[resolved] != content:
            raise BenchmarkInputError("artifact_changed")
        self.bytes[resolved] = content
        return content

    def json(self, path: Path) -> Any:
        try:
            payload = json.loads(self.read(path), object_pairs_hook=_unique_object)
            canonical_json(payload)
            return payload
        except (UnicodeError, ValueError, RecursionError):
            raise BenchmarkInputError("invalid_json") from None

    def reference(self, root: Path, ref: dict[str, Any]) -> Path:
        relative = Path(ref["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise BenchmarkInputError("artifact_path_escape")
        resolved = (root / relative).resolve(strict=True)
        if not resolved.is_relative_to(root.resolve()):
            raise BenchmarkInputError("artifact_path_escape")
        if content_sha256(self.read(resolved)) != ref["sha256"]:
            raise BenchmarkInputError("artifact_hash_mismatch")
        self.read(root / relative)
        return (root / relative).absolute()

    def export(self, path: Path, status: str) -> dict[str, Any]:
        reference = {
            "path": str(path.absolute()),
            "sha256": content_sha256(self.bytes[path.resolve()]),
            "status": status,
        }
        self.references.append((reference, path.absolute()))
        return reference

    def relativize(self, root: Path, input_roots: tuple[Path, Path], *, retain: bool) -> None:
        """Keep references beneath the report, copying evidence only when needed."""
        destinations = {}
        for source in input_roots:
            source = source.resolve()
            if source.is_relative_to(root):
                destinations[source] = source
                continue
            files = {str(p.relative_to(source)): data for p, data in self.bytes.items() if p.is_relative_to(source)}
            files.update(
                {
                    str(alias.relative_to(source)): self.bytes[target]
                    for alias, target in self.aliases.items()
                    if alias.is_relative_to(source)
                }
            )
            digest = content_sha256(canonical_json({name: content_sha256(data) for name, data in files.items()}))
            destination = root / ("native-delivery-evidence-" + digest)
            if retain:
                if destination.exists() or destination.is_symlink():
                    if destination.is_symlink() or not destination.is_dir():
                        raise BenchmarkInputError("retained_bundle_conflict")
                    for name, data in files.items():
                        target = destination / name
                        if target.resolve() != target.absolute() or target.read_bytes() != data:
                            raise BenchmarkInputError("retained_bundle_conflict")
                else:
                    with tempfile.TemporaryDirectory(dir=root, prefix=".native-delivery-") as temporary:
                        bundle = Path(temporary) / "bundle"
                        bundle.mkdir()
                        for name, data in files.items():
                            target = bundle / name
                            target.parent.mkdir(parents=True, exist_ok=True)
                            target.write_bytes(data)
                        os.rename(bundle, destination)
            destinations[source] = destination
        for reference, original in self.references:
            source = max((p for p in destinations if original.is_relative_to(p)), key=lambda p: len(p.parts))
            reference["path"] = str((destinations[source] / original.relative_to(source)).relative_to(root))


def check_output(path: Path, store: BenchmarkArtifacts, *, overwrite: bool) -> None:
    if (
        path.is_symlink()
        or path.resolve() in store.bytes
        or (path.exists() and any(path.samefile(p) for p in store.bytes))
    ):
        raise BenchmarkInputError("output_aliases_evidence")
    if path.exists() and not overwrite:
        raise BenchmarkInputError("output_exists_use_overwrite")
    if any(alias.resolve() != target for alias, target in store.aliases.items()):
        raise BenchmarkInputError("artifact_changed")
    for retained, content in store.bytes.items():
        if retained.read_bytes() != content:
            raise BenchmarkInputError("artifact_changed")


def write_report(path: Path, report: dict[str, Any], store: BenchmarkArtifacts, *, overwrite: bool) -> None:
    check_output(path, store, overwrite=overwrite)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=".native-delivery-", delete=False
        ) as stream:
            temporary = stream.name
            stream.write(canonical_json(report) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)  # Atomic no-clobber publication, including concurrent writers.
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
