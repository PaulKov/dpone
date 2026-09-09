"""Storage ports for data product audit archives."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol


class AuditArchiveStore(Protocol):
    @property
    def backend_name(self) -> str: ...

    def write_archive(
        self, *, archive_uri: str, manifest: Mapping[str, Any], artifacts: Mapping[str, bytes]
    ) -> str: ...

    def read_manifest(self, archive_uri: str) -> dict[str, Any]: ...

    def read_artifact(self, archive_uri: str, relative_path: str) -> bytes: ...


class LocalFsAuditArchiveStore:
    """Local filesystem archive store with temp-directory commit."""

    @property
    def backend_name(self) -> str:
        return "local_fs"

    def write_archive(self, *, archive_uri: str, manifest: Mapping[str, Any], artifacts: Mapping[str, bytes]) -> str:
        root = Path(archive_uri)
        tmp = root.with_name(f".{root.name}.tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        if root.exists():
            shutil.rmtree(root)
        (tmp / "artifacts").mkdir(parents=True, exist_ok=True)
        for relative, content in artifacts.items():
            target = tmp / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        (tmp / "archive-manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _write_index(tmp, manifest)
        tmp.replace(root)
        return str(root)

    def read_manifest(self, archive_uri: str) -> dict[str, Any]:
        path = Path(archive_uri) / "archive-manifest.json"
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise ValueError(f"{path} must contain an object")
        return dict(raw)

    def read_artifact(self, archive_uri: str, relative_path: str) -> bytes:
        return (Path(archive_uri) / relative_path).read_bytes()


def _write_index(root: Path, manifest: Mapping[str, Any]) -> None:
    lines = [
        "# Data Product Audit Archive",
        "",
        f"- audit_archive_id: {manifest.get('audit_archive_id')}",
        f"- product_id: {manifest.get('product_id')}",
        f"- merkle_root: {manifest.get('merkle_root')}",
        "",
        "## Artifacts",
        "",
    ]
    for artifact in manifest.get("artifacts", []):
        if isinstance(artifact, Mapping):
            lines.append(f"- {artifact.get('kind')}: {artifact.get('sha256')}")
    (root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


__all__ = ["AuditArchiveStore", "LocalFsAuditArchiveStore"]
