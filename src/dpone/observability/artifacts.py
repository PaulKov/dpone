"""Observability artifact indexing.

This module keeps metrics export evidence self-describing without depending on
the broader ops artifact index. The ops index can still index the whole output
directory later; this local index documents exactly what one metrics export
produced.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class MetricsArtifactItem:
    """One file produced by the runtime metrics exporter."""

    name: str
    path: str
    relative_path: str
    artifact_type: str
    sha256: str
    size_bytes: int
    description: str

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "path": self.path,
            "relative_path": self.relative_path,
            "artifact_type": self.artifact_type,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class MetricsArtifactIndex:
    """Checksum manifest for one observability metrics export."""

    generated_at: str
    output_dir: str
    artifacts: tuple[MetricsArtifactItem, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "output_dir": self.output_dir,
            "artifacts": [item.to_dict() for item in self.artifacts],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


class MetricsArtifactIndexService:
    """Writes a deterministic checksum index for observability artifacts."""

    _DESCRIPTIONS: Mapping[str, tuple[str, str]] = {
        "prometheus_metrics.prom": ("prometheus", "Prometheus text exposition metrics."),
        "opentelemetry_metrics.json": ("opentelemetry", "OpenTelemetry-compatible JSON metrics payload."),
        "runtime_metrics.json": ("runtime_report", "Machine-readable metrics export report."),
        "runtime_metrics.md": ("runtime_report", "Human-readable metrics export report."),
    }

    def build(self, *, output_dir: str | Path, artifacts: Mapping[str, str | Path]) -> MetricsArtifactIndex:
        root = Path(output_dir)
        items = tuple(
            self._item(root=root, name=name, path=Path(path))
            for name, path in sorted(artifacts.items(), key=lambda item: item[0])
        )
        index = MetricsArtifactIndex(
            generated_at=datetime.now(UTC).isoformat(),
            output_dir=str(root),
            artifacts=items,
        )
        (root / "metrics_index.json").write_text(index.to_json(), encoding="utf-8")
        return index

    def _item(self, *, root: Path, name: str, path: Path) -> MetricsArtifactItem:
        artifact_type, description = self._DESCRIPTIONS.get(name, ("artifact", "Observability artifact."))
        return MetricsArtifactItem(
            name=name,
            path=str(path),
            relative_path=str(path.relative_to(root)),
            artifact_type=artifact_type,
            sha256=_sha256_file(path),
            size_bytes=path.stat().st_size,
            description=description,
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
