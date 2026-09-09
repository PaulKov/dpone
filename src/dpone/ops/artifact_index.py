"""Ops artifact indexing for docs, CI, and release review."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed, artifact_requires_certification_trust
from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class ArtifactIndexItem:
    name: str
    path: str
    relative_path: str
    artifact_type: str
    sha256: str
    size_bytes: int
    modified_at: str
    release: str | None
    run_id: str | None
    passed: bool | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ArtifactIndexReport:
    release: str | None
    total_artifacts: int
    total_bytes: int
    passed: bool
    roots: tuple[str, ...]
    items: tuple[ArtifactIndexItem, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "total_artifacts": self.total_artifacts,
            "total_bytes": self.total_bytes,
            "passed": self.passed,
            "roots": list(self.roots),
            "output_dir": self.output_dir,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops artifact index",
            "",
            f"- Release: `{self.release or 'unversioned'}`",
            f"- Total artifacts: `{self.total_artifacts}`",
            f"- Total bytes: `{self.total_bytes}`",
            f"- Passed: `{self.passed}`",
            "",
            "| type | artifact | passed | release | run | sha256 | path |",
            "|---|---|---|---|---|---|---|",
        ]
        for item in self.items:
            lines.append(
                f"| `{item.artifact_type}` | `{item.name}` | `{item.passed}` | "
                f"`{item.release or ''}` | `{item.run_id or ''}` | `{item.sha256}` | `{item.relative_path}` |"
            )
        return "\n".join(lines) + "\n"


class ArtifactIndexService:
    """Builds a deterministic local index of dpone ops artifacts."""

    def build(
        self,
        *,
        output_dir: str | Path,
        roots: Sequence[str | Path],
        release: str | None = None,
    ) -> ArtifactIndexReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        root_paths = tuple(Path(root) for root in roots)
        items = tuple(
            sorted(
                (item for root in root_paths for item in self._scan_root(root)),
                key=lambda item: item.path,
            )
        )
        report = ArtifactIndexReport(
            release=release,
            total_artifacts=len(items),
            total_bytes=sum(item.size_bytes for item in items),
            passed=not any(item.passed is False for item in items),
            roots=tuple(str(root) for root in root_paths),
            items=items,
            output_dir=str(directory),
        )
        (directory / "artifact_index.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "artifact_index.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _scan_root(self, root: Path) -> tuple[ArtifactIndexItem, ...]:
        if not root.exists():
            return tuple()
        return tuple(self._item(path=path, root=root) for path in root.rglob("*") if self._should_index(path))

    def _item(self, *, path: Path, root: Path) -> ArtifactIndexItem:
        payload = self._json_payload(path)
        stat = path.stat()
        return ArtifactIndexItem(
            name=path.name,
            path=str(path),
            relative_path=str(path.relative_to(root)),
            artifact_type=self._artifact_type(path),
            sha256=sha256_file(path),
            size_bytes=stat.st_size,
            modified_at=datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
            release=self._optional_str(payload.get("release")),
            run_id=self._run_id(payload),
            passed=self._passed(self._artifact_type(path), payload),
        )

    @staticmethod
    def _should_index(path: Path) -> bool:
        if not path.is_file():
            return False
        if path.name in {"artifact_index.json", "artifact_index.md"}:
            return False
        if "__pycache__" in path.parts:
            return False
        return path.suffix.lower() in {".json", ".md", ".txt", ".log"}

    @staticmethod
    def _json_payload(path: Path) -> Mapping[str, Any]:
        if path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}

    @staticmethod
    def _artifact_type(path: Path) -> str:
        stem = path.stem
        known = {
            "artifact_index",
            "certification_history_index",
            "certification_report",
            "certification_suite",
            "connector_badges",
            "connector_marketplace",
            "data_contract_report",
            "evidence_chain_index",
            "ops_evidence_bundle",
            "ops_incident_pack",
            "release_gate",
            "security_audit",
            "slo_report",
            "strategy_certification_bundle",
        }
        if stem in known:
            return stem
        if stem.endswith("__evidence_chain"):
            return "evidence_chain"
        if stem.endswith("__certification_history"):
            return "certification_history"
        if stem.endswith("__behavior"):
            return "certification_behavior"
        return path.suffix.lower().lstrip(".") or "artifact"

    @staticmethod
    def _run_id(payload: Mapping[str, Any]) -> str | None:
        for key in ("run_id", "incident_id", "load_id"):
            value = payload.get(key)
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _passed(name: str, payload: Mapping[str, Any]) -> bool | None:
        if "passed" not in payload:
            return None
        return artifact_payload_passed(
            payload,
            name=name,
            require_certification_status=artifact_requires_certification_trust(name, payload),
        )

    @staticmethod
    def _optional_str(value: object) -> str | None:
        if value is None:
            return None
        return str(value)
