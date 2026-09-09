"""Unified run evidence pack writer."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import (
    artifact_payload_passed,
    artifact_requires_certification_trust,
)
from dpone.ops.ids import utc_now_iso

SCHEMA_VERSION = "dpone.unified_run_evidence.v1"


@dataclass(frozen=True, slots=True)
class UnifiedRunEvidencePackArtifact:
    json_path: Path
    markdown_path: Path

    def to_dict(self) -> dict[str, str]:
        return {"json_path": str(self.json_path), "markdown_path": str(self.markdown_path)}


class UnifiedRunEvidencePackWriter:
    """Collects run artifacts into one auditable evidence index."""

    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)

    def write(self, *, run_id: str, artifacts: Mapping[str, str | Path]) -> UnifiedRunEvidencePackArtifact:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        items = [self._item(name, Path(path)) for name, path in sorted(artifacts.items())]
        payload = {
            "schema_version": SCHEMA_VERSION,
            "created_at": utc_now_iso(),
            "run_id": run_id,
            "passed": all(item["exists"] and item.get("artifact_passed", True) for item in items),
            "items": items,
        }
        json_path = self.output_dir / "unified_run_evidence.json"
        markdown_path = self.output_dir / "unified_run_evidence.md"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        markdown_path.write_text(_markdown(payload), encoding="utf-8")
        return UnifiedRunEvidencePackArtifact(json_path=json_path, markdown_path=markdown_path)

    def _item(self, name: str, path: Path) -> dict[str, Any]:
        exists = path.exists()
        payload = _json_payload(path) if exists else {}
        return {
            "name": name,
            "path": str(path),
            "exists": exists,
            "sha256": _sha256(path) if exists else None,
            "artifact_passed": artifact_payload_passed(
                payload,
                require_certification_status=artifact_requires_certification_trust(name, payload),
            ),
            "schema_version": payload.get("schema_version"),
        }


def _json_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# dpone unified run evidence",
        "",
        f"- Schema version: `{payload['schema_version']}`",
        f"- Run ID: `{payload['run_id']}`",
        f"- Passed: `{payload['passed']}`",
        "",
        "| artifact | exists | passed | sha256 |",
        "|---|---:|---:|---|",
    ]
    for item in payload["items"]:
        lines.append(f"| `{item['name']}` | `{item['exists']}` | `{item['artifact_passed']}` | `{item['sha256']}` |")
    lines.extend(
        [
            "",
            "## Runbook",
            "",
            "1. If an expected artifact is missing, re-run the matching gate before promotion.",
            "2. If a checksum changes, treat the evidence pack as a new audit version.",
            "3. Attach this pack to certification, release, and incident records.",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = ["UnifiedRunEvidencePackArtifact", "UnifiedRunEvidencePackWriter"]
