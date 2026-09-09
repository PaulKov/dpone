"""Native transfer evidence artifact writer.

The writer materializes the evidence contract produced by strategy intelligence
without knowing anything about concrete connectors or sinks. Runtime code passes
real evidence payloads; this service validates completeness and writes a
checksumed index for CI, release gates and incident runbooks.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "dpone.native_transfer.evidence.v1"


@dataclass(frozen=True)
class NativeTransferEvidenceArtifactResult:
    run_id: str
    directory: Path
    index_path: Path
    markdown_path: Path
    artifact_paths: tuple[Path, ...]


class NativeTransferEvidenceArtifactWriter:
    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)

    def write(
        self,
        *,
        run_id: str,
        evidence_contract: Mapping[str, Any],
        payloads: Mapping[str, Mapping[str, Any]],
    ) -> NativeTransferEvidenceArtifactResult:
        required = [str(item) for item in evidence_contract.get("required_artifacts") or []]
        missing = [artifact for artifact in required if artifact not in payloads]
        if missing:
            joined = ", ".join(missing)
            raise ValueError(f"missing required native transfer evidence artifact payloads: {joined}")

        directory = self._base_dir / _safe_name(run_id)
        directory.mkdir(parents=True, exist_ok=True)
        artifact_paths = tuple(self._write_artifacts(directory, required, payloads))
        index_payload = self._index_payload(
            run_id=run_id,
            evidence_contract=evidence_contract,
            artifact_paths=artifact_paths,
        )
        index_path = directory / "evidence_index.json"
        markdown_path = directory / "evidence_index.md"
        _write_json(index_path, index_payload)
        markdown_path.write_text(_render_markdown(index_payload), encoding="utf-8")
        return NativeTransferEvidenceArtifactResult(
            run_id=run_id,
            directory=directory,
            index_path=index_path,
            markdown_path=markdown_path,
            artifact_paths=artifact_paths,
        )

    def _write_artifacts(
        self,
        directory: Path,
        required: list[str],
        payloads: Mapping[str, Mapping[str, Any]],
    ) -> list[Path]:
        paths: list[Path] = []
        for artifact in required:
            path = directory / artifact
            _write_json(path, payloads[artifact])
            paths.append(path)
        return paths

    def _index_payload(
        self,
        *,
        run_id: str,
        evidence_contract: Mapping[str, Any],
        artifact_paths: tuple[Path, ...],
    ) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "run_id": run_id,
            "contract": dict(evidence_contract),
            "artifacts": [
                {
                    "name": path.name,
                    "path": str(path),
                    "sha256": _sha256(path),
                    "bytes": path.stat().st_size,
                }
                for path in artifact_paths
            ],
        }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def _render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# Native transfer evidence: {payload['run_id']}",
        "",
        f"- schema_version: `{payload['schema_version']}`",
        f"- route: `{payload['contract'].get('route')}`",
        f"- strategy: `{payload['contract'].get('strategy')}`",
        f"- state_commit_gate: `{payload['contract'].get('state_commit_gate')}`",
        "",
        "## Artifacts",
        "",
    ]
    for artifact in payload["artifacts"]:
        lines.append(f"- `{artifact['name']}` sha256=`{artifact['sha256']}` bytes=`{artifact['bytes']}`")
    lines.append("")
    return "\n".join(lines)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
