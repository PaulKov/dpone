from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed

SCHEMA_VERSION = "dpone.strategy.certification_bundle.v1"


@dataclass(frozen=True, slots=True)
class CertificationEvidenceInput:
    bundle_id: str
    replay_evidence: tuple[str | Path, ...] = ()
    matrix_artifacts: tuple[str | Path, ...] = ()
    connector_artifacts: tuple[str | Path, ...] = ()
    benchmark_artifacts: tuple[str | Path, ...] = ()
    native_transfer_evidence: tuple[str | Path, ...] = ()
    docs_links: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CertificationEvidenceBundleArtifact:
    json_path: Path
    markdown_path: Path


class StrategyCertificationEvidenceBundleWriter:
    """Aggregate strategy certification evidence into one auditable bundle."""

    def __init__(self, output_dir: str | Path = "test_artifacts/strategies/certification_bundle") -> None:
        self._output_dir = Path(output_dir)

    def write(self, evidence_input: CertificationEvidenceInput) -> CertificationEvidenceBundleArtifact:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        payload = _build_payload(evidence_input)
        json_path = self._output_dir / "strategy_certification_bundle.json"
        markdown_path = self._output_dir / "strategy_certification_bundle.md"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
        return CertificationEvidenceBundleArtifact(json_path=json_path, markdown_path=markdown_path)


def _build_payload(evidence_input: CertificationEvidenceInput) -> dict[str, Any]:
    items = [
        *(_evidence_item("replay", path) for path in evidence_input.replay_evidence),
        *(_evidence_item("matrix", path) for path in evidence_input.matrix_artifacts),
        *(_evidence_item("connector", path) for path in evidence_input.connector_artifacts),
        *(_evidence_item("benchmark", path) for path in evidence_input.benchmark_artifacts),
        *(_evidence_item("native_transfer", path) for path in evidence_input.native_transfer_evidence),
    ]
    blockers = _blockers(items)
    passed_items = sum(1 for item in items if item["present"] and item["passed"])
    present_items = sum(1 for item in items if item["present"])
    summary = {
        "total_items": len(items),
        "present_items": present_items,
        "passed_items": passed_items,
        "missing_items": sum(1 for item in items if not item["present"]),
        "failed_items": sum(1 for item in items if item["present"] and not item["passed"]),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "bundle_id": evidence_input.bundle_id,
        "created_at": datetime.now(UTC).isoformat(),
        "passed": not blockers,
        "evidence_status": "PASS" if not blockers else "FAIL",
        "blockers": blockers,
        "summary": summary,
        "docs_links": list(evidence_input.docs_links),
        "evidence_items": items,
    }


def _evidence_item(kind: str, path_like: str | Path) -> dict[str, Any]:
    path = Path(path_like)
    if not path.exists():
        return {
            "kind": kind,
            "path": str(path),
            "name": path.name,
            "present": False,
            "passed": False,
            "sha256": None,
            "summary": {},
        }
    payload = _read_json(path)
    return {
        "kind": kind,
        "path": str(path),
        "name": path.name,
        "present": True,
        "passed": _is_passed(kind, payload),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "summary": _summary(kind, payload),
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "error": "invalid_json"}
    return payload if isinstance(payload, dict) else {"passed": False, "error": "non_object_json"}


def _is_passed(kind: str, payload: dict[str, Any]) -> bool:
    if kind == "replay":
        return str(payload.get("status")) in {"executed", "passed"} and bool(payload.get("state_committed", True))
    if kind == "native_transfer":
        artifacts = payload.get("artifacts")
        valid_artifacts = (
            isinstance(artifacts, list)
            and bool(artifacts)
            and all(
                isinstance(item, dict) and isinstance(item.get("sha256"), str) and len(str(item["sha256"])) == 64
                for item in artifacts
            )
        )
        return (
            payload.get("schema_version") == "dpone.native_transfer.evidence.v1"
            and isinstance(payload.get("contract"), dict)
            and valid_artifacts
        )
    return artifact_payload_passed(payload)


def _summary(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    if kind == "replay":
        return {
            "status": payload.get("status"),
            "state_committed": payload.get("state_committed"),
            "status_checks": len(payload.get("status_checks") or ()),
            "row_count_checks": len(payload.get("row_count_checks") or ()),
        }
    if kind == "matrix":
        return {
            "profile": payload.get("profile"),
            "case_count": payload.get("case_count"),
            "passed": payload.get("passed"),
        }
    if kind == "connector":
        return {"passed": payload.get("passed"), "connectors": len(payload.get("connectors") or {})}
    if kind == "benchmark":
        return {"passed": payload.get("passed"), "metrics": sorted((payload.get("metrics") or {}).keys())}
    if kind == "native_transfer":
        raw_contract = payload.get("contract")
        contract: dict[str, Any] = raw_contract if isinstance(raw_contract, dict) else {}
        raw_artifacts = payload.get("artifacts")
        artifacts: list[Any] = raw_artifacts if isinstance(raw_artifacts, list) else []
        return {
            "route": contract.get("route"),
            "strategy": contract.get("strategy"),
            "artifact_count": len(artifacts),
            "state_commit_gate": contract.get("state_commit_gate"),
        }
    return {}


def _blockers(items: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for item in items:
        if not item["present"]:
            blockers.append(f"{item['kind']}.missing:{item['name']}")
        elif not item["passed"]:
            blockers.append(f"{item['kind']}.not_passed:{item['name']}")
    return blockers


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Strategy certification evidence bundle: {payload['bundle_id']}",
        "",
        f"- schema_version: `{payload['schema_version']}`",
        f"- passed: `{payload['passed']}`",
        f"- evidence_status: `{payload['evidence_status']}`",
        f"- blockers: `{len(payload['blockers'])}`",
        "",
        "## Evidence items",
        "",
        "| Kind | Presence | Status | SHA-256 | Path |",
        "|---|---|---|---|---|",
    ]
    for item in payload["evidence_items"]:
        presence = "present" if item["present"] else "missing"
        status = "passed" if item["passed"] else "failed"
        sha = item["sha256"] or "n/a"
        lines.append(f"| {item['kind']} | {presence} | {status} | `{sha}` | `{item['path']}` |")
    lines.extend(["", "## Docs links", ""])
    lines.extend(f"- `{link}`" for link in payload["docs_links"])
    lines.append("")
    if payload["blockers"]:
        lines.extend(["## Blockers", ""])
        lines.extend(f"- `{blocker}`" for blocker in payload["blockers"])
        lines.append("")
    return "\n".join(lines)
