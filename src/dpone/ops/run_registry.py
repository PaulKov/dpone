"""Auditable run registry for ``dpone run`` results."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class RunRegistryArtifact:
    name: str
    path: str
    sha256: str
    passed: bool
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RunRegistryReport:
    run_id: str
    process: str
    manifest: str
    status: str
    passed: bool
    blockers: tuple[str, ...]
    recorded_at: str
    run_result_path: str
    run_result_sha256: str
    artifact_count: int
    artifacts: tuple[RunRegistryArtifact, ...]
    output_dir: str
    entry_path: str
    markdown_path: str
    index_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "process": self.process,
            "manifest": self.manifest,
            "status": self.status,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "recorded_at": self.recorded_at,
            "run_result_path": self.run_result_path,
            "run_result_sha256": self.run_result_sha256,
            "artifact_count": self.artifact_count,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "output_dir": self.output_dir,
            "entry_path": self.entry_path,
            "markdown_path": self.markdown_path,
            "index_path": self.index_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone run registry entry",
            "",
            f"- Run ID: `{self.run_id}`",
            f"- Process: `{self.process}`",
            f"- Manifest: `{self.manifest}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Recorded at: `{self.recorded_at}`",
            f"- Run result checksum: `{self.run_result_sha256}`",
            f"- Artifact count: `{self.artifact_count}`",
            "",
            "| artifact | status | sha256 | summary | path |",
            "|---|---|---|---|---|",
        ]
        if self.artifacts:
            for artifact in self.artifacts:
                status = "pass" if artifact.passed else "fail"
                lines.append(
                    f"| `{artifact.name}` | {status} | `{artifact.sha256}` | {artifact.summary} | `{artifact.path}` |"
                )
        else:
            lines.append(
                f"| `run_result` | pass | `{self.run_result_sha256}` | canonical run output | `{self.run_result_path}` |"
            )
        if self.blockers:
            lines.extend(["", "Run registry blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Operator runbook",
                "",
                "1. Use `dpone run --format json` to produce the canonical run result.",
                "2. Attach quality, state, reconciliation, SLO, and release artifacts with `--artifact name=path`.",
                "3. Review failed blockers before advancing state or promoting release evidence.",
                "4. Treat checksum drift in `run_registry_index.json` as an audit event.",
                "",
            ]
        )
        return "\n".join(lines)


class RunRegistryService:
    """Records ``dpone run`` outputs as immutable, checksumed run entries."""

    def record(
        self,
        *,
        output_dir: str | Path,
        run_result_path: str | Path,
        artifacts: Mapping[str, str | Path] | None = None,
    ) -> RunRegistryReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        result_path = Path(run_result_path)
        payload = self._payload(result_path)
        artifact_items = tuple(
            self._artifact_item(name, Path(path)) for name, path in sorted((artifacts or {}).items())
        )
        blockers = self._blockers(result_path, payload, artifact_items)
        run_id = self._value(payload, "run_id", "unknown_run")
        entry_path = directory / f"{run_id}__run_registry.json"
        markdown_path = directory / f"{run_id}__run_registry.md"
        index_path = directory / "run_registry_index.json"
        report = RunRegistryReport(
            run_id=run_id,
            process=self._value(payload, "process", "unknown"),
            manifest=self._value(payload, "manifest", "unknown"),
            status=self._status(payload),
            passed=not blockers,
            blockers=blockers,
            recorded_at=datetime.now(UTC).isoformat(),
            run_result_path=str(result_path),
            run_result_sha256=sha256_file(result_path) if result_path.exists() else "0" * 64,
            artifact_count=len(artifact_items),
            artifacts=artifact_items,
            output_dir=str(directory),
            entry_path=str(entry_path),
            markdown_path=str(markdown_path),
            index_path=str(index_path),
        )
        entry_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        self._write_index(index_path, report)
        return report

    def _artifact_item(self, name: str, path: Path) -> RunRegistryArtifact:
        if not path.exists():
            return RunRegistryArtifact(
                name=name,
                path=str(path),
                sha256="0" * 64,
                passed=False,
                summary="artifact missing",
            )
        payload = self._payload(path)
        return RunRegistryArtifact(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            passed=self._passed(payload),
            summary=self._summary(payload),
        )

    def _blockers(
        self,
        path: Path,
        payload: Mapping[str, Any],
        artifacts: tuple[RunRegistryArtifact, ...],
    ) -> tuple[str, ...]:
        blockers: list[str] = []
        if not path.exists():
            blockers.append("run_result.missing")
        elif not payload:
            blockers.append("run_result.invalid_json")
        elif not self._passed(payload):
            blockers.append("run_result.not_passed")
        for artifact in artifacts:
            if artifact.summary == "artifact missing":
                blockers.append(f"artifact.{artifact.name}.missing")
            elif not artifact.passed:
                blockers.append(f"artifact.{artifact.name}.not_passed")
        return tuple(blockers)

    def _write_index(self, path: Path, report: RunRegistryReport) -> None:
        existing_payload = self._payload(path)
        existing_runs = existing_payload.get("runs", [])
        runs = [item for item in existing_runs if isinstance(item, Mapping) and item.get("run_id") != report.run_id]
        runs.append(report.to_dict())
        path.write_text(
            json.dumps(
                {
                    "generated_at": datetime.now(UTC).isoformat(),
                    "run_count": len(runs),
                    "runs": runs,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}

    @classmethod
    def _passed(cls, payload: Mapping[str, Any]) -> bool:
        if "passed" in payload:
            return bool(payload["passed"])
        if payload.get("status") in {"failed", "rollback_required", "regression"}:
            return False
        blockers = payload.get("blockers", [])
        violations = payload.get("violations", [])
        findings = payload.get("findings", [])
        return not bool(blockers or violations or findings)

    @staticmethod
    def _status(payload: Mapping[str, Any]) -> str:
        result = payload.get("result")
        if isinstance(result, Mapping) and "status" in result:
            return str(result["status"])
        if "status" in payload:
            return str(payload["status"])
        if not payload:
            return "missing"
        return "success" if bool(payload.get("passed", False)) else "failed"

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        if "status" in payload:
            return f"status={payload['status']}"
        for key in ("blockers", "violations", "findings", "results", "checks", "items"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        return "passed" if bool(payload.get("passed", True)) else "failed"

    @staticmethod
    def _value(payload: Mapping[str, Any], key: str, default: str) -> str:
        value = payload.get(key, default)
        if isinstance(value, str) and value:
            return value
        return default
