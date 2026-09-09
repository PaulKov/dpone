"""Environment drift detection for release promotion gates."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from dpone.ops.checksums import sha256_file


@dataclass(frozen=True, slots=True)
class EnvironmentDriftDifference:
    path: str
    source_value: object
    target_value: object
    kind: str
    allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EnvironmentDriftReport:
    source_environment: str
    target_environment: str
    source_path: str
    target_path: str
    source_sha256: str
    target_sha256: str
    passed: bool
    blockers: tuple[str, ...]
    allowed_drift_count: int
    blocking_drift_count: int
    differences: tuple[EnvironmentDriftDifference, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "source_environment": self.source_environment,
            "target_environment": self.target_environment,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "source_sha256": self.source_sha256,
            "target_sha256": self.target_sha256,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "allowed_drift_count": self.allowed_drift_count,
            "blocking_drift_count": self.blocking_drift_count,
            "output_dir": self.output_dir,
            "differences": [difference.to_dict() for difference in self.differences],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone environment drift report",
            "",
            f"- Source environment: `{self.source_environment}`",
            f"- Target environment: `{self.target_environment}`",
            f"- Passed: `{self.passed}`",
            f"- Allowed drift: `{self.allowed_drift_count}`",
            f"- Blocking drift: `{self.blocking_drift_count}`",
            "",
            "| path | kind | allowed | source | target |",
            "|---|---|---|---|---|",
        ]
        for difference in self.differences:
            lines.append(
                f"| `{difference.path}` | `{difference.kind}` | `{difference.allowed}` | "
                f"`{difference.source_value}` | `{difference.target_value}` |"
            )
        if self.blockers:
            lines.extend(["", "Environment drift blockers:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        lines.extend(
            [
                "",
                "## Runbook",
                "",
                "1. Review each blocking drift path.",
                "2. Decide whether the drift is expected environment variance or a release blocker.",
                "3. Add only stable environment-specific paths to the allowlist.",
                "4. Re-run `dpone ops env-drift` before promotion.",
                "",
            ]
        )
        return "\n".join(lines)


class EnvironmentDriftService:
    """Compares environment manifests/configs before release promotion."""

    def compare(
        self,
        *,
        output_dir: str | Path,
        source_environment: str,
        target_environment: str,
        source_path: str | Path,
        target_path: str | Path,
        allowlist_paths: Sequence[str] = (),
    ) -> EnvironmentDriftReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        source_file = Path(source_path)
        target_file = Path(target_path)
        allowed_paths = frozenset(allowlist_paths)
        differences = self._diff(
            source=self._flatten(self._load(source_file)),
            target=self._flatten(self._load(target_file)),
            allowlist_paths=allowed_paths,
        )
        blockers = tuple(difference.path for difference in differences if not difference.allowed)
        report = EnvironmentDriftReport(
            source_environment=source_environment,
            target_environment=target_environment,
            source_path=str(source_file),
            target_path=str(target_file),
            source_sha256=sha256_file(source_file),
            target_sha256=sha256_file(target_file),
            passed=not blockers,
            blockers=blockers,
            allowed_drift_count=sum(1 for difference in differences if difference.allowed),
            blocking_drift_count=len(blockers),
            differences=differences,
            output_dir=str(directory),
        )
        (directory / "env_drift.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "env_drift.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _diff(
        self,
        *,
        source: Mapping[str, object],
        target: Mapping[str, object],
        allowlist_paths: frozenset[str],
    ) -> tuple[EnvironmentDriftDifference, ...]:
        differences: list[EnvironmentDriftDifference] = []
        for path in sorted(set(source) | set(target)):
            source_has = path in source
            target_has = path in target
            if source_has and target_has and source[path] == target[path]:
                continue
            differences.append(
                EnvironmentDriftDifference(
                    path=path,
                    source_value=source.get(path, "<missing>"),
                    target_value=target.get(path, "<missing>"),
                    kind=self._kind(source_has=source_has, target_has=target_has),
                    allowed=path in allowlist_paths,
                )
            )
        return tuple(differences)

    @staticmethod
    def _kind(*, source_has: bool, target_has: bool) -> str:
        if source_has and target_has:
            return "changed"
        if source_has:
            return "missing_in_target"
        return "added_in_target"

    @staticmethod
    def _load(path: Path) -> object:
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() in {".yaml", ".yml"}:
            return yaml.safe_load(text) or {}
        return json.loads(text)

    def _flatten(self, payload: object, *, prefix: str = "") -> dict[str, object]:
        if isinstance(payload, Mapping):
            flattened: dict[str, object] = {}
            for key, value in payload.items():
                path = f"{prefix}.{key}" if prefix else str(key)
                flattened.update(self._flatten(value, prefix=path))
            return flattened
        if isinstance(payload, list):
            return {prefix: tuple(payload)}
        return {prefix: payload}
