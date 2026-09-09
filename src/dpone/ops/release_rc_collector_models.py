"""Stable release-candidate collector contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol

MERGE_TRAIN_SCHEMA_VERSION = "dpone.release_rc_merge_train.v1"
SCHEMA_VERSION = "dpone.release_rc_collect.v1"
INPUTS_SCHEMA_VERSION = "dpone.release_rc_inputs.v1"


class ReleaseRcMergeTrainContract(Protocol):
    """Minimal merge-train contract required by collector reports."""

    def to_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class ReleaseRcEvidenceRef:
    """One evidence artifact reference passed to the release RC finalizer."""

    name: str
    path: str
    required: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleaseRcCollectorReport:
    """Stable JSON/Markdown receipt for RC input collection."""

    release: str
    previous_release: str
    package_version: str
    mode: str
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    merge_train: ReleaseRcMergeTrainContract
    evidence_refs: tuple[ReleaseRcEvidenceRef, ...]
    finalizer_command: tuple[str, ...]
    output_dir: str
    merge_train_path: str
    inputs_path: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "previous_release": self.previous_release,
            "package_version": self.package_version,
            "mode": self.mode,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "merge_train": self.merge_train.to_dict(),
            "evidence_refs": [item.to_dict() for item in self.evidence_refs],
            "finalizer_command": list(self.finalizer_command),
            "output_dir": self.output_dir,
            "merge_train_path": self.merge_train_path,
            "inputs_path": self.inputs_path,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def inputs_dict(self) -> dict[str, object]:
        return {
            "schema_version": INPUTS_SCHEMA_VERSION,
            "release": self.release,
            "previous_release": self.previous_release,
            "package_version": self.package_version,
            "mode": self.mode,
            "merge_train_json": self.merge_train_path,
            "artifacts": {item.name: item.path for item in self.evidence_refs if item.path},
            "required_artifacts": [item.name for item in self.evidence_refs if item.required],
            "finalizer_command": list(self.finalizer_command),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def inputs_json(self) -> str:
        return json.dumps(self.inputs_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def merge_train_json(self) -> str:
        payload = {"schema_version": MERGE_TRAIN_SCHEMA_VERSION, **self.merge_train.to_dict()}
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Release RC collector",
            "",
            f"- Release: `{self.release}`",
            f"- Previous release: `{self.previous_release}`",
            f"- Package version: `{self.package_version}`",
            f"- Mode: `{self.mode}`",
            f"- Passed: `{self.passed}`",
            f"- Merge train: `{self.merge_train_path}`",
            f"- Finalizer inputs: `{self.inputs_path}`",
            "",
            "## Pull requests",
            "",
            "| PR | base | head | state | merge state | checks |",
            "|---:|---|---|---|---|---:|",
        ]
        for item in self.merge_train.pull_requests:
            lines.append(
                f"| `#{item.number}` | `{item.base_ref}` | `{item.head_ref}` | "
                f"`{item.state}` | `{item.merge_state}` | `{len(item.checks)}` |"
            )
        lines.extend(
            [
                "",
                "## Evidence refs",
                "",
                "| artifact | required | path |",
                "|---|---:|---|",
            ]
        )
        for item in self.evidence_refs:
            lines.append(f"| `{item.name}` | `{item.required}` | `{item.path}` |")
        lines.extend(["", "## Finalizer command", "", "```bash", " ".join(self.finalizer_command), "```", ""])
        lines.extend(["## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.append("- Run the generated `release-rc-finalize` command and attach both collector artifacts.")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.merge_train_path).write_text(self.merge_train_json(), encoding="utf-8")
        Path(self.inputs_path).write_text(self.inputs_json(), encoding="utf-8")
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


__all__ = [
    "INPUTS_SCHEMA_VERSION",
    "MERGE_TRAIN_SCHEMA_VERSION",
    "SCHEMA_VERSION",
    "ReleaseRcCollectorReport",
    "ReleaseRcEvidenceRef",
    "ReleaseRcMergeTrainContract",
]
