from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dpone.runtime.storage_policy import RuntimeStoragePolicy


@dataclass(frozen=True, slots=True)
class TransferWorkspace:
    """Typed per-run directories for native transfer runtime artifacts."""

    root: Path
    transfer_dir: Path
    lease_dir: Path
    evidence_dir: Path
    checkpoint_dir: Path
    debug_dir: Path

    @classmethod
    def from_policy(cls, policy: RuntimeStoragePolicy, *, pipeline: str, run_id: str) -> TransferWorkspace:
        transfer_dir = policy.work_dir / _render_stage_path(policy.path_template, pipeline, run_id, "transfer")
        lease_dir = policy.work_dir / _render_stage_path(policy.path_template, pipeline, run_id, "leases")
        return cls(
            root=transfer_dir.parent,
            transfer_dir=transfer_dir,
            lease_dir=lease_dir,
            evidence_dir=policy.evidence_dir / _render_stage_path(policy.path_template, pipeline, run_id, "evidence"),
            checkpoint_dir=policy.checkpoint_dir
            / _render_stage_path(policy.path_template, pipeline, run_id, "checkpoint"),
            debug_dir=policy.debug_dir / _render_stage_path(policy.path_template, pipeline, run_id, "debug"),
        )

    def ensure(self) -> None:
        for path in (self.transfer_dir, self.lease_dir, self.evidence_dir, self.checkpoint_dir, self.debug_dir):
            path.mkdir(parents=True, exist_ok=True)

    def slice_path(self, *, partition_index: int, slice_index: int, suffix: str) -> Path:
        return self.transfer_dir / f"p{partition_index:04d}_s{slice_index:04d}{suffix}"


def _safe(value: str) -> str:
    text = "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in str(value))
    return text.strip("._") or "default"


def _render_stage_path(template: str, pipeline: str, run_id: str, stage: str) -> Path:
    contains_stage = "{stage}" in template
    rendered = template.format(pipeline=_safe(pipeline), run_id=_safe(run_id), stage=_safe(stage))
    path = Path(rendered)
    if not contains_stage:
        path = path / _safe(stage)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("runtime.storage.path_template must render a relative path without '..'")
    return path


__all__ = ["TransferWorkspace"]
