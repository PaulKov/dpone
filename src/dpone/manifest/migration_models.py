from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.migration_naming import _to_identifier
from dpone.manifest.models import ProcessSpec


@dataclass(frozen=True, slots=True)
class LegacyProcess:
    """A legacy process read from a single-manifest YAML."""

    path: Path
    spec: ProcessSpec

    @property
    def raw(self) -> Mapping[str, Any]:
        return self.spec.raw_config

    @property
    def src_schema(self) -> str:
        lc = self.spec.config.load_config
        return (lc.source_schema or "default").strip() or "default"

    @property
    def src_table(self) -> str:
        lc = self.spec.config.load_config
        if not lc.source_table:
            raise ManifestConfigurationError(f"Не удалось определить source table для {self.path}")
        return str(lc.source_table)

    @property
    def target_dataset(self) -> str:
        lc = self.spec.config.load_config
        return str(lc.target_schema or "").strip()

    @property
    def target_table(self) -> str:
        lc = self.spec.config.load_config
        return str(lc.target_table or "").strip()

    @property
    def task_group(self) -> str:
        return str(self.spec.config.task_group or "").strip()

    @property
    def strategy_mode(self) -> str:
        # Prefer raw config (it is the user-facing field name)
        try:
            sink = self.raw.get("sink") or {}
            strategy = sink.get("strategy") or {}
            mode = strategy.get("mode")
            if isinstance(mode, str) and mode.strip():
                return mode.strip()
        except Exception:
            pass
        # fallback to parsed strategy (enum)
        return str(getattr(self.spec.config, "load_strategy", "") or "").lower() or "full_refresh"


@dataclass(frozen=True, slots=True)
class ProcessRef:
    """Reference to a single process inside a batch file."""

    batch_path: Path
    selector: str


@dataclass(frozen=True, slots=True)
class GroupKey:
    """Grouping key for batch manifest generation."""

    dataset: str
    task_group: str

    def to_filename(self) -> str:
        """Stable batch filename for this group key."""
        if self.dataset and self.task_group and self.dataset != self.task_group:
            base = f"{self.dataset}__{self.task_group}"
        else:
            base = self.dataset or self.task_group or "batch"
        base = _to_identifier(base)
        return f"{base}.batch.yaml"


@dataclass(frozen=True, slots=True)
class MigrationConfig:
    src_path: Path
    out_dir: Path
    recursive: bool = True
    group_by: str = "dataset"  # dataset|task_group|dataset_task_group
    overwrite: bool = False
    dry_run: bool = False
    infer_naming: bool = True
    naming_threshold: float = 0.8  # fraction of tables that must match pattern to enable naming templates
    convention: str | None = None  # if set, add `convention:` to generated batch manifests
    registry_paths: tuple[Path, ...] = ()


@dataclass(frozen=True, slots=True)
class BatchPlan:
    key: GroupKey
    out_path: Path
    processes: tuple[LegacyProcess, ...]


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    batches: tuple[BatchPlan, ...]
    mapping: Mapping[Path, ProcessRef]
