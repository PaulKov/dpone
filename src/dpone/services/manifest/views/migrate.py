from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.manifest.migrate import BatchPlan, MigrationPlan

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestMigrateView:
    meta: ManifestViewMeta
    plan: MigrationPlan

    @property
    def total_legacy(self) -> int:
        return sum(len(batch.processes) for batch in self.plan.batches)

    def to_jsonable(self) -> dict[str, Any]:
        data = self.meta.to_jsonable()
        data.update(
            {
                "total_legacy": self.total_legacy,
                "batch_count": len(self.plan.batches),
                "batches": [self._batch_to_json(batch) for batch in self.plan.batches],
            }
        )
        return data

    @staticmethod
    def _batch_to_json(batch: BatchPlan) -> dict[str, Any]:
        return {
            "out_path": str(batch.out_path),
            "process_count": len(batch.processes),
            "dataset": batch.key.dataset,
            "task_group": batch.key.task_group,
        }
