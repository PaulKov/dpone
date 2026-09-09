from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestStatsView:
    meta: ManifestViewMeta
    total_manifests: int
    total_processes: int
    kinds: Mapping[str, int]
    by_dataset: Mapping[str, int]
    by_group: Mapping[str, int]

    def to_jsonable(self) -> dict[str, Any]:
        data = self.meta.to_jsonable()
        data.update(
            {
                "total_manifests": self.total_manifests,
                "total_processes": self.total_processes,
                "kinds": dict(self.kinds),
                "by_dataset": dict(self.by_dataset),
                "by_group": dict(self.by_group),
            }
        )
        return data
