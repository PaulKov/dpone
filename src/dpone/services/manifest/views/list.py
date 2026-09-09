from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestListRow:
    manifest: str
    kind: str
    selector: str
    name: str
    task_group: str
    source: str
    sink: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "manifest": self.manifest,
            "kind": self.kind,
            "selector": self.selector,
            "name": self.name,
            "task_group": self.task_group,
            "source": self.source,
            "sink": self.sink,
        }


@dataclass(frozen=True, slots=True)
class ManifestListView:
    meta: ManifestViewMeta
    rows: tuple[ManifestListRow, ...]
    total_manifests: int
    total_processes: int

    def to_jsonable(self) -> dict[str, Any]:
        data = self.meta.to_jsonable()
        data.update(
            {
                "total_manifests": self.total_manifests,
                "total_processes": self.total_processes,
                "rows": [r.to_jsonable() for r in self.rows],
            }
        )
        return data
