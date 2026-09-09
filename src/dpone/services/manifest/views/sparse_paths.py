from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestSparsePathsView:
    meta: ManifestViewMeta
    report: Any

    @property
    def exit_code(self) -> int:
        return 0 if self.report.passed else 2

    def to_jsonable(self) -> dict[str, Any]:
        payload = self.report.to_jsonable()
        payload["meta"] = self.meta.to_jsonable()
        return payload


__all__ = ["ManifestSparsePathsView"]
