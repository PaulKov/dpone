from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class RenderedProcessDoc:
    selector: str | None
    name: str
    config: Mapping[str, Any]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "selector": self.selector,
            "name": self.name,
            "config": dict(self.config),
        }


@dataclass(frozen=True, slots=True)
class ManifestRenderView:
    meta: ManifestViewMeta
    docs: tuple[RenderedProcessDoc, ...]

    def to_jsonable(self) -> dict[str, Any]:
        data = self.meta.to_jsonable()
        data["docs"] = [d.to_jsonable() for d in self.docs]
        return data
