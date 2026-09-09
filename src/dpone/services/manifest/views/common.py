from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ManifestViewMeta:
    kind: str
    path: str | None = None
    registry_paths: tuple[str, ...] = ()
    options: dict[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        data: dict[str, Any] = {"kind": self.kind}
        if self.path is not None:
            data["path"] = self.path
        if self.registry_paths:
            data["registry_paths"] = list(self.registry_paths)
        if self.options:
            data["options"] = dict(self.options)
        return data


def build_meta(
    kind: str,
    *,
    path: str | None = None,
    registry_paths: tuple[str, ...] = (),
    options: Mapping[str, Any] | None = None,
) -> ManifestViewMeta:
    clean_options = {str(k): v for k, v in dict(options or {}).items() if v is not None}
    return ManifestViewMeta(kind=kind, path=path, registry_paths=registry_paths, options=clean_options)
