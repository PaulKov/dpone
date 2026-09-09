from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class GitOpsViewMeta:
    kind: str
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        data: dict[str, Any] = {"kind": self.kind}
        if self.path is not None:
            data["path"] = self.path
        if self.options:
            data["options"] = dict(self.options)
        return data


@dataclass(frozen=True, slots=True)
class GitOpsView:
    meta: GitOpsViewMeta
    report: Any

    @property
    def exit_code(self) -> int:
        return 0 if self.report.passed else 2

    def to_jsonable(self) -> dict[str, Any]:
        payload = self.report.to_jsonable()
        payload["meta"] = self.meta.to_jsonable()
        return payload


def build_gitops_meta(
    kind: str,
    *,
    path: str | None = None,
    options: Mapping[str, Any] | None = None,
) -> GitOpsViewMeta:
    clean_options = {str(k): v for k, v in dict(options or {}).items() if v is not None}
    return GitOpsViewMeta(kind=kind, path=path, options=clean_options)


__all__ = ["GitOpsView", "GitOpsViewMeta", "build_gitops_meta"]
