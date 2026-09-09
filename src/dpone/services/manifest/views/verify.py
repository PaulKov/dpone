from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.manifest.verify import VerificationReport

from .common import ManifestViewMeta


@dataclass(frozen=True, slots=True)
class ManifestVerifyView:
    meta: ManifestViewMeta
    report: VerificationReport

    def exit_code(self, *, warn_only: bool) -> int:
        return 0 if warn_only or self.report.failed == 0 else 1

    def to_jsonable(self) -> dict[str, Any]:
        data = self.meta.to_jsonable()
        data.update(self.report.to_jsonable())
        return data
