"""CDC evidence artifact normalization."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.ops.cdc.models import CdcHandoffProfile
from dpone.ops.routes.evidence import RouteEvidenceReader
from dpone.ops.routes.models import RouteEvidenceItem


class CdcEvidenceReader:
    """Read CDC evidence artifacts using the shared ops pass/fail contract."""

    def __init__(self, *, reader: RouteEvidenceReader | None = None) -> None:
        self._reader = reader or RouteEvidenceReader()

    def read(
        self,
        *,
        profile: CdcHandoffProfile,
        artifacts: Mapping[str, str | Path],
    ) -> tuple[RouteEvidenceItem, ...]:
        required = tuple(profile.required_evidence)
        names = tuple(dict.fromkeys((*required, *sorted(artifacts))))
        return tuple(
            self._reader.read(name=name, path=artifacts.get(name), required=name in required) for name in names
        )
