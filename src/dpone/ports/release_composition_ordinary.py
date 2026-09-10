"""Build-plane capability for complete standalone workload source capture."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.contracts.release_composition_ordinary import OrdinaryReleaseCapture


class OrdinaryReleaseInventoryReaderPort(Protocol):
    """Capture source bytes and verify their closed executable projection."""

    def capture(self, root: Path, *, xcom_sidecar_image: str) -> OrdinaryReleaseCapture: ...


class OrdinaryArchiveUnpacker(Protocol):
    """Extract and verify the complete bounded archive, or reject the source."""

    def __call__(self, pack: Mapping[str, Any], root: Path) -> None: ...
