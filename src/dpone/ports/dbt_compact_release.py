"""Required capabilities for verified compact workspace transformation."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol


class CompactWorkspaceRewriter(Protocol):
    """Apply the existing closed transport policy, preserving source identity."""

    def pack(self, value: Mapping[str, Any], *, xcom_sidecar_image: str) -> dict[str, Any]: ...

    def dag(self, value: Mapping[str, Any], *, workload_ids: Sequence[str]) -> dict[str, Any]: ...


class ReleaseIntegrityWriter(Protocol):
    """Produce the checksum subject for the derived private release stage."""

    def write(self, compiled_root: Path) -> object: ...
