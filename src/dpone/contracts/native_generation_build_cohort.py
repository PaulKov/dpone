"""Exact observed build artifacts linked to one trusted invocation.

Shapes are not authority. The producer validates actual files and the locked
execution pack; consumers independently authenticate every original and outcome.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal

from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding


@dataclass(frozen=True, slots=True)
class NativeBuildArtifact:
    """One bounded actual file; path is relative to the captured OUTPUT root."""

    role: Literal["MANIFEST", "RUN_RESULTS"]
    relative_path: str
    size_bytes: int
    original: OriginalRef

    def __post_init__(self) -> None:
        names = {"MANIFEST": "manifest.json", "RUN_RESULTS": "run_results.json"}
        if type(self.role) is not str or self.role not in names:
            raise NativeSourceCustodyError("unknown build artifact role")
        if type(self.relative_path) is not str or not self.relative_path or len(self.relative_path) > 4096:
            raise NativeSourceCustodyError("build artifact requires a bounded relative path")
        path = PurePosixPath(self.relative_path)
        if (
            path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != self.relative_path
            or "\\" in self.relative_path
            or any(ord(c) < 32 for c in self.relative_path)
            or path.name != names[self.role]
        ):
            raise NativeSourceCustodyError("build artifact path differs from its confined role")
        if type(self.size_bytes) is not int or not 1 <= self.size_bytes <= 9223372036854775807:
            raise NativeSourceCustodyError("build artifact size must be an exact positive SQL bigint")
        _references(self.original)


@dataclass(frozen=True, slots=True)
class NativeBuildArtifactInventory:
    """Observed dbt ID mapping plus the complete fixed two-artifact cohort.

    The observed dbt invocation ID is not the trusted pre-dispatch UUID. Actual
    bytes from held output roots establish their mapping in the trusted producer.
    """

    executor: SourceExecutorBinding
    command: OriginalRef
    toolchain: OriginalRef
    termination: OriginalRef
    execution_pack: OriginalRef
    build_evidence: OriginalRef
    dbt_invocation_id: str
    artifacts: tuple[NativeBuildArtifact, ...]

    def __post_init__(self) -> None:
        if type(self.executor) is not SourceExecutorBinding:
            raise NativeSourceCustodyError("build inventory requires an exact executor")
        self.executor.__post_init__()
        _references(self.command, self.toolchain, self.termination, self.execution_pack, self.build_evidence)
        if self.command != self.executor.command:
            raise NativeSourceCustodyError("build inventory command differs from admission")
        label = self.dbt_invocation_id
        if (
            type(label) is not str
            or not 0 < len(label) <= 128
            or label != label.strip()
            or any(ord(c) < 32 for c in label)
        ):
            raise NativeSourceCustodyError("build inventory requires a canonical observed dbt identity")
        if type(self.artifacts) is not tuple or len(self.artifacts) != 2:
            raise NativeSourceCustodyError("build inventory requires exactly two immutable artifacts")
        for artifact in self.artifacts:
            if type(artifact) is not NativeBuildArtifact:
                raise NativeSourceCustodyError("build inventory requires exact artifact records")
            artifact.__post_init__()
        if tuple(item.role for item in self.artifacts) != ("MANIFEST", "RUN_RESULTS"):
            raise NativeSourceCustodyError("build inventory requires ordered manifest and run results")
        if len({PurePosixPath(item.relative_path).parent for item in self.artifacts}) != 1:
            raise NativeSourceCustodyError("build artifacts must share the admitted target directory")


def _references(*references: OriginalRef) -> None:
    for reference in references:
        if type(reference) is not OriginalRef:
            raise NativeSourceCustodyError("build cohort requires exact original references")
        reference.__post_init__()
