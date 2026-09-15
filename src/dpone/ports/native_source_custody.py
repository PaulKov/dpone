"""Narrow authentication capability for revision-checked source completion."""

from typing import Protocol

from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import SourceTrustedBuildCompletion


class SourceBuildCompletionReader(Protocol):
    """Resolve and freshly authenticate a retained completion and its cohort.

    Decoding the top-level record alone is insufficient. Missing or changed
    originals must raise; successful reads neither mutate custody nor dispatch.
    """

    def read_completion(self, reference: OriginalRef) -> SourceTrustedBuildCompletion: ...


class SourceBuildCompletionVerifier(Protocol):
    """Freshly authenticate all originals bound to one admitted executor.

    The composition root fixes invocation authorities. A successful return is not
    a dispatch or SQL grant: the caller must use these same immutable values in
    its current-owner compare-and-swap. Missing proof must raise.
    """

    def authenticate(self, completion: SourceTrustedBuildCompletion, completion_ref: OriginalRef) -> None: ...
