"""Immutable private value retained across SQLClient writer stages."""

from dataclasses import dataclass
from typing import Any

_PROOF_TOKEN = object()


@dataclass(frozen=True, slots=True, repr=False)
class _WriterAuthorityProof:
    """Producer-sealed identity, digest, fence and object-token lineage."""

    _token: object
    fence: object
    terminal: Any
    owner: Any
    retained: Any
    verify: Any
    association: Any
    transition: Any
    owner_refs: tuple[object, ...]
    association_capture: tuple[object, ...]
    grant_receipt: object
    grant_sha256: str
    object_tokens: tuple[int, ...]
    lineage_sha256: str

    def __repr__(self) -> str:
        return "_WriterAuthorityProof(<opaque>)"


__all__ = ()
