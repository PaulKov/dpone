"""Immutable observable attempt state for the permission-grant process."""

from dataclasses import dataclass
from typing import Generic, TypeVar

PermissionKind = TypeVar("PermissionKind")


@dataclass(frozen=True, slots=True, repr=False)
class PermissionGrantPublicAttempt(Generic[PermissionKind]):
    direction: str
    kind: PermissionKind
    ordinal: int
    prefix: bytes
    payload: bytes
    transferred: int
    encoded: bool
    complete: bool


@dataclass(frozen=True, slots=True, repr=False)
class PermissionGrantCredentialAttempt:
    payload_size: int
    framed_size: int
    transferred: int
    complete: bool
