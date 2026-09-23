"""Immutable physical-column layout shared by native wire implementations.

Backend-specific admission belongs to separate validators; this historical DTO
retains its full existing type vocabulary and serialization behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class NativeWireColumnLayout:
    """Physical layout for one source-native column."""

    name: str
    source_type: str
    target_type: str
    nullable: bool
    storage_type: str
    prefix_width: int = 0
    fixed_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    encoding: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# Preserve historical class/function pickle locators through the runtime re-export.
NativeWireColumnLayout.__module__ = "dpone.runtime.native_wire_models"
NativeWireColumnLayout.to_dict.__module__ = "dpone.runtime.native_wire_models"
