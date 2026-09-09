from __future__ import annotations

import tomllib as tomllib
from datetime import UTC as UTC
from enum import Enum


class StrEnum(str, Enum):  # noqa: UP042 - keep stable member typing for dpone enum contracts.
    """Project-local string enum facade with stable typing across supported Python versions."""

    def __str__(self) -> str:
        return str(self.value)


__all__ = ["StrEnum", "UTC", "tomllib"]
