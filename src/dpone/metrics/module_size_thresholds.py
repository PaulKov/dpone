"""Validated threshold value object for module-size analysis."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModuleSizeThresholds:
    """Positive warning and hard limits with optional paired SLOC limits."""

    warn_lines: int = 450
    max_lines: int = 600
    warn_sloc: int | None = None
    max_sloc: int | None = None

    def __post_init__(self) -> None:
        _validate_pair(self.warn_lines, self.max_lines, label="line")
        sloc = (self.warn_sloc, self.max_sloc)
        if any(value is not None and not _is_positive_integer(value) for value in sloc):
            raise ValueError("Module-size SLOC thresholds must be positive integers or null")
        if self.warn_sloc is not None and self.max_sloc is not None:
            _validate_pair(self.warn_sloc, self.max_sloc, label="SLOC")


def _validate_pair(warning: int, maximum: int, *, label: str) -> None:
    if not all(_is_positive_integer(value) for value in (warning, maximum)):
        raise ValueError(f"Module-size {label} thresholds must be positive integers")
    if warning >= maximum:
        raise ValueError(f"Module-size {label} warning threshold must be lower than its hard limit")


def _is_positive_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


__all__ = ["ModuleSizeThresholds"]
