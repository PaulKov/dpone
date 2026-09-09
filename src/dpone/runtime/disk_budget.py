from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class DiskBudget:
    """Small active-file budget guard for native transfer workers."""

    max_active_files: int
    max_active_bytes: int
    active_files: int = 0
    active_bytes: int = 0

    def try_acquire(self, *, expected_bytes: int) -> bool:
        expected = max(0, int(expected_bytes))
        if self.active_files + 1 > self.max_active_files:
            return False
        if self.active_bytes + expected > self.max_active_bytes:
            return False
        self.active_files += 1
        self.active_bytes += expected
        return True

    def release(self, *, actual_bytes: int) -> None:
        self.active_files = max(0, self.active_files - 1)
        self.active_bytes = max(0, self.active_bytes - max(0, int(actual_bytes)))


__all__ = ["DiskBudget"]
