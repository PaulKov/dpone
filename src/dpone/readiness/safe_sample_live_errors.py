"""Stable failures shared by live safe-sample composition services."""

from __future__ import annotations

from typing import Any


class LiveSafeSampleRuntimeAssemblyError(ValueError):
    """Reject invalid live inputs before credentials or external I/O."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        verification: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.verification = verification


__all__ = ["LiveSafeSampleRuntimeAssemblyError"]
