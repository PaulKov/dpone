"""Transport-neutral failures for Studio application services."""

from __future__ import annotations

from typing import Any

from dpone.readiness.error_contract import dpone_error


class StudioError(ValueError):
    """Safe application failure mapped by each external adapter."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        stage: str = "studio_api",
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.stage = stage

    def to_payload(self) -> dict[str, Any]:
        return {
            "passed": False,
            "errors": [
                dpone_error(
                    self.code,
                    self.message,
                    stage=self.stage,
                    docs_url="docs/studio.md#troubleshooting",
                )
            ],
        }


__all__ = ["StudioError"]
