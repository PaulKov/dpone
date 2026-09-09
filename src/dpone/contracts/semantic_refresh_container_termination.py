"""Exact container termination observation for semantic-refresh recovery."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.semantic_refresh_evidence_common import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_text,
    require_utc_timestamp,
)

_FIELDS = frozenset({"name", "container_id", "reason", "finished_at"})
_OPTIONAL_FIELDS = frozenset({"exit_code"})


@dataclass(frozen=True, order=True, slots=True)
class SemanticRefreshContainerTermination:
    """Terminal state of one exact Kubernetes container runtime identity."""

    name: str
    container_id: str
    reason: str
    finished_at: str
    exit_code: int | None = None

    def __post_init__(self) -> None:
        for field in ("name", "container_id", "reason"):
            require_text(getattr(self, field), f"container.{field}")
        require_utc_timestamp(self.finished_at, "container.finished_at")
        if self.exit_code is not None and (isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int)):
            raise SemanticRefreshContractError("container.exit_code must be an integer")

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshContainerTermination:
        """Parse one exact terminal container observation."""

        raw = require_closed_mapping(
            value,
            "container_termination",
            required=_FIELDS,
            optional=_OPTIONAL_FIELDS,
        )
        exit_code = raw.get("exit_code")
        if "exit_code" in raw and exit_code is None:
            raise SemanticRefreshContractError("container.exit_code cannot be null")
        if exit_code is not None and (isinstance(exit_code, bool) or not isinstance(exit_code, int)):
            raise SemanticRefreshContractError("container.exit_code must be an integer")
        return cls(
            name=require_text(raw.get("name"), "container.name"),
            container_id=require_text(raw.get("container_id"), "container.container_id"),
            reason=require_text(raw.get("reason"), "container.reason"),
            finished_at=require_utc_timestamp(raw.get("finished_at"), "container.finished_at"),
            exit_code=exit_code,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the canonical observation."""

        result: dict[str, object] = {
            "container_id": self.container_id,
            "finished_at": self.finished_at,
            "name": self.name,
            "reason": self.reason,
        }
        if self.exit_code is not None:
            result["exit_code"] = self.exit_code
        return result


__all__ = ["SemanticRefreshContainerTermination"]
