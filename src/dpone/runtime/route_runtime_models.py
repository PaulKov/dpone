"""Route runtime value objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class LoadStepAuditRecord:
    run_id: str
    load_id: str
    step_id: str
    phase: str
    kind: str
    status: str
    started_at: datetime
    finished_at: datetime
    details_json: dict[str, object] = field(default_factory=dict)


__all__ = ["LoadStepAuditRecord"]
