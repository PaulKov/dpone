"""Контекст выполнения ETL процессов."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunContext:
    run_id: str
    watermark: Any = None
    config: dict[str, Any] = field(default_factory=dict)

    def emit(self, event: str, payload: dict[str, Any]) -> None:
        # Пока placeholder:
        pass
