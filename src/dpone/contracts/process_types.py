"""Общие типы для ядра ETL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TransformConfig:
    name: str
    params: dict[str, Any]


@dataclass(frozen=True)
class DependencyConfig:
    path: str
    alias: str | None = None
    group: str | None = None


@dataclass(frozen=True)
class ProcessResult:
    status: str
    inserted_rows: int
    updated_rows: int
    final_rows: int
    extracted_rows: int
    duration_seconds: float
    errors: list[str]
    details: dict[str, Any] | None = None
    """Optional structured evidence (e.g. chunked backfill progress)."""

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows,
            "final_rows": self.final_rows,
            "extracted_rows": self.extracted_rows,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
        }
        if self.details:
            payload["details"] = self.details
        return payload
