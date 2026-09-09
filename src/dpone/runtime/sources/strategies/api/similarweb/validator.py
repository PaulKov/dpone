from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)


class SimilarwebDQValidator:
    """DQ checks for SimilarWeb keyword snapshots."""

    UNIQUE_KEY: tuple[str, ...] = ("date", "domain", "keyword", "top_url")
    RANGE_FIELDS: tuple[str, ...] = ("zero_clicks_share", "traffic_share")

    def __init__(self, etl_logger: Any = None):
        self._logger = etl_logger or logger

    def check_period(self, snapshot_month: str) -> None:
        expected = datetime.strptime(snapshot_month, "%Y-%m-%d").date()
        current_month_start = date.today().replace(day=1)
        if expected >= current_month_start:
            raise RuntimeError(f"SimilarWeb snapshot_month={snapshot_month} must be earlier than the current month.")

    def validate(
        self,
        records: list[dict[str, Any]],
        *,
        snapshot_month: str,
        min_keywords_count: int,
    ) -> None:
        self.check_period(snapshot_month)
        self._check_min_count(records, snapshot_month=snapshot_month, min_keywords_count=min_keywords_count)
        self._check_duplicates(records)
        self._check_metric_ranges(records)

    def _check_min_count(
        self,
        records: list[dict[str, Any]],
        *,
        snapshot_month: str,
        min_keywords_count: int,
    ) -> None:
        if not records:
            raise RuntimeError(f"SimilarWeb returned no records for snapshot_month={snapshot_month}")
        if len(records) < min_keywords_count:
            raise RuntimeError(
                f"SimilarWeb returned {len(records)} rows for snapshot_month={snapshot_month}; "
                f"minimum required is {min_keywords_count}"
            )

    def _check_duplicates(self, records: list[dict[str, Any]]) -> None:
        seen: set[tuple[str, ...]] = set()
        duplicates: list[tuple[str, ...]] = []
        for record in records:
            key = tuple(str(record.get(field)) for field in self.UNIQUE_KEY)
            if key in seen:
                duplicates.append(key)
            else:
                seen.add(key)
        if duplicates:
            raise RuntimeError(
                f"SimilarWeb contains duplicate rows for key {self.UNIQUE_KEY}. First duplicates: {duplicates[:5]}"
            )

    def _check_metric_ranges(self, records: list[dict[str, Any]]) -> None:
        violations: list[str] = []
        for record in records:
            keyword = record.get("keyword", "?")
            for field in self.RANGE_FIELDS:
                value = record.get(field)
                if value is not None and not (0.0 <= value <= 1.0):
                    violations.append(f"{field}={value} for keyword={keyword}")
        if violations:
            self._logger.warning(
                "SimilarWeb metric range violations outside [0, 1]: %s",
                violations[:5],
            )
