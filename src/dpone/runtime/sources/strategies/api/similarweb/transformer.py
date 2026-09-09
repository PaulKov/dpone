from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class SimilarwebRowTransformer:
    """Transforms SimilarWeb keyword rows into landing schema records."""

    def transform(
        self,
        raw_rows: list[dict[str, Any]],
        *,
        snapshot_month: str,
        domain: str,
    ) -> list[dict[str, Any]]:
        month_date = datetime.strptime(snapshot_month, "%Y-%m-%d").date()
        return [self._transform_row(raw, month_date, domain) for raw in raw_rows]

    def _transform_row(self, raw: dict[str, Any], month_date: Any, domain: str) -> dict[str, Any]:
        return {
            "date": month_date,
            "domain": domain,
            "keyword": self._coerce_str(raw.get("keyword")),
            "clicks": self._coerce_float(raw.get("clicks")),
            "traffic_share": self._coerce_float(raw.get("traffic_share")),
            "difficulty": self._coerce_int(raw.get("difficulty")),
            "competition": self._coerce_competition(raw.get("competition")),
            "primary_intent": self._coerce_str(raw.get("primary_intent")),
            "secondary_intent": self._coerce_str(raw.get("secondary_intent")),
            "volume": self._coerce_int(raw.get("volume")),
            "cpc": self._coerce_float(raw.get("cpc")),
            "cpc_low_bid": self._coerce_float(raw.get("cpc_low_bid")),
            "cpc_high_bid": self._coerce_float(raw.get("cpc_high_bid")),
            "zero_clicks_share": self._coerce_float(raw.get("zero_clicks_share")),
            "position": self._coerce_int(raw.get("position")),
            "serp_features": self._coerce_serp_features(raw.get("serp_features")),
            "top_url": self._coerce_str(raw.get("top_url")),
        }

    @staticmethod
    def _coerce_str(value: Any) -> str | None:
        if value is None or value == "":
            return None
        return str(value)

    @staticmethod
    def _coerce_competition(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_serp_features(value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, list):
            return [str(item) for item in value if item is not None]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (json.JSONDecodeError, TypeError, ValueError):
                parsed = None
            if isinstance(parsed, list):
                return [str(item) for item in parsed if item is not None]
            return [value] if value else None
        return None
