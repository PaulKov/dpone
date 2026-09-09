from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.sources.strategies.api.google_ads.specs import ADS_STATS_QUERY_SPECS


def _micros_to_currency(micros: int | None) -> float | None:
    return micros / 1_000_000.0 if micros is not None else None


class GoogleAdsFullExtractStrategy(APIBaseStrategy):
    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        del last_state
        options = self._get_options(load_config)

        resource = options.get("resource", "ads_stats")
        start_date = options.get("start_date")
        end_date = options.get("end_date")
        customer_ids = self._normalize_customer_ids(options.get("customer_ids"))

        if resource != "ads_stats":
            raise ValueError(f"Google Ads full_extract: unknown resource='{resource}'. Supported resource: ads_stats")
        if not start_date or not end_date:
            raise ValueError("Google Ads full_extract requires options.start_date and options.end_date")

        customers = customer_ids or self.connector.credentials.customer_ids
        if not customers:
            raise ValueError("Google Ads customer_ids are empty")

        self.logger.log_etl_progress(
            "API_FULL_EXTRACT",
            {
                "API": "google_ads",
                "Resource": resource,
                "Start_Date": str(start_date),
                "End_Date": str(end_date),
                "Started_At_UTC": datetime.now(UTC).isoformat(),
            },
        )

        rows = self._fetch_ads_stats_rows(
            customer_ids=customers,
            start_date=str(start_date),
            end_date=str(end_date),
        )
        if not rows:
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=None,
                force_full_refresh=True,
            )

        schema = self._detect_schema_from_records(rows[:10])
        self.logger.log_etl_progress(
            "API_FULL_EXTRACT_DONE",
            {"API": "google_ads", "Resource": resource, "Rows": len(rows)},
        )
        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
            state=None,
            force_full_refresh=True,
        )

    def _fetch_ads_stats_rows(self, *, customer_ids: list[str], start_date: str, end_date: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for raw_customer_id in customer_ids:
            customer_id = str(raw_customer_id).replace("-", "").strip()
            if not customer_id:
                continue
            for spec in ADS_STATS_QUERY_SPECS:
                raw_rows = self.connector.execute_query(
                    customer_id=customer_id,
                    query=spec.query.format(start_date=start_date, end_date=end_date),
                )
                for row in raw_rows:
                    term = spec.default_term
                    if spec.report == "keywords":
                        term = getattr(getattr(row.ad_group_criterion, "keyword", None), "text", None)
                    rows.append(
                        {
                            "date": str(row.segments.date),
                            "login": customer_id,
                            "campaign": row.campaign.name,
                            "campaign_id": int(row.campaign.id),
                            "term": term,
                            "impressions": int(row.metrics.impressions or 0),
                            "clicks": int(row.metrics.clicks or 0),
                            "cost": _micros_to_currency(row.metrics.cost_micros),
                            "report": spec.report,
                        }
                    )
        return rows

    @staticmethod
    def _normalize_customer_ids(raw_value: Any) -> list[str] | None:
        if raw_value is None:
            return None
        if isinstance(raw_value, list):
            return [str(x).strip().replace("-", "") for x in raw_value if str(x).strip()]
        return [item.strip().replace("-", "") for item in str(raw_value).split(",") if item and item.strip()]
