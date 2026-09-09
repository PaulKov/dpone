from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy
from dpone.runtime.sources.strategies.api.google_ads.specs import ADS_STATS_QUERY_SPECS


def _micros_to_currency(micros: int | None) -> float | None:
    return micros / 1_000_000.0 if micros is not None else None


class GoogleAdsIncrementalMergeExtractStrategy(APIBaseStrategy):
    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        options = self._get_options(load_config)
        resource = options.get("resource", "ads_stats")
        if resource != "ads_stats":
            raise ValueError(
                f"Google Ads incremental_merge: unknown resource='{resource}'. Supported resource: ads_stats"
            )

        lookback_days = int(options.get("lookback_days", 3) or 3)
        full_refresh_days = int(options.get("full_refresh_days", 30) or 30)
        explicit_start = options.get("start_date")
        explicit_end = options.get("end_date")
        customer_ids = self._normalize_customer_ids(options.get("customer_ids"))

        customers = customer_ids or self.connector.credentials.customer_ids
        if not customers:
            raise ValueError("Google Ads customer_ids are empty")

        today = datetime.now(UTC).date()
        if explicit_start:
            from_day = datetime.strptime(str(explicit_start), "%Y-%m-%d").date()
            to_day = datetime.strptime(str(explicit_end), "%Y-%m-%d").date() if explicit_end else today
            if from_day > to_day:
                raise ValueError(f"Google Ads incremental_merge: start_date ({from_day}) > end_date ({to_day})")
        else:
            to_day = datetime.strptime(str(explicit_end), "%Y-%m-%d").date() if explicit_end else today
            if not last_state or not last_state.get("last_value"):
                from_day = to_day - timedelta(days=full_refresh_days)
                self.logger.info("Google Ads: no state found, initial load for last %d days", full_refresh_days)
            else:
                try:
                    last_day = datetime.strptime(str(last_state["last_value"]), "%Y-%m-%d").date()
                except ValueError:
                    last_day = to_day - timedelta(days=full_refresh_days)
                from_day = last_day - timedelta(days=lookback_days)
            if from_day > to_day:
                from_day = to_day

        start_date = from_day.isoformat()
        end_date = to_day.isoformat()
        self.logger.log_etl_progress(
            "API_INCREMENTAL_MERGE",
            {
                "API": "google_ads",
                "Resource": resource,
                "From_Day": start_date,
                "To_Day": end_date,
                "Lookback_Days": lookback_days,
            },
        )

        rows = self._fetch_ads_stats_rows(
            customer_ids=customers,
            start_date=start_date,
            end_date=end_date,
        )
        new_state = {"last_value": end_date}
        if not rows:
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=new_state,
                force_full_refresh=False,
            )

        schema = self._detect_schema_from_records(rows[:10])
        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
            state=new_state,
            force_full_refresh=False,
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
