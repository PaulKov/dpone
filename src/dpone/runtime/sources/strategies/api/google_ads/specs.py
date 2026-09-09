from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "GoogleAdsQuerySpec",
    "ADS_STATS_QUERY_SPECS",
]


@dataclass(frozen=True)
class GoogleAdsQuerySpec:
    report: str
    query: str
    default_term: str | None = None


ADS_STATS_QUERY_SPECS: tuple[GoogleAdsQuerySpec, ...] = (
    GoogleAdsQuerySpec(
        report="keywords",
        query="""
            SELECT
              segments.date,
              campaign.name,
              campaign.id,
              ad_group_criterion.keyword.text,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM keyword_view
            WHERE campaign.advertising_channel_type != 'MULTI_CHANNEL'
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """,
    ),
    GoogleAdsQuerySpec(
        report="display",
        query="""
            SELECT
              segments.date,
              campaign.name,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM ad_group_ad
            WHERE campaign.advertising_channel_type = 'DISPLAY'
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """,
        default_term="Не определено",
    ),
    GoogleAdsQuerySpec(
        report="webpages",
        query="""
            SELECT
              segments.date,
              campaign.name,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM webpage_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """,
        default_term="Динамика",
    ),
    GoogleAdsQuerySpec(
        report="pmax",
        query="""
            SELECT
              segments.date,
              campaign.name,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM campaign
            WHERE campaign.advertising_channel_type = PERFORMANCE_MAX
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """,
        default_term="Не определено",
    ),
    GoogleAdsQuerySpec(
        report="discovery",
        query="""
            SELECT
              segments.date,
              campaign.name,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM campaign
            WHERE campaign.advertising_channel_type = DEMAND_GEN
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """,
        default_term="Не определено",
    ),
    GoogleAdsQuerySpec(
        report="app",
        query="""
            SELECT
              segments.date,
              campaign.name,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM campaign
            WHERE campaign.advertising_channel_type = 'MULTI_CHANNEL'
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """,
        default_term="Не определено",
    ),
    GoogleAdsQuerySpec(
        report="shopping",
        query="""
            SELECT
              segments.date,
              campaign.name,
              campaign.id,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM campaign
            WHERE campaign.advertising_channel_type = 'SHOPPING'
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """,
        default_term="Не определено",
    ),
)
