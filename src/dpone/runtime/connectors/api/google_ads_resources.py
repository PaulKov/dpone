from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GoogleAdsResourceSpec:
    """Typed description of a supported Google Ads logical resource."""

    name: str
    table_prefix: str = "app"
    default_load_strategy: str = "incremental_merge"
    description: str = ""

    @property
    def table_name(self) -> str:
        return google_ads_table_name(self.name, prefix=self.table_prefix)


_GOOGLE_ADS_RESOURCES: dict[str, GoogleAdsResourceSpec] = {
    "ads_stats": GoogleAdsResourceSpec(
        name="ads_stats",
        description="Daily aggregated Google Ads statistics across supported report slices.",
    ),
}


def list_google_ads_resources() -> list[GoogleAdsResourceSpec]:
    return list(_GOOGLE_ADS_RESOURCES.values())


def get_google_ads_resource(name: str) -> GoogleAdsResourceSpec:
    key = str(name).strip().lower()
    try:
        return _GOOGLE_ADS_RESOURCES[key]
    except KeyError as exc:
        supported = ", ".join(sorted(_GOOGLE_ADS_RESOURCES))
        raise KeyError(f"Unknown Google Ads resource '{name}'. Supported: {supported}") from exc


def google_ads_table_name(resource_name: str, *, prefix: str = "app") -> str:
    return f"{prefix}__{resource_name}"


__all__ = [
    "GoogleAdsResourceSpec",
    "get_google_ads_resource",
    "google_ads_table_name",
    "list_google_ads_resources",
]
