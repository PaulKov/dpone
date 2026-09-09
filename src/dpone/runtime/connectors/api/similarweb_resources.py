from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SimilarwebResourceSpec:
    """Typed description of a supported SimilarWeb pull resource."""

    name: str
    endpoint_path: str
    table_prefix: str = "default"
    partition_column: str = "date"
    default_load_strategy: str = "incremental_append"
    unique_key: tuple[str, ...] = ("date", "domain", "keyword", "top_url")
    description: str = ""

    @property
    def table_name(self) -> str:
        return similarweb_table_name(self.name, prefix=self.table_prefix)


SIMILARWEB_KEYWORDS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("date", "DATE"),
    ("domain", "STRING"),
    ("keyword", "STRING"),
    ("clicks", "FLOAT64"),
    ("traffic_share", "FLOAT64"),
    ("difficulty", "INT64"),
    ("competition", "FLOAT64"),
    ("primary_intent", "STRING"),
    ("secondary_intent", "STRING"),
    ("volume", "INT64"),
    ("cpc", "FLOAT64"),
    ("cpc_low_bid", "FLOAT64"),
    ("cpc_high_bid", "FLOAT64"),
    ("zero_clicks_share", "FLOAT64"),
    ("position", "INT64"),
    ("serp_features", "REPEATED STRING"),
    ("top_url", "STRING"),
)


_SIMILARWEB_RESOURCES: dict[str, SimilarwebResourceSpec] = {
    "keywords": SimilarwebResourceSpec(
        name="keywords",
        endpoint_path="/v4/website-analysis/keywords",
        description="Monthly website keywords snapshot for a domain.",
    ),
}


def list_similarweb_resources() -> list[SimilarwebResourceSpec]:
    return list(_SIMILARWEB_RESOURCES.values())


def get_similarweb_resource(name: str) -> SimilarwebResourceSpec:
    key = str(name).strip().lower()
    try:
        return _SIMILARWEB_RESOURCES[key]
    except KeyError as exc:
        supported = ", ".join(sorted(_SIMILARWEB_RESOURCES))
        raise KeyError(f"Unknown SimilarWeb resource '{name}'. Supported: {supported}") from exc


def similarweb_table_name(resource_name: str, *, prefix: str = "default") -> str:
    return f"{prefix}__{resource_name}"


__all__ = [
    "SIMILARWEB_KEYWORDS_SCHEMA",
    "SimilarwebResourceSpec",
    "get_similarweb_resource",
    "list_similarweb_resources",
    "similarweb_table_name",
]
