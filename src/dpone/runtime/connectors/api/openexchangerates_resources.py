from __future__ import annotations

from dataclasses import dataclass

OPENEXCHANGERATES_HISTORICAL_RATES_SCHEMA: tuple[tuple[str, str], ...] = (
    ("as_of_date", "DATE"),
    ("base_currency", "STRING"),
    ("symbol", "STRING"),
    ("rate", "FLOAT64"),
    ("provider_timestamp_utc", "TIMESTAMP"),
    ("disclaimer", "STRING"),
    ("license", "STRING"),
)


@dataclass(frozen=True)
class OpenExchangeRatesResourceSpec:
    name: str
    endpoint_path_template: str
    table_prefix: str = "default"
    partition_column: str = "as_of_date"
    default_load_strategy: str = "incremental_merge"
    unique_key: tuple[str, ...] = ("as_of_date", "symbol")
    description: str = ""

    @property
    def table_name(self) -> str:
        return openexchangerates_table_name(self.name, prefix=self.table_prefix)


_OPENEXCHANGERATES_RESOURCES: dict[str, OpenExchangeRatesResourceSpec] = {
    "historical_rates_daily": OpenExchangeRatesResourceSpec(
        name="historical_rates_daily",
        endpoint_path_template="/historical/{as_of_date}.json",
        description="Daily historical FX rates flattened to one row per currency symbol.",
    ),
}


def list_openexchangerates_resources() -> list[OpenExchangeRatesResourceSpec]:
    return list(_OPENEXCHANGERATES_RESOURCES.values())


def get_openexchangerates_resource(name: str) -> OpenExchangeRatesResourceSpec:
    key = str(name).strip().lower()
    try:
        return _OPENEXCHANGERATES_RESOURCES[key]
    except KeyError as exc:
        supported = ", ".join(sorted(_OPENEXCHANGERATES_RESOURCES))
        raise KeyError(f"Unknown OpenExchangeRates resource '{name}'. Supported: {supported}") from exc


def openexchangerates_table_name(resource_name: str, *, prefix: str = "default") -> str:
    return f"{prefix}__{resource_name}"


__all__ = [
    "OPENEXCHANGERATES_HISTORICAL_RATES_SCHEMA",
    "OpenExchangeRatesResourceSpec",
    "get_openexchangerates_resource",
    "list_openexchangerates_resources",
    "openexchangerates_table_name",
]
