from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

MindboxFormat = Literal["json", "csv"]
MindboxWindowMode = Literal["none", "datetime_utc", "date_project"]
MindboxStrategy = Literal["full_refresh", "incremental_merge", "replace"]
MindboxDateMode = MindboxWindowMode


@dataclass(frozen=True)
class MindboxResourceSpec:
    """Description of a Mindbox async-export resource."""

    name: str
    operation: str
    format: MindboxFormat
    data_key: str | None
    window_mode: MindboxWindowMode
    recommended_strategy: MindboxStrategy
    default_lookback_days: int | None = None
    default_replace_column: str | None = None


_MINDBOX_RESOURCES: dict[str, MindboxResourceSpec] = {
    "getclients": MindboxResourceSpec(
        name="getclients",
        operation="GetClients",
        format="json",
        data_key="customers",
        window_mode="datetime_utc",
        recommended_strategy="incremental_merge",
        default_lookback_days=2,
    ),
    "getorders": MindboxResourceSpec(
        name="getorders",
        operation="GetOrders",
        format="json",
        data_key="orders",
        window_mode="datetime_utc",
        recommended_strategy="incremental_merge",
        default_lookback_days=30,
    ),
    "getactions": MindboxResourceSpec(
        name="getactions",
        operation="GetActions",
        format="json",
        data_key="customerActions",
        window_mode="datetime_utc",
        recommended_strategy="replace",
        default_lookback_days=2,
        default_replace_column="creationDateTimeUtc",
    ),
    "getmailings": MindboxResourceSpec(
        name="getmailings",
        operation="GetMailings",
        format="json",
        data_key="mailings",
        window_mode="datetime_utc",
        recommended_strategy="incremental_merge",
    ),
    "getmessagingreport": MindboxResourceSpec(
        name="getmessagingreport",
        operation="GetMessagingReport",
        format="csv",
        data_key=None,
        window_mode="date_project",
        recommended_strategy="full_refresh",
    ),
    "operationslogs": MindboxResourceSpec(
        name="operationslogs",
        operation="Exports.OperationsLogs",
        format="csv",
        data_key=None,
        window_mode="datetime_utc",
        recommended_strategy="replace",
        default_lookback_days=2,
        default_replace_column="OperationLogStartDateTimeUtc",
    ),
}


def list_mindbox_resources() -> list[MindboxResourceSpec]:
    return list(_MINDBOX_RESOURCES.values())


def get_mindbox_resource(name: str) -> MindboxResourceSpec:
    try:
        return _MINDBOX_RESOURCES[str(name).strip().lower()]
    except KeyError as exc:
        supported = ", ".join(sorted(_MINDBOX_RESOURCES))
        raise KeyError(f"Неизвестный ресурс Mindbox '{name}'. Поддерживаемые: {supported}") from exc


def mindbox_table_name(resource_name: str, prefix: str = "app") -> str:
    return f"{prefix}__{resource_name}"


__all__ = [
    "MindboxDateMode",
    "MindboxFormat",
    "MindboxResourceSpec",
    "MindboxStrategy",
    "MindboxWindowMode",
    "get_mindbox_resource",
    "list_mindbox_resources",
    "mindbox_table_name",
]
