from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

FasttrackTransport = Literal["dashboard_report", "flex_ticket"]
FasttrackStrategyName = Literal["full_refresh", "incremental_merge"]


@dataclass(frozen=True)
class FasttrackResourceSpec:
    """Typed description of a supported Fasttrack pull resource."""

    name: str
    transport: FasttrackTransport
    endpoint_path: str
    table_prefix: str = "default"
    default_params: dict[str, Any] = field(default_factory=dict)
    description: str = ""
    date_fields: dict[str, str | None] = field(default_factory=dict)
    default_load_strategy: FasttrackStrategyName = "full_refresh"
    merge_keys: tuple[str, ...] = ()

    @property
    def table_name(self) -> str:
        return fasttrack_table_name(self.name, prefix=self.table_prefix)


_FASTTRACK_RESOURCES: dict[str, FasttrackResourceSpec] = {
    "cascade_transactions": FasttrackResourceSpec(
        name="cascade_transactions",
        transport="dashboard_report",
        endpoint_path="reports/",
        default_params={
            "type": "CASCADE_TRANSACTIONS",
            "uuid": "0890b115-c568-4764-ad6b-3d866e78db2d",
        },
        description="Dashboard CSV report with cascade transaction facts.",
        date_fields={
            "created_at": None,
            "done_at": None,
        },
        default_load_strategy="incremental_merge",
        merge_keys=("transaction_uuid",),
    ),
    "chat_sessions": FasttrackResourceSpec(
        name="chat_sessions",
        transport="dashboard_report",
        endpoint_path="reports/",
        default_params={
            "type": "SESSIONS",
            "uuid": "0890b115-c568-4764-ad6b-3d866e78db2d",
        },
        description="Dashboard CSV report with chat sessions.",
        date_fields={
            "data_nachala": "%d-%m-%Y %H:%M:%S",
            "data_okonchaniya": "%d-%m-%Y %H:%M:%S",
            "naznachen_na_komandu": "%d-%m-%Y %H:%M:%S",
            "naznachen_na_operatora": "%d-%m-%Y %H:%M:%S",
        },
        default_load_strategy="incremental_merge",
        merge_keys=("uuid_polzovatelya", "data_nachala"),
    ),
    "flex_cms_ratings": FasttrackResourceSpec(
        name="flex_cms_ratings",
        transport="flex_ticket",
        endpoint_path="ticket/",
        default_params={
            "category": "b934e608-aa3d-4359-84c0-169d264bc76d",
            "page_size": 10000,
        },
        description="Flex JSON endpoint with CMS ticket ratings.",
        date_fields={
            "created": None,
            "modified": None,
        },
        default_load_strategy="full_refresh",
    ),
}


def list_fasttrack_resources() -> list[FasttrackResourceSpec]:
    return list(_FASTTRACK_RESOURCES.values())


def get_fasttrack_resource(name: str) -> FasttrackResourceSpec:
    key = str(name).strip().lower()
    try:
        return _FASTTRACK_RESOURCES[key]
    except KeyError as exc:
        raise KeyError(
            f"Unknown Fasttrack resource '{name}'. Supported: {', '.join(sorted(_FASTTRACK_RESOURCES))}"
        ) from exc


def fasttrack_table_name(resource_name: str, *, prefix: str = "default") -> str:
    return f"{prefix}__{resource_name}"


__all__ = [
    "FasttrackResourceSpec",
    "FasttrackTransport",
    "FasttrackStrategyName",
    "fasttrack_table_name",
    "get_fasttrack_resource",
    "list_fasttrack_resources",
]
