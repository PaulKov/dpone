from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GoogleSheetsResourceSpec:
    """Typed description of a supported Google Sheets logical resource."""

    name: str
    table_prefix: str = "app"
    default_load_strategy: str = "full_refresh"
    description: str = ""

    @property
    def table_name(self) -> str:
        return google_sheets_table_name(self.name, prefix=self.table_prefix)


_GOOGLE_SHEETS_RESOURCES: dict[str, GoogleSheetsResourceSpec] = {
    "worksheet_rows": GoogleSheetsResourceSpec(
        name="worksheet_rows",
        description="Rows extracted from a worksheet or worksheet range.",
    ),
}


def list_google_sheets_resources() -> list[GoogleSheetsResourceSpec]:
    return list(_GOOGLE_SHEETS_RESOURCES.values())


def get_google_sheets_resource(name: str) -> GoogleSheetsResourceSpec:
    key = str(name).strip().lower()
    try:
        return _GOOGLE_SHEETS_RESOURCES[key]
    except KeyError as exc:
        supported = ", ".join(sorted(_GOOGLE_SHEETS_RESOURCES))
        raise KeyError(f"Unknown Google Sheets resource '{name}'. Supported: {supported}") from exc


def google_sheets_table_name(resource_name: str, *, prefix: str = "app") -> str:
    return f"{prefix}__{resource_name}"


__all__ = [
    "GoogleSheetsResourceSpec",
    "get_google_sheets_resource",
    "google_sheets_table_name",
    "list_google_sheets_resources",
]
