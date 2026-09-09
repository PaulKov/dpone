from __future__ import annotations

from datetime import datetime
from typing import Any

from dpone._compat import UTC
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy


class GoogleSheetsFullExtractStrategy(APIBaseStrategy):
    """FULL_REFRESH extract strategy for Google Sheets worksheets."""

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        return None

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        del last_state
        options = self._get_options(load_config)
        resource = str(options.get("resource") or "worksheet_rows")
        if resource != "worksheet_rows":
            raise ValueError(
                "GoogleSheets full_extract supports only options.resource='worksheet_rows' in canonical runtime"
            )

        spreadsheet_id = options.get("spreadsheet_id")
        spreadsheet_url = options.get("spreadsheet_url")
        worksheet_title = options.get("worksheet_title")
        worksheet_index = options.get("worksheet_index")
        worksheet_gid = options.get("worksheet_gid")
        range_name = options.get("range_name")

        if not spreadsheet_id and not spreadsheet_url:
            raise ValueError(
                "GoogleSheets full_extract: необходимо указать options.spreadsheet_id или options.spreadsheet_url"
            )

        self.logger.log_etl_progress(
            "API_FULL_EXTRACT",
            {
                "API": "google_sheets",
                "Resource": resource,
                "Spreadsheet_ID": spreadsheet_id or "",
                "Worksheet_Title": worksheet_title or "",
                "Worksheet_Index": worksheet_index if worksheet_index is not None else "",
                "Worksheet_GID": worksheet_gid if worksheet_gid is not None else "",
                "Range": range_name or "",
                "Started_At_UTC": datetime.now(UTC).isoformat(),
            },
        )

        rows = self.connector.get_records(
            spreadsheet_id=spreadsheet_id,
            spreadsheet_url=spreadsheet_url,
            worksheet_title=worksheet_title,
            worksheet_index=worksheet_index,
            worksheet_gid=worksheet_gid,
            range_name=range_name,
            header_row=int(options.get("header_row", 1) or 1),
            skip_rows=int(options.get("skip_rows", 0) or 0),
            limit_rows=int(options["limit_rows"]) if options.get("limit_rows") not in (None, "") else None,
            limit_columns=int(options["limit_columns"]) if options.get("limit_columns") not in (None, "") else None,
            add_metadata_columns=bool(options.get("add_metadata_columns", True)),
            value_render_option=str(options.get("value_render_option", "UNFORMATTED_VALUE")),
            date_time_render_option=str(options.get("date_time_render_option", "FORMATTED_STRING")),
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
            {
                "API": "google_sheets",
                "Resource": resource,
                "Rows": len(rows),
            },
        )
        return ExtractResult(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
            state=None,
            force_full_refresh=True,
        )
