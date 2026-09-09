from __future__ import annotations

import re
from typing import Any


class GoogleSheetsRowsMixin:
    def _values_to_records(
        self,
        *,
        raw_values: list[list[Any]],
        spreadsheet_id: str,
        spreadsheet_title: str | None,
        worksheet_title: str,
        range_name: str | None,
        header_row: int,
        skip_rows: int,
        limit_rows: int | None,
        limit_columns: int | None,
        add_metadata_columns: bool,
    ) -> list[dict[str, Any]]:
        if not raw_values:
            return []
        header_idx = max(header_row - 1, 0)
        if header_idx >= len(raw_values):
            return []

        header_values = raw_values[header_idx]
        headers = self._normalize_headers(header_values)
        start_idx = header_idx + 1 + max(skip_rows, 0)
        data_rows = raw_values[start_idx:]
        if limit_rows is not None:
            data_rows = data_rows[: max(limit_rows, 0)]
        if limit_columns is not None:
            effective_limit = max(limit_columns, 0)
            headers = headers[:effective_limit]
            data_rows = [list(row_values)[:effective_limit] for row_values in data_rows]

        records: list[dict[str, Any]] = []
        for row_number, row_values in enumerate(data_rows, start=start_idx + 1):
            normalized_values = list(row_values)[: len(headers)]
            if len(normalized_values) < len(headers):
                normalized_values.extend([None] * (len(headers) - len(normalized_values)))

            row_dict = {header: value for header, value in zip(headers, normalized_values, strict=False)}
            if all(value in (None, "") for value in row_dict.values()):
                continue

            if add_metadata_columns:
                row_dict["_meta_spreadsheet_id"] = spreadsheet_id
                row_dict["_meta_spreadsheet_title"] = spreadsheet_title
                row_dict["_meta_worksheet_title"] = worksheet_title
                row_dict["_meta_range_name"] = range_name
                row_dict["_meta_row_number"] = row_number

            records.append(row_dict)
        return records

    def _normalize_headers(self, header_values: list[Any]) -> list[str]:
        seen: dict[str, int] = {}
        result: list[str] = []
        for idx, raw in enumerate(header_values, start=1):
            header = self._normalize_header_value(raw, idx)
            if header in seen:
                seen[header] += 1
                header = f"{header}_{seen[header]}"
            else:
                seen[header] = 1
            result.append(header)
        return result

    @staticmethod
    def _normalize_header_value(value: Any, idx: int) -> str:
        raw = "" if value is None else str(value).strip().lower()
        raw = raw.replace("\n", " ").replace("\r", " ")
        raw = re.sub(r"\s+", "_", raw)
        raw = re.sub(r"[^\w]+", "_", raw, flags=re.UNICODE)
        raw = re.sub(r"_+", "_", raw).strip("_")
        if not raw:
            return f"column_{idx}"
        if raw[0].isdigit():
            raw = f"column_{idx}_{raw}"
        return raw
