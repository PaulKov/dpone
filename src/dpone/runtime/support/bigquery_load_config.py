"""BigQuery load job configuration models shared by connectors and sinks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CSVLoadConfig:
    """CSV settings passed to BigQuery load jobs."""

    skip_leading_rows: int = 1
    allow_quoted_newlines: bool = True
    allow_jagged_rows: bool = True
    field_delimiter: str = ","
    encoding: str = "UTF-8"


def postgres_headerless_csv_load_config() -> CSVLoadConfig:
    """Return CSV settings for headerless PostgreSQL COPY exports."""
    return CSVLoadConfig(
        skip_leading_rows=0,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        field_delimiter=",",
        encoding="UTF-8",
    )


__all__ = ["CSVLoadConfig", "postgres_headerless_csv_load_config"]
