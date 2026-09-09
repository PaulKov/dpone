"""Deprecated compatibility facade for BigQuery load job configuration models."""

from __future__ import annotations

from dpone.runtime.support.bigquery_load_config import (
    CSVLoadConfig,
    postgres_headerless_csv_load_config,
)

__all__ = ["CSVLoadConfig", "postgres_headerless_csv_load_config"]
