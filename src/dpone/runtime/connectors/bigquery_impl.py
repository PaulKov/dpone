"""Deprecated compatibility shim for BigQuery connector imports.

Use ``dpone.runtime.connectors.bigquery`` for public imports or
``dpone.runtime.connectors.bigquery_connector`` for focused connector imports.
"""

from __future__ import annotations

from dpone.runtime.connectors.bigquery_connector import (
    BigQueryConnector,
    CSVLoadConfig,
    _is_pandas_dataframe,
    _load_pandas,
    ensure_client_ready,
    logger,
    postgres_headerless_csv_load_config,
)
from dpone.runtime.connectors.bigquery_runtime import _require_bigquery, format_gcs_uri_for_display

__all__ = [
    "_require_bigquery",
    "_load_pandas",
    "_is_pandas_dataframe",
    "format_gcs_uri_for_display",
    "ensure_client_ready",
    "BigQueryConnector",
    "CSVLoadConfig",
    "logger",
    "postgres_headerless_csv_load_config",
]
